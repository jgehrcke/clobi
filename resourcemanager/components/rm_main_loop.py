# -*- coding: UTF-8 -*-
#
#   ::::::::> RESOURCE MANAGER <::::::::
#   Resource Manager MainLoop
#
#   by Jan-Philip Gehrcke (jgehrcke@gmail.com)
#
#   Copyright (C) 2009 Jan-Philip Gehrcke
#
#   LICENSE:
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 3 of the License, or
#   (at your option) any later version. This program is distributed in
#   the hope that it will be useful, but WITHOUT ANY WARRANTY; without
#   even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#   PARTICULAR PURPOSE. See the GNU General Public License for more
#   details. You should have received a copy of the GNU General Public
#   License along with this program; if not, see
#   <http://www.gnu.org/licenses/>.
#
########################################################################

from __future__ import unicode_literals

import optparse
import threading
import time
import sys
import os
import Queue
import logging

from components.cfg_parse_strzip import SafeConfigParserStringZip
from components.rm_session import Session


class ResourceManagerMainLoop(threading.Thread):
    def __init__(self,
                 pipe_cmdresp_write,
                 pipe_uiinfo_update_write,
                 queue_uicmds,
                 start_options,
                 session_dirs):
        self.logger = logging.getLogger("RM.MainLoop")
        self.logger.debug("initialize ResourceManagerMainLoop object")

        threading.Thread.__init__(self)
        self.pipe_cmdresp_write = pipe_cmdresp_write
        self.pipe_uiinfo_update_write = pipe_uiinfo_update_write
        self.queue_uicmds  = queue_uicmds
        self.start_options = start_options
        self.session_dirs = session_dirs

    def run (self):
        """
        Thread start method. Initialize main loop.
        """
        start_options = self.start_options
        self.logger.info("thread started.")
        self.session = Session(start_options, self.session_dirs)
        self.logger.info("LOAD INITIAL SESSION CONFIGURATION...")
        self.session.load_initial_session_config()

        cloudstring = 'Clouds: '
        if self.session.inicfg.ec2.use:
            cloudstring += 'EC2, '
        for nimbus_cloud in self.session.nimbus_clouds:
            cloudstring += 'Nb%s, ' % nimbus_cloud.cloud_index
        self.request_update_uiinfo(dict(
            txt_cloud=cloudstring.rstrip(", "),
            txt_name=self.session.inicfg.session.name))

        self.logger.info("FINISH SESSION SETUP...")
        self.session.finish_setup()
        self.main_loop()

    def main_loop(self):
        """
        Resource Manager's main loop. Do all the work after session init.
        Provide interactive mode.
        """
        self.logger.debug("main_loop() started.")

        # init some needed variables
        self.sqs_last_checked = time.time()
        self.sdb_last_checked = time.time()
        pause_loop = False

        self.ui_msg(("Starting main loop. It provides an interactive mode. Type"
            " 'help' to get a list of available commands."))
        while True:
            # check user-given command (`uicmd` is a unicode object!)
            uicmd = self.poll_command_queue(timeout=1)
            if uicmd is not None:
                self.ui_msg('>>> '+uicmd)
                if uicmd == 'quit':
                    self.ui_msg('end RM main loop')
                    break
                    # continue statement .. really quit? enter again..
                elif uicmd == 'help':
                    self.display_help_message()
                elif uicmd == 'pause':
                    pause_loop = True
                    self.ui_msg(("ResourceManagerMainLoop paused."
                        " Enter 'continue' to go on."))
                elif uicmd == 'continue':
                    pause_loop = False
                    self.ui_msg("ResourceManagerMainLoop continues.")
                elif uicmd == 'poll_sqs':
                    self.ui_msg("Manual SQS monitoring data update triggered.")
                    self.sqs_check()
                elif uicmd == 'poll_sdb':
                    self.ui_msg("Manual SDB monitoring data update triggered.")
                    self.sdb_check()
                elif uicmd.startswith('run_vms'):
                    self.run_vms(uicmd)
                else:
                    self.ui_msg('unknown command')

            # continue with automatic process, if not paused:
            if not pause_loop:
                self.do_sqs_sdb_update_if_necessary()

    def run_vms(self, cmd):
        """
        check for "run_vms cloud_name number_of_vms" structure.
        E.g.: "run_vms EC2 15" or "run_vms nb1 2"
        invoke run command after re-insurance.
        """
        def nbx(string, nbcldidcs):
            parts = string.split('nb')
            if len(parts) == 2:
                if parts[1].isdigit():
                    if int(parts[1]) in nbcldidcs:
                        return int(parts[1])
            return False

        words = [word.lower() for word in cmd.split()]
        nbcldidcs = [nbcld.cloud_index for nbcld in self.session.nimbus_clouds]
        if len(words) == 3 and words[0] == 'run_vms':
            cloud = words[1]               # cloud name
            number = words[2]              # number of vms
            index = nbx(cloud,nbcldidcs)   # valid nimbus cloud index or False
            if (cloud == 'ec2' or index) and number.isdigit():
                number = int(number)
                if cloud == 'ec2':
                    if self.yes_no('Run %s VMs on EC2?' % number):
                        self.session.run_vms_ec2(number)
                    else:
                        self.ui_msg('aborted.')
                elif self.yes_no('Run %s VMs on Nimbus Cloud %s?'%(number,index)):
                    self.session.run_vms_nimbus(index,number)
                else:
                    self.ui_msg('aborted.')
                return
        self.ui_msg(("better: e.g. 'run_vms EC2 3' or 'run_vms Nb4 2'."
                     " Nimbus cloud indices available: %s"%str(nbcldidcs)))

    def yes_no(self, question):
        while True: # wait for instring to be a known command
            self.ui_msg(question+" [yes/no]")
            uicmd = self.poll_command_queue(timeout=None) # infinite block
            self.ui_msg('>>> '+uicmd)
            if uicmd == 'yes': return True
            if uicmd == 'no': return False

    def do_sqs_sdb_update_if_necessary(self):
        now = time.time()
        next_sqs_check_in = abs(min(0, (now -
            (self.sqs_last_checked + self.session.inicfg.sqs.monitor_queues_pollinterval))))
        next_sdb_check_in = abs(min(0, (now -
            (self.sdb_last_checked + self.session.inicfg.sdb.monitor_vms_pollinterval))))
        self.request_update_uiinfo(dict(
            txt_sqs_upd="SQS update: "+str(int(round(next_sqs_check_in))).zfill(5)+" s",
            txt_sdb_upd="SDB update: "+str(int(round(next_sdb_check_in))).zfill(5)+" s"))
        if next_sqs_check_in == 0:
            self.logger.info("Automatic SQS monitoring data update triggered.")
            self.sqs_check()
        if next_sdb_check_in == 0:
            self.logger.info("Automatic SDB monitoring data update triggered.")
            self.sdb_check()

    def sdb_check(self):
        self.logger.debug("ResourceManagerMainLoop.sdb_check() called")
        self.sdb_last_checked = time.time()

    def sqs_check(self):
        self.logger.debug("ResourceManagerMainLoop.sqs_check() called")
        self.session.sqs_session.query_queues()
        self.sqs_last_checked = time.time()
        stringlist = []
        for prio, jobnr in self.session.sqs_session.queue_jobnbrs_laststate.items():
            stringlist.append("P"+str(prio).zfill(2)+": %s job(s)" % jobnr)
        self.request_update_uiinfo(dict(txt_sqs_jobs="\n".join(stringlist)))

    def ui_msg(self, msg):
        """
        Send message to UI
        """
        os.write(self.pipe_cmdresp_write, (msg+"\n").encode('UTF-8'))

    def display_help_message(self):
        """
        Write help message to `self.pipe_cmdresp_write` -> UI
        """
        helpstring = ("Available commands:"
            +"\n* help:            Display this help message."
            +"\n* quit:            Quit Resource Manager."
            +"\n* pause:           Pause main loop (stop automatic operation)."
            +"\n* continue:        Continue main loop after break."
            +"\n* run_vms cloud X: Run X VMs on cloud (EC2|NbY). E.g. 'run_vms Nb1 2'"
            +"\n* poll_sdb:        Update SDB monitoring data."
            +"\n* poll_sqs:        Update SQS monitoring data.")
        self.ui_msg(helpstring)

    def poll_command_queue(self, timeout):
        """
        Poll queue in a blocking manner and return received item.
        If there is nothing within timeout, return None
        """
        try:
            return self.queue_uicmds.get(block=True, timeout=timeout)
        except Queue.Empty:
            # os.write(self.pipe_cmdresp_write, '1 sec and got nothing :-(')
            return None

    def request_update_uiinfo(self, update_dict):
        """
        Make ConfigParser Config from update_dict, delimit with %% and && and
        send via pipe to GUI. Section "uiinfo" is hardcoded on both sides.
        """
        config = SafeConfigParserStringZip()
        config.add_section('uiinfo')
        for key, value in update_dict.items():
            config.set('uiinfo'.encode('utf-8'),key.encode('utf-8'),value.encode('utf-8'))
        configstring = "%%"+config.write_to_string().decode('utf-8')+"&&"
        os.write(self.pipe_uiinfo_update_write, configstring.encode('utf-8'))
