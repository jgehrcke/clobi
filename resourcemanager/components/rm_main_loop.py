﻿# -*- coding: UTF-8 -*-
#
#   ::::::::> RESOURCE MANAGER <::::::::
#   Resource Manager MainLoop module
#
#   Contact: http://gehrcke.de
#
#   Copyright (C) 2009 Jan-Philip Gehrcke
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
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

        cloudstring = ''
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
        pause_loop = True
        timeout = 1

        self.ui_msg(("Interactive mode started. Type"
            " 'help' to get a list of available commands."))
        while True:
            # check user-given command (`uicmd` is a unicode object!)
            uicmd = self.poll_command_queue(timeout)
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
                        " Enter 'start' to go on."))
                elif uicmd == 'start':
                    pause_loop = False
                    self.ui_msg("ResourceManagerMainLoop starts/continues.")
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

            # check nimbus cloud subprocesses
            self.check_nimbus_cloud_client_wrappers()

    def check_nimbus_cloud_client_wrappers(self):
        """
        Check for running/returned subprocesses of nimbus cloud client.
        This will e.g. trigger update of save.session.vms file if there was
        success/error with starting some VM(s).
        """
        for nimbus_cloud in self.session.nimbus_clouds:
            nimbus_cloud.check_nimbus_cloud_client_wrappers()

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
        """
        Ask `question`, read input until "yes" or "no" comes and return True,
        False, respectively.
        """
        while True: # wait for instring to be a known command
            self.ui_msg(question+" [yes/no]")
            uicmd = self.poll_command_queue(timeout=None) # infinite block
            self.ui_msg('>>> '+uicmd)
            if uicmd == 'yes': return True
            if uicmd == 'no': return False

    def do_sqs_sdb_update_if_necessary(self):
        """
        Check current time against last checked times, using the user-given
        poll intervals to decide if a new SQS / SDB check has to be performed.
        """
        now = time.time()
        next_sqs_check_in = abs(min(0, (now -
            (self.sqs_last_checked + self.session.inicfg.sqs.monitor_queues_pollinterval))))
        next_sdb_check_in = abs(min(0, (now -
            (self.sdb_last_checked + self.session.inicfg.sdb.monitor_vms_pollinterval))))
        self.request_update_uiinfo(dict(
            txt_sqs_upd=str(int(round(next_sqs_check_in))).zfill(5)+" s",
            txt_sdb_upd=str(int(round(next_sdb_check_in))).zfill(5)+" s"))
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
        """poll
        Write help message to `self.pipe_cmdresp_write` -> UI
        """
        helpstring = ("Available commands:"
            +"\n* help:            Display this help message."
            +"\n* quit:            Quit Resource Manager."
            +"\n* pause:           Pause main loop (stop automatic operation)."
            +"\n* start:           Start main loop / continue after break."
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
