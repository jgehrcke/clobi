# -*- coding: UTF-8 -*-
#
#   ::::::::> Clobi RESOURCE MANAGER <::::::::
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
from components.utils import *


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
        self.ec2_inststates_last_checked = time.time()

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
                elif uicmd.startswith('query_fact_rp'):
                    self.nimbus_query_factory_rp(uicmd)
                elif uicmd.startswith('query_workspace'):
                    self.nimbus_query_workspace(uicmd)
                elif uicmd.startswith('destroy_workspace'):
                    self.nimbus_destroy_workspace(uicmd)
                elif uicmd.startswith('show_vm_info'):
                    self.show_vm_info(uicmd)
                elif uicmd.startswith('set_highest_prio'):
                    self.set_highest_prio(uicmd)
                elif uicmd.startswith('write_jmi_cfg_file'):
                    self.write_jmi_cfg_file()
                elif uicmd.startswith('softkill_vm'):
                    self.softkill_vm(uicmd)
                else:
                    self.ui_msg('unknown command')

            # continue with automatic process, if not paused:
            if not pause_loop:
                self.do_sqs_sdb_update_if_necessary()

            # check nimbus cloud subprocesses
            # this is done each main loop turn, it's not logged. (only success/
            # error events are logged, but not when subprocesses didn't return)
            self.check_nimbus_cloud_client_wrappers()

            # check EC2 instances that were requested to start recently
            # do it based on self.session.inicfg.ec2.instance_state_pollinterval
            self.check_runinstances_request_states_if_necessary()

            # this basically builds the `started_vms_string` and displays it
            self.display_started_vms()

    def write_jmi_cfg_file(self):
        """
        Instruct the Session class to write a config file that contains all
        information the JMI needs.
        """
        self.ui_msg(("Instructed to write Clobi's Job Management Interface"
            " configuration file..."))
        self.session.write_job_management_interface_config_file()

    def display_started_vms(self):
        """
        Generate a string like "EC2: 3    Nb1: 15    Nb2: 1" and display it i
        the UI info panel.
        """
        started_vms_string = self.session.get_started_vms_string()
        if started_vms_string:
            self.request_update_uiinfo(dict(txt_started_vms=started_vms_string))

    def check_runinstances_request_states_if_necessary(self):
        """
        Check states of EC2 instances that were requested to start recently.
        This updates save.session.vms about success/error while starting.
        After this information is gathered, the corresponding boto ec2
        reservation objects will be deleted (information about VMs/instances
        in save.session.vms is the only information left).
        """
        now = time.time()
        lastchecked = self.ec2_inststates_last_checked
        interval = self.session.inicfg.ec2.instance_state_pollinterval
        next_state_check_in = abs(min(0, (now - (lastchecked + interval))))
        if next_state_check_in == 0:
            self.session.ec2.check_runinstances_request_states()
            self.ec2_inststates_last_checked = time.time()

    def check_nimbus_cloud_client_wrappers(self):
        """
        Check for running/returned subprocesses of nimbus cloud client.
        This will e.g. trigger update of save.session.vms file if there was
        success/error with starting some VM(s). After this information is
        gathered, the corresponding Nimbus Cloud Client Wrapper objects will be
        deleted (information about VMs in save.session.vms is the only
        information left).
        """
        for nimbus_cloud in self.session.nimbus_clouds:
            nimbus_cloud.check_nimbus_cloud_client_wrappers()

    def nimbus_query_factory_rp(self, cmd):
        words = [word.lower() for word in cmd.split()]
        nbcldidcs = self.session.nimbus_cloud_indices()
        if len(words) == 2 and words[0] == 'query_fact_rp':
            cloud = words[1]                 # cloud name
            index = self.session.nbx(cloud)  # valid nimbus cloud index or False
            if index:
                self.session.nimbus_query_factory_rp(cloud_index=index)
                return
        self.ui_msg(("better: e.g. 'query_fact_rp Nb1' or 'query_fact_rp Nb13'."
            " Nimbus cloud indices available: %s"%str(nbcldidcs)))

    def nimbus_query_workspace(self, cmd):
        words = [word.lower() for word in cmd.split()]
        if len(words) == 2 and words[0] == 'query_workspace':
            entered_vm_id = words[1]
            vm_info = self.session.get_vm_info_from_file(vm_id=entered_vm_id)
            if vm_info:
                if vm_info['EC2']:
                    self.ui_msg("This VM corresponds to an EC2 instance.")
                elif vm_info['Nimbus']:
                    if vm_info['eprfile']:
                        self.session.nimbus_query_workspace(
                            cloud_index=vm_info['cloudindex'],
                            eprfile=vm_info['eprfile'],
                            vm_id=vm_info['vm_id'])
                    else:
                        self.logger.error(("No EPR file defined in "
                            "save.session.vms for VM %s" % entered_vm_id))
                else:
                    self.logger.error("This state should never be reached :-)")
            return
        self.ui_msg("better: e.g. 'query_workspace vm-0133'.")

    def nimbus_destroy_workspace(self, cmd):
        words = [word.lower() for word in cmd.split()]
        if len(words) == 2 and words[0] == 'destroy_workspace':
            entered_vm_id = words[1]
            vm_info = self.session.get_vm_info_from_file(vm_id=entered_vm_id)
            if vm_info:
                if vm_info['EC2']:
                    self.ui_msg("This VM corresponds to an EC2 instance.")
                elif vm_info['Nimbus']:
                    if vm_info['eprfile']:
                        if self.yes_no(("Really destroy %s (workspace on"
                        " Nimbus cloud %s with EPR file %s)?"
                        % (entered_vm_id,vm_info['cloudindex'],
                        vm_info['eprfile']))):
                            self.session.nimbus_destroy_workspace(
                                cloud_index=vm_info['cloudindex'],
                                eprfile=vm_info['eprfile'],
                                vm_id=vm_info['vm_id'])
                        else:
                            self.ui_msg('aborted.')
                    else:
                        self.logger.error(("No EPR file defined in "
                            "save.session.vms for VM %s" % entered_vm_id))
                else:
                    self.logger.error("This state should never be reached :-)")
            return
        self.ui_msg("better: e.g. 'query_workspace vm-0133'.")

    def show_vm_info(self, cmd):
        """
        Try to read and display VM info from save.session.vms
        """
        words = [word.lower() for word in cmd.split()]
        if len(words) == 2 and words[0] == 'show_vm_info':
            entered_vm_id = words[1]
            vm_info = self.session.get_vm_info_from_file(vm_id=entered_vm_id)
            if vm_info:
                self.ui_msg('\n'+repr(vm_info))
            return
        self.ui_msg("better: e.g. 'show_vm_info vm-0001'.")

    def softkill_vm(self, cmd):
        """
        Instruct to set softkill flag for given VM ID. Check if VM is listed in
        save.session.vms.
        """
        words = [word.lower() for word in cmd.split()]
        if len(words) == 2 and words[0] == 'softkill_vm':
            entered_vm_id = words[1]
            vm_info = self.session.get_vm_info_from_file(vm_id=entered_vm_id)
            if vm_info:
                if self.yes_no("Set softkill flag for VM %s?" % entered_vm_id):
                    self.session.softkill_vm(entered_vm_id)
            return
        self.ui_msg("better: e.g. 'softkill_vm vm-0001'.")        
        
    def set_highest_prio(self, cmd):
        """
        Validate input. Distinguish between higher/lower HP than before.
        Instruct HP change.
        """
        words = [word.lower() for word in cmd.split()]
        if len(words) == 2 and words[0] == 'set_highest_prio':
            try:
                entered_new_hp = int(words[1])
            except ValueError:
                self.logger.error("entered value is not a number.")
                return
            if entered_new_hp == self.session.sdb_session.highest_priority:
                self.ui_msg(("Highest Priority is already set to"
                    % entered_new_hp))
                return
            if entered_new_hp < self.session.sdb_session.highest_priority:
                self.ui_msg(("Entered value is smaller than current HP setting."
                    " Do you really want to delete (a) queue(s)? Deletion"
                    " fails if a queue is not empty."))
            if entered_new_hp > self.session.sdb_session.highest_priority:
                self.ui_msg(("Entered value is higher than current HP setting."
                    " Do you really want to create (a) queue(s)? This increases"
                    " cost a bit."))
            if self.yes_no("Please confirm"):
                self.session.change_highest_priority(entered_new_hp)
            return
        self.ui_msg("better: e.g. 'set_highest_prio 3'.")

    def run_vms(self, cmd):
        """
        check for "run_vms cloud_name number_of_vms" structure.
        E.g.: "run_vms EC2 15" or "run_vms nb1 2"
        invoke run command after re-insurance.
        """
        words = [word.lower() for word in cmd.split()]
        nbcldidcs = self.session.nimbus_cloud_indices()
        if len(words) == 3 and words[0] == 'run_vms':
            cloud = words[1]                 # cloud name
            number = words[2]                # number of vms
            index = self.session.nbx(cloud)  # valid nimbus cloud index or False
            if (cloud == 'ec2' or index) and number.isdigit():
                number = int(number)
                if cloud == 'ec2':
                    if self.yes_no('Run %s VM(s) on EC2?' % number):
                        self.session.run_vms_ec2(number)
                    else:
                        self.ui_msg('aborted.')
                elif self.yes_no(('Run %s VM(s) on Nimbus Cloud %s?'
                %(number,index))):
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
        next_sqs_check_in = min(0, (now -
            (self.sqs_last_checked +
                self.session.inicfg.sqs.monitor_queues_pollinterval)))
        next_sdb_check_in = min(0, (now -
            (self.sdb_last_checked +
                self.session.inicfg.sdb.monitor_vms_pollinterval)))
        self.request_update_uiinfo(dict(
            txt_sqs_upd=str(int(round(next_sqs_check_in))).zfill(5)+" s",
            txt_sdb_upd=str(int(round(next_sdb_check_in))).zfill(5)+" s"))
        if next_sqs_check_in == 0:
            self.logger.info("Automatic SQS job number update triggered.")
            self.sqs_check()
        if next_sdb_check_in == 0:
            self.logger.info("Automatic SDB monitoring data update triggered.")
            self.sdb_check()

    def sdb_check(self):
        """
        Query SDB for state of VMs / JAa
        """
        self.logger.debug("ResourceManagerMainLoop.sdb_check() called")
        # instruct to receive total number of running job agents.
        # if not False, then it is an integer :-)
        nbr_running_jobagents = self.session.sdb_session.count_jobagents()
        if nbr_running_jobagents is not False:
            self.request_update_uiinfo(dict(
                txt_total_nbr_jas=str(nbr_running_jobagents)))
        self.sdb_last_checked = time.time()

    def sqs_check(self):
        """
        Query SQS queues to receive the approximate number of jobs contained in
        them.
        """
        self.logger.debug("ResourceManagerMainLoop.sqs_check() called")
        self.session.sqs_session.query_queues()
        self.sqs_last_checked = time.time()
        stringlist = []
        for p, j in self.session.sqs_session.queue_jobnbrs_laststate.items():
            stringlist.append("P"+str(p).zfill(2)+": %s job(s)" % j)
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
            +"\n* help:                   Display this help message."
            +"\n* quit:                   Quit Resource Manager."
            +"\n* pause:                  Pause main loop (stop automatic operation)."
            +"\n* start:                  Start main loop / continue after break."
            +"\n* run_vms cloud X:        Run X VMs on cloud (EC2|NbY). E.g. 'run_vms Nb1 2'."
            +"\n* query_fact_rp NbX:      Query Nimbus Factory RP."
            +"\n* query_workspace vm-X:   Query Nimbus workspace corresponding to VM ID."
            +"\n* destroy_workspace vm-X: Destroy Nimbus workspace corresponding to VM ID."
            +"\n* show_vm_info vm-X:      Show VM info as stored in save.session.vms."
            +"\n* softkill_vm vm-X:       Set softkill flag for given VM."
            +"\n* set_highest_prio HP:    Set highest priority to HP; create/delete queues."
            +"\n* poll_sdb:               Update SDB monitoring data."
            +"\n* poll_sqs:               Update SQS monitoring data."
            +"\n* write_jmi_cfg_file:     Write Job Management Interface config file.")
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
            config.set(
                'uiinfo'.encode('utf-8'),
                key.encode('utf-8'),value.encode('utf-8'))
        configstring = "%%"+config.write_to_string().decode('utf-8')+"&&"
        os.write(self.pipe_uiinfo_update_write, configstring.encode('utf-8'))
