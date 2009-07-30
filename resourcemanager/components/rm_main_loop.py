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
        start_options = self.start_options
        self.logger.info("thread started.")
        self.session = Session(start_options, self.session_dirs)
        self.logger.info("LOAD INITIAL SESSION CONFIGURATION...")
        self.session.load_initial_session_config()
        self.logger.info("FINISH SESSION SETUP...")

        cloudstring = 'Clouds: '
        if self.session.inicfg.ec2.use:
            cloudstring += 'EC2, '
        for nimbus_cloud in self.session.nimbus_clouds:
            cloudstring += 'Nb%s, ' % nimbus_cloud.cloud_index
        self.request_update_uiinfo(dict(
        txt_cloud=cloudstring.rstrip(", "),
        txt_name=self.session.inicfg.session.name))

        self.session.finish_setup()
        self.main_loop()

    def main_loop(self):
        self.logger.debug("main_loop() started.")
        #self.request_update_uiinfo(dict(txt_cloud='BOAH ALDA EY KRASS!'))
        #self.txt_cloud.set_text(self.uiinfo_dict['txt_cloud'])
        #self.txt_sqs_upd.set_text(self.uiinfo_dict['txt_sqs_upd'])
        #self.txt_sdb_upd.set_text(self.uiinfo_dict['txt_sdb_upd'])
        sqs_last_checked = sdb_last_checked = time.time()
        while True:
            try:
                uicmd = self.queue_uicmds.get(block=True, timeout=1)
            except Queue.Empty:
                # os.write(self.pipe_cmdresp_write, '1 sec and got nothing :-(')
                uicmd = None
            if uicmd == 'quit':
                os.write(self.pipe_cmdresp_write, 'end RM main loop')
                break
                # continue statement .. really quit? enter again..
            elif uicmd is not None:
                os.write(self.pipe_cmdresp_write, uicmd.encode('utf-8'))

            now = time.time()
            next_sqs_check_in = abs(min(0, (now -
                (sqs_last_checked + self.session.inicfg.sqs.monitor_queues_pollinterval))))
            next_sdb_check_in = abs(min(0, (now -
                (sdb_last_checked + self.session.inicfg.sdb.monitor_vms_pollinterval))))
            self.request_update_uiinfo(dict(
                txt_sqs_upd="SQS update: "+str(int(round(next_sqs_check_in))).zfill(5)+" s",
                txt_sdb_upd="SDB update: "+str(int(round(next_sdb_check_in))).zfill(5)+" s"))

            if next_sqs_check_in == 0:
                self.logger.info("SQS monitoring triggered.")
                self.session.sqs_session.query_queues()
                sqs_last_checked = time.time()
                stringlist = []
                for prio, jobnr in self.session.sqs_session.queue_jobnbrs_laststate.items():
                    stringlist.append("P"+str(prio).zfill(2)+": %s job(s)" % jobnr)
                self.request_update_uiinfo(dict(txt_sqs_jobs="\n".join(stringlist)))

            if next_sdb_check_in == 0:
                self.logger.info("SDB monitoring triggered.")
                sdb_last_checked = time.time()

             #self.session.run_vm()

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
