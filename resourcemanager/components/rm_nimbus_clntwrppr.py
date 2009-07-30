# -*- coding: UTF-8 -*-
#
#   ::::::::> RESOURCE MANAGER <::::::::
#   Nimbus Client Wrapper
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

import os
import subprocess
import logging


class NimbusClientWrapper(object):
    def __init__(self, nimbus_cloud, action, vm_id, session_run_dir):
        """
        @params: ncc: Nimbus Cloud Config object
        """
        self.logger = logging.getLogger("RM.MainLoop.NimbusClientWrapper")
        self.logger.debug("initialize NimbusClientWrapper object")

        self.nc = nimbus_cloud
        self.action = action
        self.vm_id = vm_id
        self.nimbus_cloud_dir = "nimbus_cloud_%s" % self.nc.cloud_index
        self.vm_dir = os.path.join(session_run_dir,
                                   self.nimbus_cloud_dir,
                                   "vm-%s" % self.vm_id)
        self.epr_file = os.path.join(self.vm_dir,"vm-%s-epr.xml" % self.vm_id)
        self.displayname = "cloud-%s-vm-%s" % (self.nc.cloud_index,self.vm_id)

    def set_up_cmdline_params(self):
        self.cmdline = []
        exe = os.path.join(self.nc.inicfg.nimbus_cloud_client_root, "lib/workspace.sh")
        self.cmdline.append("/bin/sh")
        self.cmdline.append(exe)
        if self.action == "deploy":
            self.cmdline.append("--deploy")
            self.cmdline.append("--metadata")
            self.cmdline.append(self.nc.inicfg.metadata_file_path)
            self.cmdline.append("--service")
            self.cmdline.append(self.nc.inicfg.service_url)
            self.cmdline.append("--request")
            self.cmdline.append(self.nc.inicfg.request_file_path)
            self.cmdline.append("--file")
            self.cmdline.append(self.epr_file)
            self.cmdline.append("--authorization")
            self.cmdline.append(self.nc.inicfg.service_identity)
            self.cmdline.append("--exit-state")
            self.cmdline.append("Running")
            self.cmdline.append("--sshfile")
            self.cmdline.append(self.nc.inicfg.ssh_pubkey_file_path)
            self.cmdline.append("--poll-delay")
            self.cmdline.append("4000")
            self.cmdline.append("--displayname")
            self.cmdline.append(self.displayname)
            self.cmdline.append("--debug")
        self.logger.debug("assembled shell command: %s" % str(self.cmdline))

    def run(self):
        timestring = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        stdouterr_file_name = "cloudclient_%s_%s_.log"%(self.action,timestring)
        if not os.path.exists(self.vm_dir):
            os.makedirs(self.vm_dir)
        stdouterr_file_path = os.path.join(self.vm_dir,stdouterr_file_name)
        stdouterr_file = open(stdouterr_file_path,'w')

        cloudclient_sp = subprocess.Popen(args=self.cmdline,
                                    stdout=stdouterr_file,
                                    stderr=subprocess.STDOUT,
                                    cwd=self.vm_dir)
        self.logger.debug("wait for subprocess to return...")
        returncode = cloudclient_sp.wait()
        self.logger.debug("workspace.sh returned with code %s" % returncode)
        if returncode is not 0:
            self.logger.critical(("workspace.sh returnecode was "
                                  "not 0. Check %s" % stdouterr_file_path))
        elif os.path.exists(self.epr_file):
            self.logger.info("EPR written to %s" % self.epr_file)
        else:
            self.logger.critical("EPR file was not created")

    def set_up_env_vars(self):
        self.logger.debug(("set env variable 'X509_USER_PROXY' to %s"
                            % self.nc.grid_proxy_file_path))
        os.environ['X509_USER_PROXY'] = self.nc.grid_proxy_file_path
        returncode = subprocess.call(["env > environment.log"],shell=True)
        #print "envvarr test returncode" + str(returncode)