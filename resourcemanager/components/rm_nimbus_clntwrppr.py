# -*- coding: UTF-8 -*-
#
#   ::::::::> RESOURCE MANAGER <::::::::
#   Nimbus Client Wrapper module
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

import os
import subprocess
import logging
import time


class NimbusClientWrapper(object):
    def __init__(self,  run_id,
                        gridproxyfile,
                        exe,
                        action,
                        workdir,
                        userdata,
                        eprfile,
                        sshfile,
                        metadatafile,
                        serviceurl,
                        requestfile,
                        serviceidentity,
                        displayname,
                        exitstate,
                        polldelay):

        self.logger = logging.getLogger("RM.CloudClientWrapper.%s" % run_id)
        self.logger.debug("initialize NimbusClientWrapper object")

        # check working dir
        if not os.path.exists(workdir):
            self.logger.debug("create workdir %s" % workdir)
            os.makedirs(workdir)
        else:
            self.logger.error("weird.. %s should not have existed"%workdir)

        # save some constructor arguments
        self.workdir = workdir
        self.action = action
        self.eprfile = eprfile

        # create userdata file from string. assume bytestring!!
        userdatafile = os.path.join(workdir,"userdata")
        self.logger.debug("write userdata to file %s" % userdatafile)
        fd = open(userdatafile,'w')
        fd.write(str(userdata)) # this will explode if userdata is not ascii..
        fd.close()

        # set up environment for subprocess:
        self.logger.debug("set environment: 'X509_USER_PROXY'=%s"%gridproxyfile)
        self.environment = {}
        self.environment['X509_USER_PROXY'] = gridproxyfile

        # to be populated
        self.subprocess = None
        self.starttime = None
        self.endtime = None

        # define command to be run
        self.cmdline = []
        self.cmdline.append("/bin/sh")
        self.cmdline.append(exe)
        if action == "deploy":
            self.cmdline.append("--deploy")
            self.cmdline.append("--metadata")
            self.cmdline.append(metadatafile)
            self.cmdline.append("--service")
            self.cmdline.append(serviceurl)
            self.cmdline.append("--request")
            self.cmdline.append(requestfile)
            self.cmdline.append("--file")
            self.cmdline.append(eprfile)
            self.cmdline.append("--authorization")
            self.cmdline.append(serviceidentity)
            self.cmdline.append("--exit-state")
            self.cmdline.append(exitstate)
            self.cmdline.append("--sshfile")
            self.cmdline.append(sshfile)
            self.cmdline.append("--poll-delay")
            self.cmdline.append(polldelay)
            self.cmdline.append("--displayname")
            self.cmdline.append(displayname)
            self.cmdline.append("--mdUserdata")
            self.cmdline.append(userdatafile)
            self.cmdline.append("--debug")
            self.cmdline.append("--dryrun")
        self.logger.debug("assembled shell command: %s" % str(self.cmdline))

    def run(self):
        self.logger.info("run Nimbus Cloud Client as subprocess")

        timestring = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        stdouterr_file_name = "cloudclient_%s_%s.log"%(self.action,timestring)
        self.stdouterr_file_path = os.path.join(self.workdir,stdouterr_file_name)
        self.logger.debug(("open subprocess logfile for writing: %s"
            % self.stdouterr_file_path))
        self.stdouterr_file = open(self.stdouterr_file_path,'w')

        self.logger.debug("run subprocess with cwd %s" % self.workdir)
        self.starttime = time.time()
        self.subprocess = subprocess.Popen(
            args=self.cmdline,
            stdout=self.stdouterr_file,
            stderr=subprocess.STDOUT,
            cwd=self.workdir,
            env=self.environment)

        # self.logger.debug("wait for subprocess to return...")
        # returncode = cloudclient_sp.wait()
        # self.logger.debug("workspace.sh returned with code %s" % returncode)
        # if returncode is not 0:
            # self.logger.critical(("workspace.sh returnecode was "
                                  # "not 0. Check %s" % stdouterr_file_path))
        # elif os.path.exists(self.epr_file):
            # self.logger.info("EPR written to %s" % self.epr_file)
        # else:
            # self.logger.critical("EPR file was not created")

    def get_epr_file_on_success(self):
        """
        Return None if there is no subprocess or process is still running.
        Return False if subprocess returned with other returncode than 0.
        Else: success: Return path to EPR file.
        """
        if self.subprocess is not None:
            returncode = self.subprocess.poll()
            if returncode is not None:
                # mark subprocess as finished
                self.subprocess = None
                self.stdouterr_file.close()
                self.endtime = time.time()
                executiontime = time.strftime("%H:%M:%S",
                    time.gmtime(self.endtime-self.starttime))
                self.logger.info(("subprocess ended after %s with returncode %s"
                    % (executiontime,returncode)))
                if returncode == 0:
                    return self.eprfile
                else:
                    self.logger.error(("Nimbus Cloud Client subprocess ended with"
                                        " an error. Check logfile for details: %s"
                                        % self.stdouterr_file_path))
                    return False
        return None

    def set_up_env_vars(self):
        """
        Currently no more needed
        Set up environment variables needed vor Nimbus Cloud Client run.

        @return: True, if everything was okay. False otherwise.
        """
        if self.nc.grid_proxy_file_path is not None:
            if os.path.exists(self.nc.grid_proxy_file_path):
                self.logger.debug(("set env variable 'X509_USER_PROXY' to %s"
                                    % self.nc.grid_proxy_file_path))
                os.environ['X509_USER_PROXY'] = self.nc.grid_proxy_file_path
                #returncode = subprocess.call(["env > environment.log"],shell=True)
                #print "envvarr test returncode" + str(returncode)
                return True
        self.logger.error("grid proxy file path not defined / does not exist")
        return False

