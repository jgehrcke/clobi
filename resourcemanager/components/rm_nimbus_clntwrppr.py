# -*- coding: UTF-8 -*-
#
#   ::::::::> Clobi RESOURCE MANAGER <::::::::
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
import traceback


class NimbusClientWrapper(object):
    def __init__(self,  exe,
                        workdir,
                        gridproxyfile,
                        action,
                        run_id,
                        eprfile=None,
                        serviceurl=None,
                        userdata=None,
                        sshfile=None,
                        metadatafile=None,
                        requestfile=None,
                        serviceidentity=None,
                        displayname=None,
                        exitstate=None,
                        polldelay=None):

        self.logger = logging.getLogger("RM.CloudClientWrapper.%s" % run_id)
        self.logger.debug("initialize NimbusClientWrapper object")

        # save some constructor arguments
        self.workdir = workdir
        self.action = action

        # to be populated
        self.subprocess = None
        self.starttime = None
        self.endtime = None

        # misconfigured
        # http://www.abc.net.au/newsradio/txt/s1908113.htm
        # "I too failed to find “misconfigured” in any of the online dictionaries
        # – including the giants: Oxford and Webster’s. It’s also not in most
        # computer spell-checkers. But it’s clearly a real word in common use
        # since Google can find countless websites dealing with “misconfigured”
        # software. This is just a case of the dictionaries having not yet caught
        # up with the real world – but they will. By the way, one colleague
        # insists that “misconfigured” is a new movie starring Sandra Bullock."
        # :-)
        self.misconfigured = False

        # check working dir
        if isinstance(workdir, basestring):
            if not os.path.exists(workdir):
                self.logger.debug("create workdir %s ..." % workdir)
                try:
                    os.makedirs(workdir)
                except:
                    self.logger.error("error while creating %s" % workdir)
                    traceback.print_exc()
                    self.misconfigured = True
            else:
                self.logger.debug("workdir %s already existed" % workdir)
        else:
            self.logger.error("given workdir argument no basestring")
            self.misconfigured = True

        # set up grid proxy environment variable for subprocess:
        if isinstance(gridproxyfile, basestring):
            if not os.path.isfile(gridproxyfile):
                self.logger.debug(("grid proxy file does not exist: %s"
                    % gridproxyfile))
                self.misconfigured = True
            else:
                self.logger.debug(("set environment: 'X509_USER_PROXY'=%s"
                    % gridproxyfile))
                self.environment = {}
                self.environment['X509_USER_PROXY'] = gridproxyfile
        else:
            self.logger.error("given gridproxyfile argument no basestring")
            self.misconfigured = True

        # start defining command to be run, check executable
        if isinstance(exe, basestring):
            if not os.path.isfile(exe):
                self.logger.debug(("executable does not exist: %s"
                    % exe))
                self.misconfigured = True
            else:
                self.cmdline = []
                self.cmdline.append("/bin/sh")
                self.cmdline.append(exe)
        else:
            self.logger.error("given exe argument no basestring")
            self.misconfigured = True

        if not self.misconfigured:
            if action == "deploy":
                # create userdata file from string. assume bytestring!!
                userdatafile = os.path.join(workdir,"deploy.userdata")
                self.logger.debug("write userdata to file %s" % userdatafile)
                try:
                    fd = open(userdatafile,'w')
                    fd.write(str(userdata))
                    fd.close()
                except:
                    traceback.print_exc()
                    self.misconfigured = True
                    self.logger.error(("error while writing userdata %s to file"
                        % repr(userdata)))

                if (not os.path.isfile(metadatafile) or
                not os.path.isfile(requestfile) or
                not os.path.isfile(sshfile) or
                not os.path.isfile(userdatafile)):
                    self.misconfigured = True
                    self.logger.error(("at least one of given files does not"
                        "exist: %s, %s, %s, %s, %s" % (metadatafile,
                        requestfile, sshfile, userdatafile)))

                if not os.path.isdir(os.path.dirname(eprfile)):
                    self.logger.error(("directory of given EPR file path does"
                        " not exist: %s" % os.path.dirname(eprfile)))
                    self.misconfigured = True

                if (not serviceurl or not serviceidentity or not exitstate or
                not polldelay or not displayname):
                    self.logger.error(("at least one of these arguments "
                        "missing: serviceurl, serviceidentity, exitstate, "
                        "polldelay, displayname"))
                    self.misconfigured = True

                if not self.misconfigured:
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
                    #self.cmdline.append("--dryrun")
            elif action == "factoryrp":
                if not serviceurl:
                    self.logger.error("service url argument missing")
                    self.misconfigured = True
                if not self.misconfigured:
                    self.cmdline.append("--factoryrp")
                    self.cmdline.append("--service")
                    self.cmdline.append(serviceurl)
                    #self.cmdline.append("--dryrun")
            elif action == "rpquery":
                if not os.path.isfile(eprfile):
                    self.logger.error("eprfile doesn't exist: %s" %eprfile)
                    self.misconfigured = True
                if not self.misconfigured:
                    self.cmdline.append("--rpquery")
                    self.cmdline.append("--eprFile")
                    self.cmdline.append(eprfile)
                    #self.cmdline.append("--dryrun")
            elif action == "destroy":
                if not os.path.isfile(eprfile):
                    self.logger.error("eprfile doesn't exist: %s" %eprfile)
                    self.misconfigured = True
                if not self.misconfigured:
                    self.cmdline.append("--destroy")
                    self.cmdline.append("--eprFile")
                    self.cmdline.append(eprfile)
                    #self.cmdline.append("--dryrun")
            else:
                self.logger.error("unknown action: %s" % action)
                self.misconfigured = True

        if not self.misconfigured:
            self.logger.debug("assembled shell command: %s" % str(self.cmdline))

    def run(self):
        """
        Create stdout/stderr log file and start subprocess.
        Return True when there was not misconfigured and no Exception.
        Return False when misconfigured.
        """
        if not self.misconfigured:
            self.logger.info("run Nimbus Cloud Client as subprocess")

            timestring = time.strftime("%Y%m%d-%H%M%S", time.localtime())
            stdouterr_file_name = ("cloudclient_%s_%s.log"
                %(self.action,timestring))
            self.stdouterr_file_path = os.path.join(
                self.workdir,
                stdouterr_file_name)
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
            return True
        else:
            self.logger.error(("Nimbus Cloud Client run blocked because of"
                " wrong configuration"))
            return False

    def was_successfull(self):
        """
        Return None if there is no subprocess or process is still running.
        Return False if subprocess returned with other returncode than 0.
        Else: success: Return True.
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
                    return True
                else:
                    self.logger.error(("Nimbus Cloud Client subprocess ended "
                        " with an error. Check logfile for details: %s"
                        % self.stdouterr_file_path))
                    return False
        return None
