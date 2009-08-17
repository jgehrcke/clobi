# -*- coding: UTF-8 -*-
#
#   ::::::::> Clobi Ganga Backend <::::::::
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
#########################################################################

__author__  = "Jan-Philip Gehrcke <jgehrcke@googlemail.com>"
__date__    = "some date"
__version__ = "some version"

import os

from Ganga.Core import Sandbox
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers


from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.Utility.ColourText import Foreground, Effects

import Ganga.Utility.Config
import Ganga.Utility.logging

# there are two ways of making boto import work in components.clobi_jmi:
# 1) keeping boto in components and do path modification and boto import here
# 2) move boto to same level as Clobi.py  and just do "import boto" in
#    components.clobi_jmi
# solution 1:
#sys.path.append(os.path.join(os.path.dirname(
#    os.path.realpath( __file__ )),'components'))
#import boto
from components.clobi_jmi import ClobiJobManagementInterface

"""
Set up logger and Config
"""
logger = Ganga.Utility.logging.getLogger()
logconfig = Ganga.Utility.Config.getConfig("Logging")
config = Ganga.Utility.Config.makeConfig(
    'ClobiJMIConfig',
    'Clobi Job Management Interface configuration')
config = Ganga.Utility.Config.getConfig("ClobiJMIConfig")
config.addOption('jmi_aws_accesskey','not_set','AWS accesskey')
# I don't want this option to appear in any log (and it should not be viewable
# from GPI?!). Don't know exactly what `hidden=True` means, but the attribute
# is still available in GPI via `config.ClobiJMIConfig`
config.addOption('jmi_aws_secretkey','not_set','AWS secretkey',hidden=True)
config.addOption('jmi_session_id','not_set','Clobi session ID')
config.addOption(
    'jmi_sandbox_storage_service',
    'not_set',
    'sandbox storage svc (S3,..)')
config.addOption('jmi_sandbox_bucket','not_set','sandbox bucket on storage svc')


class Clobi(IBackend):
    """Clobi backend - submit jobs to a Clobi resource pool (VMs on clouds like
    EC2 and Nimbus.
    """
    _schema = Schema(Version(1,0),
        {"id" : SimpleItem(defvalue="", protected=1, copyable=0,
         doc = "Clobi job ID" ),
         "job_owner" : SimpleItem(defvalue="none", copyable=1,
         doc = "Clobi job owner; builds hash in job ID" ),
         "priority" : SimpleItem(defvalue=1, copyable=1,
         doc = "Clobi job priority (higher: 'faster')" ),
         "status" : SimpleItem(defvalue="", protected=1, copyable=0,
         doc = "Clobi status"),})
    _category = "backends"
    _name =  "Clobi"

    def __init__(self):
        pass
        super(Clobi, self).__init__()

    def gather_clobi_job_config(self, ganga_jobconfig):
        """
        Gather job configuration from current `ganga_jobconfig` and return Clobi
        job config dictionary
        """
        job = self.getJobObject()

        # gather Clobi Job Config attributes:
        # (executable, job_owner, input_sandbox_files, output_sandbox_files,
        # production_system_job_id)

        job_config = {}
        # set clobi job config input_sandbox_files.
        # this setting will result in all content of `inbox_dir` recursively
        # added to Clobi's main input sandbox tar BZ2 archive.
        inbox_dir = os.path.abspath(job.getInputWorkspace().getPath())
        job_config['input_sandbox_files'] = "-C %s ." % inbox_dir

        job_config['output_sandbox_files'] = ';'.join(job.outputsandbox)

        # set clobi job config executable. actually, this can be a whole cmd
        # string. build it up here, containing the executable file and cmdline
        # arguments.
        exestr = ganga_jobconfig.getExeString().strip()
        quoted_arg_list = []
        for arg in ganga_jobconfig.getArgStrings():
            quoted_arg_list.append("\\'%s\\'" % arg)
        cmdstr = " ".join([exestr] + quoted_arg_list)
        job_config['executable'] = cmdstr

        job_config['owner'] = job.backend.job_owner
        job_config['priority'] = str(job.backend.priority)
        job_config['production_system_job_id']  = str(job.id)

        return job_config



    def submit(self, jobconfig, master_input_sandbox):
        """Submit job to backend.
        Return value: True if job is submitted successfully,
        or False otherwise
        """
        try:

            # current limitations:
            # no args, no env, only Executable Application

            job = self.getJobObject()
            # copy files to input sandbox directory (*not* compressed)
            # but: this creates an "_input_sandbox_NR_master.tgz" file, too.
            # I want the master input sandbox uncompressed!
            job.createInputSandbox(jobconfig.getSandboxFiles())
            inbox_dir = os.path.abspath(job.getInputWorkspace().getPath())

            # ----------------------------------------------------------------------
            # gather Clobi Job Management Interface cfg attributes and create it!
            # (jmi_aws_accesskey, jmi_aws_secretkey, jmi_session_id,
            # jmi_sandbox_storage_service, jmi_sandbox_bucket)
            # ----------------------------------------------------------------------
            jmi_config = {}
            jmi_config['aws_secretkey'] = config['jmi_aws_secretkey']
            jmi_config['aws_accesskey'] = config['jmi_aws_accesskey']
            jmi_config['sandbox_bucket'] = config['jmi_sandbox_bucket']
            jmi_config['sandbox_storage_service'] = config['jmi_sandbox_storage_service']
            jmi_config['jmi_session_id'] = config['jmi_session_id']
            clobijmi = ClobiJobManagementInterface(
                jmi_config,jmi_sandboxarc_dir=inbox_dir,logging_logger=logger)
            clobi_job_config = self.gather_clobi_job_config(jobconfig)
            clobi_job_id = clobijmi.generate_job_id(clobi_job_config)
            clobijmi.submit_job(clobi_job_config, clobi_job_id)

            # print
            # print jobconfig.getSandboxFiles()
            # print
            # inbox =
            # inbox = job.createPackedInputSandbox( jobconfig.getSandboxFiles() )
            # print inbox
            # print

            # for file in os.listdir(inbox_dir):
                # print file

            # inbox = job.createPackedInputSandbox( jobconfig.getSandboxFiles() )
            # inpDir = job.getInputWorkspace().getPath()
            # outDir = job.getOutputWorkspace().getPath()



            # cdfpath = self.preparejob(jobconfig, master_input_sandbox)
            # status = self.submit_cdf(cdfpath)
            # return status
        except:
            import traceback
            logger.error("Traceback:\n%s"%traceback.format_exc())
        return False


    def resubmit( self ):
        """Resubmit job that has already been configured.

          Return value: True if job is resubmitted successfully,
                        or False otherwise"""

        pass


    def kill( self  ):
        """Kill running job.

         No arguments other than self

         Return value: True if job killed successfully,
                       or False otherwise"""
        pass



    def preparejob( self, jobconfig, master_input_sandbox ):
        """Prepare Condor description file"""

        pass

    def updateMonitoringInformation( jobs ):
        pass

    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

"""
Declare Clobi Job Config and Clobi Runtime Handler.
Add ClobiRTHandler for Executable application to allHandlers.
"""

class ClobiJobConfig(StandardJobConfig):
    """
    I support job_owner and priority,
    currently no env, args, inputdata
    """
    def __init__(self, exe=None, inputbox=[], outputbox=[], owner=None,
    priority=1, args=[], env=None):
        if owner is None:
            logger.error("Please define a job owner via job.owner='name'")
        if env is not None:
            logger.error("Clobi backend currently does not support exe env")
            env = {}
        self.owner = str(owner)
        StandardJobConfig.__init__(self,exe,inputbox,args,outputbox,env)

    def getArguments(self):
        return ' '.join(self.getArgStrings())

    def getExecutable(self):
        exe=self.getExeString()
        if os.path.dirname(exe) == '.':
            return os.path.basename(exe)
        else:
            return exe


class ClobiRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        return ClobiJobConfig(
            app.exe,
            app._getParent().inputsandbox,
            app.args,app._getParent().outputsandbox,
            app.env)


allHandlers.add('Executable','Clobi', ClobiRTHandler)
