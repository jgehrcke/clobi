# -*- coding: UTF-8 -*-
#
#   ::::::::> Clobi JOB AGENT <::::::::
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

import optparse
import logging
import time
import os
import sys
import traceback
import base64
import subprocess
import tarfile

from components.cfg_parse_strzip import SafeConfigParserStringZip
from components.utils import *

sys.path.append("components")
import boto


JOB_WORK_BASEDIR = "/mnt/jobs_workdir/"
JOB_LOG_BASEDIR = "/mnt/jobs_logdir/"

def main():
    # define and create Job Agent's log dir (for stdout / stderr of this script)
    jobagent_logdir = "jobagent_log"
    jobagent_logdir = os.path.abspath(jobagent_logdir)
    if not os.path.exists(jobagent_logdir):
        os.makedirs(jobagent_logdir)

    # log stderr to stderr and to a real file
    stderr_logfile_path = os.path.join(
        jobagent_logdir,
        "%s_JA_stderr.log" % utc_timestring())
    stderr_log_fd = open(stderr_logfile_path,'w')
    sys.stderr = Tee(sys.stderr, stderr_log_fd)

    # set up logger
    rootlog = JobAgentLogger(jobagent_logdir)
    logger.debug("parse commandline arguments..")
    start_options = parseargs()
    try:
        jobagent = JobAgent(
            start_options=start_options,
            boto_logfile_path=rootlog.get_boto_log_file_path(),
            ja_logfile_path=rootlog.get_ja_log_file_path(),
            ja_stderr_logfile_path=stderr_logfile_path)
        # this main method does *all* the work. In a proper use case, the script
        # won't return from this method. Instead, machine shutdown should be
        # invoked within this method, remotely controlled via SDB flag.
        jobagent.main()
    except:
        logger.critical("Job Agent ended exceptionally. Try to save some logs.")
        logger.critical("Traceback:\n%s"%traceback.format_exc())
        try:
            jobagent.compress_upload_log()
        except:
            logger.critical("Could not save logs. Error:")
            logger.critical("Traceback:\n%s"%traceback.format_exc())
    finally:
        stderr_log_fd.close()
        # Here, one could invoke a system shutdown command. My first
        # decision is not to do so, because it will be difficult to debug a
        # system that automatically shuts itself down after an error
        # occured. Maybe logfile saving was not possible: Then, the only way to
        # debug is to log into the VM via ssh and look what's going.
        # On the other hand, starting many VMs which won't shut down
        # properly because of an error in jobagent.main() will result in many
        # VMs that have to be detected as "failed" and have to be shut down
        # manually.


class SimpleDB(object):
    """
    Set up SimpleDB for this job agent. Provide an interface to SDB (all
    job agent communication with SDB is done via this class)
    """
    def __init__(self, initial_jobagent_config):
        self.logger = logging.getLogger("jobagent.py.SimpleDB")
        self.logger.debug("initialize SimpleDB object")

        # constructor arguments
        self.inicfg = initial_jobagent_config

        # init boto SDB connection
        self.logger.debug("create SimpleDB connection object")
        self.sdbconn = boto.connect_sdb(self.inicfg.aws_accesskey,
                                        self.inicfg.aws_secretkey)

        # to be populated
        self.boto_domainobj_session = None
        self.boto_domainobj_jobs = None

        # init domain names for session data (session & VM data) and job data
        # this is hard coded in the Job Agent *and* in the Resource Manager
        self.domain_name_session = self.inicfg.sessionid+"_sess"
        self.domain_name_jobs = self.inicfg.sessionid+"_jobs"

    def check_domains(self):
        """
        Call self.check_domain() for all domains this session needs.
        """
        self.logger.info("check SDB domain for session & VM data...")
        self.boto_domainobj_session = self.check_domain(
            self.domain_name_session)
        self.logger.info("check SDB domain for job data...")
        self.boto_domainobj_jobs = self.check_domain(
            self.domain_name_jobs)

    def check_domain(self, domainname):
        """
        Check a domain in several steps:
            - look up domain (is it already there?)
                no: this must not happen, error & exit
                yes: return boto domain object
        """
        self.logger.debug("look up SDB domain "+domainname+" ...")
        domainobj = self.sdbconn.lookup(domainname)
        if domainobj is None:
            self.logger.critical(("SDB domain does not exist: %s. But it must!"
                " Exit." % domainname))
            sys.exit(1)
        self.logger.info("SDB domain "+domainname+" is now available.")
        return domainobj

    def get_highest_priority(self):
        """
        Get HighestPriority flag from SDB.
        """
        self.logger.debug("Retrieve HighestPriority item from SDB")
        try:
            item = self.boto_domainobj_session.get_item('session_props')
        except:
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return None
        if item is not None:
            try:
                hp = item['HighestPriority']
                self.logger.info(("got HighestPriority from SDB: %s" % hp))
                return hp
            except KeyError:
                self.logger.critical(("No 'HighestPriority' value set in SDB!"
                    " Means that something went very wrong. It must be set by"
                    " RM before any VM is started up. I'll exit now"))
                sys.exit(1)
        else:
            self.logger.critical(("No 'session_props' item set in SDB! This"
                " means that something went very wrong. It must be set by RM"
                " before any VM is started up. I'll exit now"))
            sys.exit(1)

    def register_ja_started(self, second=False):
        """
        Register VM/Job Agent as 'JA_running' in SDB. If an error occures, wait
        some time and simply try again. If an error appears again, return False.
        This will result in JA & VM shutdown.
        Try two times, because a "random web service error" at this point can
        kick off the whole VM.
        """
        self.logger.info("Try to register JA as started in SDB...")
        error = False
        try:
            item = self.boto_domainobj_session.get_item(self.inicfg.vm_id)
            timestr = utc_timestring()
            if item is not None:
                self.logger.info(("updating SDB item %s with status=JA_running "
                    "and 'JA_startuptime=%s'" % (self.inicfg.vm_id, timestr)))
                item['status'] = 'JA_running'
                item['JA_startuptime'] = timestr
                item['nbr_cores'] = self.inicfg.nbr_cores
                item.save()
                # at this point everything was successfull. return True.
                return True
            else:
                self.logger.critical(("No %s item set in SDB! This"
                    " means that something went very wrong. It must be set by"
                    " RM before any VM is started up. Hence, I am an out-of-"
                    "control-VM and will shut down " % self.inicfg.vm_id))
                return False
        except:
            self.logger.critical(("ERR while receiving/changing item %s"
                %self.inicfg.vm_id))
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())

        if not second:
            self.logger.error(("There was an error while registering this VM"
                " with SDB. Wait 30 seconds and try again."))
            time.sleep(30)
            return self.register_ja_started(second=True)
        self.logger.error(("This is the second time register_ja_started() was"
            " called and the second time an exceptional error appeared."))
        return False

    def set_shutdown_triggered(self):
        """
        The Job Agent is in shutdown phase. Set this information in SDB.
        """
        try:
            self.logger.info(("VM %s: set 'status' value to"
                " 'JA_shutdown_triggered'"%(self.inicfg.vm_id)))
            temp = {}
            temp['status'] = "JA_shutdown_triggered"
            temp['JA_shutdowntime'] = utc_timestring()
            item = self.boto_domainobj_session.put_attributes(
                item_name=self.inicfg.vm_id,
                attributes=temp)
            return True
        except:
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False

    def poll_job_kill_flag(self, job_id):
        """
        Query SDB for a special job item in jobs' domain. Look for the kill flag
        """
        try:
            self.logger.debug("Job %s: Check if job kill flag is set..."%job_id)
            item = self.boto_domainobj_jobs.get_attributes(
                item_name=job_id,
                attribute_name='kill_flag')
            if 'kill_flag' in item:
                if item['kill_flag'] == '1':
                    self.logger.info("Job %s: kill_flag set to 1 in SDB"%job_id)
                    return True
                else:
                    self.logger.debug("Job %s: kill_flag set; not 1."%job_id)
            else:
                self.logger.debug("Job %s: kill_flag not set."%job_id)
        except:
            self.logger.debug(("Job %s: could not retrieve jobs' kill flag."
                " don't kill :-)" % job_id))
        return False

    def poll_vm_softkill_flag(self, item=None):
        """
        Query SDB for soft kill flag 'VM_softkill_flag' Return True if set to
        '1'; False otherwise.

        @params: job_id: Job ID (string)
                 item: boto SDB item object. if given, this is used and
                       and evaluated (saves one web service call).
        """
        self.logger.debug(("Check if VM softkill flag is set in SDB"))
        try:
            if item is None:
                item = self.boto_domainobj_session.get_attributes(
                    item_name=self.inicfg.vm_id,
                    attribute_name='VM_softkill_flag')
            if 'VM_softkill_flag' in item:
                if item['VM_softkill_flag'] == '1':
                    self.logger.info("VM_softkill_flag set to 1 in SDB")
                    return True
                else:
                    self.logger.debug("VM_softkill_flag set, but not 1.")
            else:
                self.logger.debug("VM_softkill_flag not set.")
        except:
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
        return False

    def update_job_state(self, job_id, state, returncode=None):
        """
        Update job item in sdb with new state: "get_input"
        """
        try:
            self.logger.info("Job %s: set 'status' value to %s"%(job_id,state))
            temp = {}
            temp['status'] = state
            if state == 'running':
                temp['runstarttime'] = utc_timestring()
            if state == 'killed':
                temp['killtime'] = utc_timestring()
            if state == 'save_output':
                temp['savestarttime'] = utc_timestring()
                temp['returncode'] = str(returncode)
            if state == 'completed_error' or state == 'completed_success':
                temp['completedtime'] = utc_timestring()
            item = self.boto_domainobj_jobs.put_attributes(
                item_name=job_id,
                attributes=temp)
            return True
        except:
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False

    def initialize_job(self, job_id, sqs_msg_id, job_machine_index):
        """
        Update SDB that we've received a job. The expected case is that an item
        with the job id is NOT existing in the jobs' SDB domain.

        Later on, this method should be extended to edit VMs' SDB domain, too:
        - for one VM item all job ID's should be listed (this VM got via SQS)
        """

        # at first create JOB item in JOB SDB domain
        try:
            self.logger.debug(("Try to receive SDB item %s from domain %s"
                % (job_id,self.boto_domainobj_jobs.name)))
            item = self.boto_domainobj_jobs.get_item(job_id)
        except:
            self.logger.critical("SDB error")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False
        if item is not None:
            if 'kill_flag' in item and item['kill_flag'] == '1':
                # the job is marked to be killed even before processing has
                # started. The outer fct will delete the SQS msg.
                self.logger.info(("Job %s is already marked to be killed"
                    % job_id))
                return 'job_killed'
            if 'status' in item and item['status'] == 'removed':
                # the job is marked as "removed" in SDB
                # (there is no way to delete an SQS message once
                # sent, it has to be received and then deleted via
                # receipt handle. Hence, to delete a job, it must
                # be marked as removed in SDB, which was checked here.
                # The outer fct will delete the SQS msg.)
                self.logger.info("Job %s is marked as removed in SDB."%job_id)
                return 'job_removed'
            if ('status' in item and
            (item['status'] == 'initialized' or item['status'] == 'running')):
                # This case is very unlikely: two JAs received the same job
                # msg. When we are here, the time difference was big enough so
                # that SDB could act as a "rescuer" here. Real concurrent
                # access / atomicity is not a feature of SDB, but I hope that
                # this here does it. If not, then one job is really processed
                # two times and the job item in SDB will be written by two
                # JA's :-(
                self.logger.error(("SDB item %s is already existing in %s."
                    % (job_id, self.boto_domainobj_jobs.name)))
                return 'reject_job'
            else:
                self.logger.critical(("SDB item %s is already existing."
                    % job_id))
                return False
        try:
            self.logger.info(("Item %s did not exist (as expected). Create"
                " it: set status/inittime/sqs_message_id" % job_id))
            item = self.boto_domainobj_jobs.new_item(job_id)
            item['status'] = 'initialized'
            item['inittime'] = utc_timestring()
            item['sqs_message_id'] = sqs_msg_id
            item.save()
        except:
            self.logger.critical("SDB error")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False

        # now edit VM item in SESSION SDB domain to modify the number
        # of jobs initialized by this JobAgent / VM. If this does not succeed,
        # do not return False because it's not crucial
        try:
            self.logger.info(("VM %s: set 'nbr_jobs_initialized' value to %s"
                % (self.inicfg.vm_id, job_machine_index)))
            temp = {}
            temp['nbr_jobs_initialized'] = str(job_machine_index)
            item = self.boto_domainobj_session.put_attributes(
                item_name=self.inicfg.vm_id,
                attributes=temp)
        except:
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())

        return 'success'


class SQS(object):
    """
    Set up SQS for this session. Provide an interface to SQS (all
    ResourceManager communication with SQS is done via this class)
    """
    def __init__(self, initial_jobagent_config, initial_highest_priority):
        self.logger = logging.getLogger("jobagent.py.SQS")
        self.logger.debug("initialize SQS object")

        # constructor arguments
        self.inicfg = initial_jobagent_config
        self.highest_priority = int(initial_highest_priority)

        self.prefix = self.inicfg.sessionid
        self.suffix = "_P"

        # init boto objects
        self.logger.debug("create SQS connection object")
        self.sqsconn = boto.connect_sqs(self.inicfg.aws_accesskey,
                                        self.inicfg.aws_secretkey)

        # to be populated..
        self.queues_priorities_botosqsqueueobjs = {}
        self.queues_priorities_names = {}

        self.generate_queues_priorities_names()

    def generate_queues_priorities_names(self):
        """
        Populate self.queues_priorities_names:
        For each priority put together a string: the queue name. Then put it
        into a dict with the priority (integer) as key.
        """
        self.highest_priority = int(self.highest_priority)
        self.queues_priorities_names = {}
        for p in range(1,self.highest_priority+1):
            self.queues_priorities_names[p] = ("%s%s%s"
                % (self.prefix, self.suffix, p))
        outputstring = '\n'.join(
                      ["         queue name for priority %s: %s" % (key,value)
                       for key, value in self.queues_priorities_names.items()])
        self.logger.info(("generated SQS priority/queuename pairs:\n"
                          + outputstring))

    def query_queues(self):
        """
        Perform a 'GetQueueAttributes' request on all session queues; only
        receive approximate number of messages.
        """
        self.logger.debug("get attributes for all SQS queues...")
        for prio, queue  in self.queues_priorities_botosqsqueueobjs.items():
            attr = queue.get_attributes(
                attributes="ApproximateNumberOfMessages")
            jobnbr = attr['ApproximateNumberOfMessages']
            self.queue_jobnbrs_laststate[prio] = jobnbr
            self.logger.info(("queue for priority %s:\napprox nbr of jobs: %s"
                 % (prio, jobnbr)))

    def check_queues(self):
        """
        Populate self.queues_priorities_botosqsqueueobjs with boto Queue objects
        Check queues in several steps:
            - Retrieve all existing SQS queues with this session prefix
            - take self.queues_priorities_names (containing priority/namy pairs
              this session NEEDS)
                - for each name in there look if a corresponding queue
                  exists online
                    yes: store boto queue object to
                         self.queues_priorities_botosqsqueueobjs
                    no: must not happen-> EXIT
        """
        self.logger.info("check queues with prefix "+self.prefix)
        try:
            existing_queues = self.sqsconn.get_all_queues(prefix=self.prefix)
        except:
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False

        # Here, two possibilities exist:
        # 1) MORE queues than before or LESS queues than before.
        # To avoid deletion of a needed *and* existing boto queue object,
        # a temporary dict is created, populated with items from the old
        # dict if necessary or with new boto queue objects. After this,
        # the "old original dict" is replaced with this temp dict to build
        # the "new original dict". Boto queue objects that are no more
        # needed won't survive this process; intentionally.
        queues_priorities_botosqsqueueobjs_temp = {}
        error = False
        for prio, queue_name in self.queues_priorities_names.items():
            self.logger.info(("check queue %s for priority %s"
                % (queue_name,prio)))
            found = False
            for q in existing_queues:
                if q.url.endswith(queue_name):
                    self.logger.info(("SQS queue %s for priority %s is"
                        " existing" % (q.url,prio)))
                    found = True
                    queues_priorities_botosqsqueueobjs_temp[prio] = q
                    break
            if not found:
                    error = True
                    self.logger.critical(("SQS queue for priority %s is not "
                        "existing!" % prio))
                    self.logger.debug(("Try to save boto queue object from old"
                        " original dict"))
                    if prio in self.queues_priorities_botosqsqueueobjs:
                        found = True
                        queues_priorities_botosqsqueueobjs_temp[prio] = (
                            self.queues_priorities_botosqsqueueobjs[prio])
                        selb.logger.debug(("Queue object for priority %s"
                            " restored. But it's only a try and"
                            " likely the access to this queue will fail, since"
                            " it wasn't returned by `get_all_queues`."))
            if found:
                self.logger.info(("boto SQS queue object for priority %s"
                    " is now accessible: %s" % (prio, q.url)))
        self.queues_priorities_botosqsqueueobjs = (
            queues_priorities_botosqsqueueobjs_temp)
        if error:
            return False
        return True

    def get_job(self):
        """
        Step through `self.queues_priorities_botosqsqueueobjs` from high to
        low priorities and try to receive *one* new message, representing one
        new job.
        """
        keys = self.queues_priorities_botosqsqueueobjs.keys()
        keys.sort()
        for prio in reversed(keys):
            self.logger.debug(("Retrieve message from queue for priority %s..."
                % prio))
            try:
                message = self.queues_priorities_botosqsqueueobjs[prio].read()
            except:
                self.logger.critical("Traceback:\n%s"%traceback.format_exc())
                message = None
            if message is not None:
                self.logger.info(("Received SQS message with ID %s and Receipt"
                    " Handle %s from Queue %s."
                    % (message.id, message.receipt_handle, message.queue.name)))
                return message
            else:
                self.logger.debug("got no message.")
        return False

    def update_highest_priority(self, new_highest_priority_value):
        """
        Compare new HP value with old. If changed, invoke complete Queues
        initialization (name generation and boto SQS queue object init)
        """
        if self.highest_priority != new_highest_priority_value:
            self.logger.info(("Recognized change of Highest Priority, so the "
                " SQS queues will be re-initialized."))
            self.highest_priority = new_highest_priority_value
            self.generate_queues_priorities_names()
            if not self.check_queues():
                self.logger.critial(("An error appeared while queue"
                    " re-initialization after HighestPriority change. Let's"
                    " see what the future brings: Some queue(s) won't be"
                    " polled for jobs until the next HP change comes!"))

class Job(object):
    def __init__(self, boto_sqsmsg, ja_inicfg, sdb, sqs, job_machine_index):
        """
        @params: boto_sqsmsg: boto SQS Message object containing job info
                 ja_inicfg: Job Agent initial config object
                 sdb: SimpleDB class instance
                 sqs: SQS class instance
                 job_machine_index: A "counter": how many Job objects have been
                                    initialized so far. This is used to update
                                    VM's 'nbr_jobs_initialized' attribute in SDB
        """
        self.logger = logging.getLogger("jobagent.py.Job.%s"%job_machine_index)
        self.logger.debug("initialize Job object")
        self.ja_inicfg = ja_inicfg
        self.sdb = sdb
        self.sqs = sqs
        self.boto_sqsmsg = boto_sqsmsg
        self.machine_index = job_machine_index

        self.workingdir = None
        self.logdir = None
        self.returncode = None
        self.subprocess = None
        self.input_sandbox_arc_file_path = None
        self.stdouterr_file_path = None
        self.stdouterr_file = None
        self.subprocess_starttime = None
        self.sdb_jobkill_flag_last_polled = 0
        self.done = None

        self.job_id = None

        if not (self.evaluate_sqsmsg() and self.set_up_job_dirs_files()):
            self.logger.critical("Job initialization stopped.")
            # done = True means that this job object gets deleted by JobAgent
            self.done = True
            return
        self.start()

    def set_up_job_dirs_files(self):
        """
        Job and log dir must not exist. Create them then. Build important
        pathts (e.g. input sandbox archive file path)
        """
        self.workingdir = os.path.join(JOB_WORK_BASEDIR,"workdir_"+self.job_id)
        self.logdir = os.path.join(JOB_LOG_BASEDIR,"joblog_"+self.job_id)
        self.logger.info(("Set up working dir %s and log dir %s"
            % (self.workingdir, self.logdir)))
        if os.path.exists(self.workingdir):
            self.logger.critical("%s already exists. Stop Job."%self.workingdir)
            return False
        if os.path.exists(self.logdir):
            self.logger.critical("%s already exists. Stop Job."%self.logdir)
            return False
        try:
            os.makedirs(self.workingdir)
            os.makedirs(self.logdir)
        except:
            self.logger.critical("Error while creating job dirs.")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False
        self.output_sandbox_arc_file_path = os.path.join(
            self.workingdir,
            self.output_sandbox_arc_filename)
        self.input_sandbox_arc_file_path = os.path.join(
            self.workingdir,
            os.path.basename(self.input_sandbox_archive_key))
        return True

    def evaluate_sqsmsg(self):
        """
        Parse SQS job message. Check whether S3 or Cumulus it the storage
        service. Case S3: use same AWS credentials as for SDB and SQS.
        Case Cumulus: use special credentials, try to read them from job msg,
        too.
        """
        self.logger.info("Evaluate SQS job message..")
        jobmsg = SQSJobMessage()
        try:
            msg = self.boto_sqsmsg.get_body()
            self.logger.debug("jog msg: %s" % repr(msg))
            self.logger.debug("parse message..")
            jobmsg.init_read(self.boto_sqsmsg.get_body())
            self.logger.debug("config:\n%s"%jobmsg.config.write_to_string())
            self.job_id = jobmsg.get_job_id()
            self.executable = jobmsg.get_executable()
            ss = jobmsg.get_sandbox_storage_service().lower()
            if not (ss == 's3' or ss == 'cumulus'):
                self.logger.critical("Storage Service is not 's3' or 'cumulus'")
                return False
            elif ss == 'cumulus':
                self.cumulus_hostname = jobmsg.get_cumulus_hostname()
                self.cumulus_port = jobmsg.get_cumulus_port()
                self.cumulus_accesskey = jobmsg.get_cumulus_accesskey()
                self.cumulus_secretkey = jobmsg.get_cumulus_secretkey()
            self.storage_service = ss
            self.output_sandbox_arc_filename = (
                jobmsg.get_output_sandbox_arc_filename())
            self.output_sandbox_archive_key = (
                jobmsg.get_output_sandbox_archive_key())
            self.sandbox_archive_bucket = jobmsg.get_sandbox_bucket()
            self.input_sandbox_archive_key = (
                jobmsg.get_input_sandbox_archive_key())
            self.job_msg_creation_time = jobmsg.get_job_msg_creation_time()
            self.output_sandbox_files = jobmsg.get_output_sandbox_files()
            self.job_owner = jobmsg.get_job_owner()
            self.production_system_job_id = (
                jobmsg.get_production_system_job_id())
        except:
            self.logger.critical("Error while parsing SQS job message.")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False
        return True

    def check_and_manage(self):
        """
        Check job's state. Act accordingly.
        """
        self.logger.debug("check and manage job %s" % self.job_id)
        if self.check_job_kill_flag():
            if self.kill():
                self.logger.debug("subprocess killed.")
        if self.subprocess is not None:
            self.logger.debug("poll subprocess..")
            self.returncode = self.subprocess.poll()
            if self.returncode is not None:
                self.logger.info(("job returned with returncode %s"
                    % self.returncode))
                self.subprocess = None
                self.finish()
                self.done = True

    def finish(self):
        """
        The job subprocess ended. Update SDB, build&upload output sandbox,
        delete SQS msg.
        """
        self.logger.debug("Finish job...")
        self.sdb.update_job_state(self.job_id, 'save_output', self.returncode)
        if self.build_output_sandbox_arc():
            self.logger.info("output sandbox archive built.")
            if self.save_output_sandbox():
                self.logger.info("output sandbox archive uploaded.")
                self.delete_sqs_message()
                if self.returncode != 0:
                    self.sdb.update_job_state(self.job_id, 'completed_error')
                else:
                    self.sdb.update_job_state(self.job_id, 'completed_success')

    def start(self):
        """
        Make everything okay with this job regarding SDB and SQS, receive
        input sandbox and start subprocess.
        """
        self.logger.info("Job SDB initialization (check/create item)...")
        initreturn = self.sdb.initialize_job(
            self.job_id,
            self.boto_sqsmsg.id,
            self.machine_index)
        if initreturn == 'success':
            self.logger.info("Job SDB init successfull.")
            if self.sdb.update_job_state(self.job_id, 'get_input'):
                if self.get_input_sandbox():
                    if self.extract_input_sandbox():
                        if self.run_subprocess():
                            self.sdb.update_job_state(self.job_id, 'running')
                            # this is the real success -> return
                            # any other case will lead in self.done = True and
                            # the deletion of the SQS job message.
                            return
                        else:
                            self.logger.critical("Subprocess run error.")
                            self.sdb.update_job_state(self.job_id, 'run_error')
        elif initreturn == 'reject_job':
            self.logger.info(("Job %s is rejected because of duplicity"
                % self.job_id))
            # another VM is working on this job. I think this means that this
            # VM here received the SQS job message at last. This means that
            # the SQS job message can only be deleted with the receipt handle
            # of THIS message here. (one get's another receipt handle on each
            # receipt and the msg can only be deleted with the most recent one.)
            # Hence, we have to delete the message, because the other VM simply
            # can't.
        elif initreturn == 'job_removed' or initreturn == 'job_killed':
            self.logger.error("marked as killed/removed; now delete SQS msg")
            # here, it is clear that it makes sense to delete the SQS msg :-)
        else:
            self.logger.error("Initialization problem. Job processing aborted.")
        # when we got here, the job's subprocess did not start. Mark job
        # as "done" which results in deletion of the Job object out
        # of JobAgent's joblist. Additionally, delete job's SQS msg (as argued
        # above).
        self.done = True
        self.delete_sqs_message()

    def delete_sqs_message(self):
        """
        Delete SQS message. No consequences if it does not work.
        """
        try:
            self.logger.debug("delete SQS msg with ID %s" % self.boto_sqsmsg.id)
            foo = self.boto_sqsmsg.delete()
            if foo:
                self.logger.debug("deletion successfull")
            else:
                self.logger.debug("boto_sqsmsg.delete() returned %s" % foo)
        except:
            self.logger.critical(("Error while deleting SQS msg %s"
                % self.boto_sqsmsg.id))
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())

    def run_subprocess(self):
        """
        Run job executable as subprocess. Store stdout/err in job log directory.
        """
        try:
            self.logger.info("run Job %s as subprocess" % self.job_id)
            timestr = utc_timestring()
            stdouterr_file_name = ("%s_stdouterr_%s.log"%(self.job_id,timestr))
            self.stdouterr_file_path = os.path.join(
                self.logdir,
                stdouterr_file_name)
            self.logger.debug(("open subprocess logfile for writing: %s"
                % self.stdouterr_file_path))
            self.stdouterr_file = open(self.stdouterr_file_path,'w')

            exe = os.path.abspath(os.path.join(self.workingdir,self.executable))
            self.logger.debug(("run %s as subprocess in directory %s"
                % (exe,self.workingdir)))
            self.subprocess_starttime = time.time()
            self.subprocess = subprocess.Popen(
                args=[exe],
                stdout=self.stdouterr_file,
                stderr=subprocess.STDOUT,
                cwd=self.workingdir,
                shell=True)
        except:
            self.logger.critical("Error in run_subprocess()")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False
        return True

    def get_input_sandbox(self):
        """
        Receive input sandbox data from storage service. Currently, only
        S3 is supported; Cumulus follows. Store it to job's working dir.
        Return True in case of success
        """
        self.logger.info("retrieve input sandbox from %s"%self.storage_service)
        if self.storage_service == 's3':
            try:
                conn = boto.connect_s3(
                    self.ja_inicfg.aws_accesskey,
                    self.ja_inicfg.aws_secretkey)
                bucket = conn.lookup(
                    bucket_name=self.sandbox_archive_bucket.lower())
                k = boto.s3.key.Key(bucket)
                k.key = self.input_sandbox_archive_key
                self.logger.info(("Retrieve key %s from bucket %s to file %s"
                    % (k.key, bucket.name, self.input_sandbox_arc_file_path)))
                k.get_contents_to_filename(self.input_sandbox_arc_file_path)
                return True
            except:
                self.logger.critical("Error while retrieving input sandbox")
                self.logger.critical("Traceback:\n%s"%traceback.format_exc())
        else:
            self.logger.error("unkown storage service")
        return False

    def extract_input_sandbox(self):
        """
        Extract via tar subprocess. Assume gzipped tarfile.
        """
        # extract input sandbox archive to job working directory
        cmd = ("tar xjf %s --verbose -C %s"
            % (self.input_sandbox_arc_file_path,self.workingdir))
        self.logger.info(("run input sandbox archive extraction as subprocess:"
            " %s" % cmd))
        try:
            sp = subprocess.Popen(
                args=[cmd],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True)
            # wait for process to terminate, get stdout and stderr
            stdout, stderr = sp.communicate()
            self.logger.debug("subprocess STDOUT:\n%s" % stdout)
            if stderr:
                self.logger.error("cmd %s STDERR:\n%s" % (cmd,stderr))
                return False
        except:
            self.logger.critical("Error while extracting input sandbox")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False
        return True

    def build_output_sandbox_arc(self):
        """
        Start subprocess  tar cjf arc.tar.gz --ignore-failed-read x x x' to
        compress all desired output files into an archive. Additionally,
        put
        - job's logfile
        - Job Agent's stdout logfile and stderr logfile (current state)
        - boto's logfile
        into the archive, too.
        """
        def tar_dirswitch_append_file_with_1lvlup_dir(file_abspath):
            # grab the directory and the filename
            filedir = os.path.dirname(file_abspath)
            filedir_1lvlup,filedir_name = os.path.split(filedir)
            filename = os.path.basename(file_abspath)
            # tar's directory switch to /path/dir1/dir2 and then add dirN/file:
            return " -C %s %s" % (
                filedir_1lvlup,
                os.path.join(filedir_name,filename))

        # at first process the filenames that were given in the job message
        filename_list = self.output_sandbox_files.split(";")
        tar_files_list = ' '.join(filename_list)

        # now add the job's logfile to the list.
        # at first: close it!
        self.stdouterr_file.close()
        tar_files_list += tar_dirswitch_append_file_with_1lvlup_dir(
            self.stdouterr_file_path)

        # now add the Job Agents's logfile (STDOUT) to the list.
        tar_files_list += tar_dirswitch_append_file_with_1lvlup_dir(
            self.ja_inicfg.logfile_path)

        # now add the Job Agents's logfile (STDERR) to the list.
        tar_files_list += tar_dirswitch_append_file_with_1lvlup_dir(
            self.ja_inicfg.stderr_logfile_path)

        # now add boto's logfile to the list.
        tar_files_list += tar_dirswitch_append_file_with_1lvlup_dir(
            self.ja_inicfg.boto_logfile_path)

        cmd = ("tar cjf %s --verbose --ignore-failed-read  %s"
            % (self.output_sandbox_arc_file_path, tar_files_list))
        self.logger.info(("run output sandbox compression as subprocess:"
            " %s in workingdir %s" % (cmd, self.workingdir)))
        try:
            sp = subprocess.Popen(
                args=[cmd],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True,
                cwd=self.workingdir)
            # wait for process to terminate, get stdout and stderr
            stdout, stderr = sp.communicate()
            self.logger.debug("subprocess STDOUT:\n%s" % stdout)
            if stderr:
                self.logger.error("cmd %s STDERR:\n%s" % (cmd,stderr))
                # existing stderr does not necessarily mean that the archive
                # wasn't created.
                if not os.path.exists(self.output_sandbox_arc_file_path):
                    self.logger.error("output sandbox archive was not created.")
                    return False
        except:
            self.logger.critical("Error while compressing output sandbox")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False
        return True

    def save_output_sandbox(self):
        """
        Send output sandbox data to storage service. Currently, only
        S3 is supported; Cumulus follows.
        Return True in case of success
        """
        self.logger.info("send output sandbox to %s" % self.storage_service)
        if self.storage_service == 's3':
            try:
                conn = boto.connect_s3(
                    self.ja_inicfg.aws_accesskey,
                    self.ja_inicfg.aws_secretkey)
                bucket = conn.lookup(
                    bucket_name=self.sandbox_archive_bucket.lower())
                k = boto.s3.key.Key(bucket)
                k.key = self.output_sandbox_archive_key
                self.logger.info(("store file %s as key %s to bucket %s"
                    % (self.output_sandbox_arc_file_path, k.key, bucket.name)))
                k.set_contents_from_filename(self.output_sandbox_arc_file_path)
                return True
            except:
                self.logger.critical("Error while uploading output sandbox")
                self.logger.critical("Traceback:\n%s"%traceback.format_exc())
        else:
            self.logger.error("unkown storage service")
        return False

    def kill(self):
        """
        Kill job's subprocess via SIGKILL.
        """
        if self.subprocess is not None:
            self.logger.info("KILL job's subprocess")
            try:
                # new in Python 2.6
                self.subprocess.kill()
                self.sdb.update_job_state(self.job_id, 'killed')
                return True
            except:
                self.logger.critical("Error while killing subprocess")
                self.logger.critical("Traceback:\n%s"%traceback.format_exc())
        else:
            self.logger.debug("job's subprocess is None -- nothing to kill")

    def check_job_kill_flag(self):
        """
        Check if it's time to poll the kill flag from SDB. yes: poll and return
        no: return old state.
        """
        if alarm(
        self.sdb_jobkill_flag_last_polled,
        self.ja_inicfg.sdb_poll_jobkill_flag_interval):
            self.logger.debug("Triggered to check kill flag")
            self.kill_flag = self.sdb.poll_job_kill_flag(self.job_id)
            self.sdb_jobkill_flag_last_polled = time.time()
        return self.kill_flag


class JobAgent(object):
    def __init__(self,
            start_options,
            boto_logfile_path,
            ja_logfile_path,
            ja_stderr_logfile_path,):
        """
        Evaluate given userdata. Read instance ID. Get number of cores.
        """
        self.logger = logging.getLogger("jobagent.py.JobAgent")
        self.logger.debug("initialize JobAgent object")

        userdata_file_path = check_file(start_options.userdata_file_path)
        #instanceid_file_path = check_file(start_options.instanceid_file_path)

        startconfig = self.get_startconfig_from_userdata(userdata_file_path)
        self.logger.debug(("startconfig from userdata: \n%s"
            % self.config_to_string(startconfig)))
        # The startconfig was created by the ResourceManager:
        # config.add_section('userdata')
        # config.set('userdata','sessionid',self.session_id)
        # config.set('userdata','vmid',vm_id)
        # config.set('userdata','accesskey',self.inicfg.aws.accesskey)
        # config.set('userdata','secretkey',self.inicfg.aws.secretkey)
        # config.set('userdata','ja_sqs_poll_job_interval',
            # str(self.inicfg.ja.ja_sqs_poll_job_interval))
        # config.set('userdata','ja_sdb_poll_softkill_flag_interval',
            # str(self.inicfg.ja.ja_sdb_poll_softkill_flag_interval))
        # config.set('userdata','ja_sdb_poll_highestprio_interval',
            # str(self.inicfg.ja.ja_sdb_poll_highestprio_interval))
        # config.set('userdata','ja_sdb_poll_jobkill_flag_interval',
            # str(self.inicfg.ja.ja_sdb_poll_jobkill_flag_interval))
        # config.set('userdata','ja_log_storage_service',
            # str(self.inicfg.ja.ja_log_storage_service))
        # config.set('userdata','ja_log_bucket',self.inicfg.ja.ja_log_bucket)
        self.logger.debug("process ConfigParser config delivered by userdata")
        self.inicfg = Object()
        self.inicfg.sessionid = startconfig.get('userdata','sessionid')
        self.inicfg.vm_id = startconfig.get('userdata','vmid')
        self.inicfg.aws_accesskey = startconfig.get('userdata','accesskey')
        self.inicfg.aws_secretkey = startconfig.get('userdata','secretkey')
        self.inicfg.boto_logfile_path = boto_logfile_path
        self.inicfg.logfile_path = ja_logfile_path
        self.inicfg.stderr_logfile_path = ja_stderr_logfile_path

        #self.logger.debug("read instance ID from %s" % instanceid_file_path)
        #self.inicfg.instance_id = open(instanceid_file_path).read()
        #self.logger.info("instance ID: %s" % self.inicfg.instance_id)

        # poll intervals in seconds (the higher, the cheaper the process..)
        self.inicfg.sqs_poll_job_interval = startconfig.getfloat(
            'userdata',
            'ja_sqs_poll_job_interval')
        self.inicfg.sdb_poll_softkill_flag_interval = startconfig.getfloat(
            'userdata',
            'ja_sdb_poll_softkill_flag_interval')
        self.inicfg.sdb_poll_highestprio_interval = startconfig.getfloat(
            'userdata',
            'ja_sdb_poll_highestprio_interval')
        self.inicfg.sdb_poll_jobkill_flag_interval = startconfig.getfloat(
            'userdata',
            'ja_sdb_poll_jobkill_flag_interval')
        self.inicfg.log_storage_service = startconfig.get(
            'userdata',
            'ja_log_storage_service')
        self.inicfg.log_bucket = startconfig.get(
            'userdata',
            'ja_log_bucket')



        self.logger.debug("get number of cores for this VM..")
        nbr_cores = self.get_number_of_cores()
        if nbr_cores:
            self.inicfg.nbr_cores = nbr_cores
            self.logger.info("number of cores: %s" % nbr_cores)
        else:
            self.logger.error(("number of cores could not be determined. Set "
                "it to 1."))
            self.inicfg.nbr_cores = 1

        # to be populated / changed during runtime
        self.sdb = None
        self.sqs = None
        self.highest_priority = None
        self.jobs = None
        self.sqs_jobs_last_polled = 0
        self.sdb_softkill_flag_last_polled = 0
        self.sdb_highestprio_last_polled = 0
        self.job_machine_index_counter = 1
        self.softkill_flag = False
        self.jobs = []
        # import pprint
        # pprint.pprint(vars(self))
        # pprint.pprint(vars(self.inicfg))

    def main(self):
        """
        Initialize SDB and SQS and then pass into mainloop. Shut down in error
        case.
        """
        if self.init_sdb():
            self.logger.info("SDB init successfull.")
            if self.init_sqs():
                self.logger.info("SQS init successfull.")
                if self.sdb.register_ja_started():
                    self.logger.info("JA successfully registered with SDB.")
                    self.main_loop()
        self.logger.critical("JA initialization error. Invoke JA shutdown.")
        self.shutdown()

    def check_highest_priority(self):
        """
        Receive HP from SDB. If changed, set self.highest_priority to new
        value AND invoke self.sqs.update_highest_priority(new_hp_value)
        """
        if alarm(
        self.sdb_highestprio_last_polled,
        self.inicfg.sdb_poll_highestprio_interval):
            self.logger.debug(("SDB HighestPriority update triggered"))
            new_highest_priority = self.sdb.get_highest_priority()
            self.sdb_highestprio_last_polled = time.time()
            if new_highest_priority is None:
                self.logger.critical(("The value for HighestPriority could not"
                    " be updated SDB. Hopefully it works in the next turn."))
                return
            if new_highest_priority != self.highest_priority:
                self.highest_priority = new_highest_priority
                self.sqs.update_highest_priority(new_highest_priority)

    def start_new_job_if_available(self):
        """
        Receive new job from SQS. But, only if it's time to. This is defined
        by self.inicfg.sqs_poll_job_interval. If we've got a new job, then
        initialize and process it.
        """
        if alarm(self.sqs_jobs_last_polled, self.inicfg.sqs_poll_job_interval):
            self.logger.debug(("SQS poll job triggered"))
            self.check_highest_priority()
            boto_sqsmsg = self.sqs.get_job()
            self.sqs_jobs_last_polled = time.time()
            if boto_sqsmsg:
                self.logger.debug("Received a new SQS job message. Init job..")
                self.jobs.append(Job(
                    boto_sqsmsg,
                    self.inicfg,
                    self.sdb,
                    self.sqs,
                    self.job_machine_index_counter))
                self.job_machine_index_counter += 1
        else:
            self.logger.debug("SQS poll job delayed..")

    def check_and_manage_running_jobs(self):
        """
        Check out what's up with the running jobs. When they change status
        (subprocess return etc), act accordingly.
        Delete job from the joblist, if it's totally finished (DONE!)
        """
        delete_indices = []
        for idx, job in enumerate(self.jobs):
            # this actually checks the job and acts accordingly to what's
            # happened
            job.check_and_manage()
            if job.done:
                self.logger.debug(("Job.done is true."
                    " Mark object for deletion."))
                delete_indices.append(idx)

        # delete job objects that contain finished jobs
        for idx in reversed(delete_indices):
            self.logger.debug(("delete job object with job id %s from job list"
                % job.job_id))
            del self.jobs[idx]

    def check_vm_softkill_flag(self):
        """
        Query SimpleDB for the soft kill flag. But don't do it every time this
        method is called. Instead, consider
        self.inicfg.sdb_poll_softkill_flag_interval
        """
        if alarm(
        self.sdb_softkill_flag_last_polled,
        self.inicfg.sdb_poll_softkill_flag_interval):
            self.logger.debug("Triggered to poll VM softkill flag from SDB")
            self.softkill_flag = self.sdb.poll_vm_softkill_flag()
            self.logger.debug(("SDB returned softkill flag '%s' for VM '%s'"
                % (self.softkill_flag, self.inicfg.vm_id)))
            self.sdb_softkill_flag_last_polled = time.time()
        return self.softkill_flag

    def main_loop(self):
        """
        This is the Job Agent's main loop which will run over and over again.
        The only way to get out is the shutdown() method. Polling of stuff like
        subprocesses, SDB and SQS must not happen with very high frequency.
        Hence, it's okay to send this to sleep for some seconds every turn.

        There are only two ways to kill a VM from the outside:
        (1) via softkill (don't interrupt running jobs, but shutdown afterwards)
        (2) via hardkill (reckless destruction of the VM from outside)
        (1) is checked within this loop, (2) happens autonomous ;-)
        """
        self.logger.info("Entering Job Agent's main loop.")
        while True:
            # get & start a new job, but only if *BOTH* is fulfilled:
            # 1) currently, there are less jobs processed than nbr_cores
            # 2) the VM softkill flag isn't set
            if len(self.jobs) < self.inicfg.nbr_cores:
                self.logger.debug(("Nbr of jobs (%s) smaller than nbr of"
                    " cores (%s)" %(len(self.jobs),self.inicfg.nbr_cores)))
                # should the VM be killed after finishing current job(s)?
                if self.check_vm_softkill_flag():
                    if len(self.jobs) == 0:
                        # all current jobs done, shut JobAgent and VM down!
                        self.logger.info(("VM softkill flag is set *and* there"
                            " is no running job. Invoke cleanup/shutdown!"))
                        self.shutdown()
                else:
                    self.start_new_job_if_available()

            # check out what's up with the running jobs. Are subprocesses
            # finished? What's the returncode? -> Update job status in SDB,
            # upload output sandboxes, etc...
            self.check_and_manage_running_jobs()

            # sleep for a while, before starting the next turn..
            time.sleep(6)

    def compress_upload_log(self):
        """
        Create compressed tar archive of JA stdout / stderr and boto log.
        Upload to storage service.
        """
        tarfile_path = os.path.join(
            os.path.dirname(self.inicfg.logfile_path),
            "jobagentlog_%s.tar.bz2" % self.inicfg.vm_id)
        # this very small task is okay for Python's tarfile module
        self.logger.info(("MY LOG WILL NOW BE BUNDLED TO %s. THIS IS"
            " LIKELY THE LAST LOG MESSAGE YOU WILL EVER SEE FROM ME"
            % tarfile_path))
        tar = tarfile.open(tarfile_path, "w:bz2")
        tar.add(
            self.inicfg.boto_logfile_path,
            os.path.basename(self.inicfg.boto_logfile_path))
        tar.add(
            self.inicfg.logfile_path,
            os.path.basename(self.inicfg.logfile_path))
        tar.add(
            self.inicfg.stderr_logfile_path,
            os.path.basename(self.inicfg.stderr_logfile_path))
        tar.close()
        self.logger.debug("bundled.")

        # now upload..
        self.logger.info("send Job Agent log to %s"
            % self.inicfg.log_storage_service)
        if self.inicfg.log_storage_service.lower() == 's3':
            try:
                conn = boto.connect_s3(
                    self.inicfg.aws_accesskey,
                    self.inicfg.aws_secretkey)
                bucket = conn.lookup(bucket_name=self.inicfg.log_bucket.lower())
                k = boto.s3.key.Key(bucket)
                k.key = os.path.join(
                    self.inicfg.sessionid,
                    "jobagentslog",
                    os.path.basename(tarfile_path))
                self.logger.info(("store file %s as key %s to bucket %s"
                    % (tarfile_path, k.key, bucket.name)))
                k.set_contents_from_filename(tarfile_path)
                return True
            except:
                self.logger.critical("Error while uploading log archive")
                self.logger.critical("Traceback:\n%s"%traceback.format_exc())
        else:
            self.logger.error("unkown storage service")
        return False

    def shutdown(self):
        """
        This is the last method executed by JobAgent, because it invokes a
        shutdown of this VM. But before:
        - upload logs
        - set JobAgent/ VM status in SDB to 'JA_kill_triggered'
        """
        self.logger.info(("cleaning up: compress and upload Job Agent log,"
            " notify SimpleDB and shut down the system"))
        self.compress_upload_log()
        self.sdb.set_shutdown_triggered()
        sys.exit(1)
        #os.system("shutdown -h now")

    def init_sdb(self):
        """
        Initialize SimpleDB domains for this Resource Session. Receive
        HighestPriority setting from SDB.
        """
        try:
            self.sdb = SimpleDB(self.inicfg)
            # check and init domains
            self.sdb.check_domains()
            self.highest_priority = self.sdb.get_highest_priority()
        except:
            self.logger.critical("Error while initializing SDB")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False

        if self.highest_priority is None:
            self.logger.critical(("The initial value for HighestPriority could"
                " not be retrieved from SDB. But it's needed to go on."))
            return False
        return True

    def init_sqs(self):
        """
        Initializes SQS queues. SDB has to be initialized before
        (self.highest_priority must be set):
        """
        if not self.highest_priority :
            self.logger.error(("highest priority must be set before "
                "initializing SQS!"))
            return False
        try:
            self.sqs = SQS(self.inicfg, self.highest_priority)
            if not self.sqs.check_queues():
                self.logger.critical(("SQS queue initialization error! Exit!"))
                return False
            return True
        except:
            self.logger.critical("Error while initializing SQS")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False

    def get_startconfig_from_userdata(self, userdata_file_path):
        """
        This function inverts the actions done in
        ResourceManager.Session.generate_userdata()
        This means:
            - decode strong with b64
            - make ConfigParser config out of zipped string using
              SafeConfigParserStringZip
        """
        self.logger.debug("read userdata file %s ..." % userdata_file_path)
        b64zipcfg = open(userdata_file_path).read()
        zipcfg = base64.b64decode(b64zipcfg)
        config = SafeConfigParserStringZip()
        self.logger.debug("re-create ConfigParser config from zipped string..")
        config.read_from_zipped_string(zipcfg)
        return config

    def config_to_string(self, configparserconfig):
        """
        A convenience method to get a string out of a ConfigParser config
        """
        configstringlist = []
        for section in configparserconfig.sections():
            for option in configparserconfig.options(section):
                val = configparserconfig.get(section,option)
                if option == 'secretkey': val = 'X'
                configstringlist.append("%s:%s=%s" % (section,option,val))
        return '\n'.join(configstringlist)

    def get_number_of_cores(self):
        """
        Start subprocess 'grep -c processor /proc/cpuinfo' to count number of
        available CPU cores on this machine.
        """
        sp = subprocess.Popen(
            args=['grep -c processor /proc/cpuinfo'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)
        # wait for process to terminate, get stdout and stderr
        stdout, stderr = sp.communicate()
        if stderr:
            self.logger.error(("'grep -c processor /proc/cpuinfo' error: %s"
                % stderr))
            return False
        try:
            nbr_cpus = int(stdout)
        except ValueError:
            self.logger.error(("stdout of 'grep -c processor /proc/cpuinfo' was"
                " not a number: %s" % stdout))
            return False
        return nbr_cpus


class JobAgentLogger(object):
    """
    Configuration class for logging with the logging module.
    """
    def __init__(self, logdir):
        # create logdir
        if not os.path.exists(logdir):
            os.makedirs(logdir)

        # create log filenames (with prefix from time)
        log_filename_prefix = time.strftime("UTC%Y%m%d-%H%M%S", time.gmtime())
        ja_log_file_path = os.path.join(
            logdir,
            log_filename_prefix+"_JobAgent.log")
        boto_log_file_path = os.path.join(
            logdir,
            log_filename_prefix+"_boto.log")
        self.ja_log_file_path = ja_log_file_path
        self.boto_log_file_path = boto_log_file_path

        # set up main/root logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        # file handler for a real file with level DEBUG
        self.fh = logging.FileHandler(ja_log_file_path, encoding="UTF-8")
        self.fh.setLevel(logging.DEBUG)
        self.formatter_file = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s: %(message)s")
        self.fh.setFormatter(self.formatter_file)

        # "console" handler with level ERROR
        # to stderr by default, but we want it to stdout!
        self.ch = logging.StreamHandler(sys.stdout)
        self.ch.setLevel(logging.DEBUG)
        self.formatter_console = logging.Formatter(
           "%(asctime)s %(levelname)-8s %(name)s: %(message)s")
        self.ch.setFormatter(self.formatter_console)

        # add handler
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ch)

        # set logging for boto -> to file, not to console, no propagation to
        # higher levels in hierarchy
        self.logger_boto = logging.getLogger("boto")
        self.logger_boto.propagate = False
        self.logger_boto.setLevel(logging.DEBUG)
        self.fh_boto = logging.FileHandler(boto_log_file_path, encoding="UTF-8")
        self.fh_boto.setFormatter(self.formatter_file)
        self.logger_boto.addHandler(self.fh_boto)

    def debug(self, msg):
        self.logger.debug(msg)
    def info(self, msg):
        self.logger.info(msg)
    def warn(self, msg):
        self.logger.warn(msg)
    def error(self, msg):
        self.logger.error(msg)
    def critical(self, msg):
        self.logger.critical(msg)
    def get_boto_log_file_path(self):
        return self.boto_log_file_path
    def get_ja_log_file_path(self):
        return self.ja_log_file_path


class SQSJobMessage(object):
    def __init__(self):
        self.config = SafeConfigParserStringZip()
        self.section = 'job_message'

    def init_write(self):
        self.config.add_section(self.section)

    def init_read(self, config_zipped_string):
        self.config.read_from_zipped_string(config_zipped_string)

    def zip_string(self):
        return self.config.write_to_zipped_string()

    def b64zip_string(self):
        zipcfg = self.config.write_to_zipped_string()
        b64zipcfg = base64.b64encode(zipcfg)
        return b64zipcfg

    def string(self):
        return self.config.write_to_string()

    def set_job_id(self, job_id):
        self.config.set(self.section,'job_id',job_id)
    def get_job_id(self):
        return self.config.get(self.section,'job_id')
    def set_executable(self, exe):
        self.config.set(self.section,'executable',exe)
    def get_executable(self):
        return self.config.get(self.section,'executable')
    def set_sandbox_storage_service(self, service):
        self.config.set(self.section,'sandbox_storage_service', service)
    def get_sandbox_storage_service(self):
        return self.config.get(self.section,'sandbox_storage_service')
    def set_cumulus_hostname(self, cumulus_hostname):
        self.config.set(self.section,'cumulus_hostname',cumulus_hostname)
    def get_cumulus_hostname(self):
        return self.config.get(self.section,'cumulus_hostname')
    def set_cumulus_port(self, cumulus_port):
        self.config.set(self.section,'cumulus_port',cumulus_port)
    def get_cumulus_port(self):
        return self.config.get(self.section,'cumulus_port')
    def set_cumulus_accesskey(self, cumulus_accesskey):
        self.config.set(self.section,'cumulus_accesskey',cumulus_accesskey)
    def get_cumulus_accesskey(self):
        return self.config.get(self.section,'cumulus_accesskey')
    def set_cumulus_secretkey(self, cumulus_secretkey):
        self.config.set(self.section,'cumulus_secretkey',cumulus_secretkey)
    def get_cumulus_secretkey(self,):
        return self.config.get(self.section,'cumulus_secretkey')
    def set_sandbox_bucket(self, bucket):
        self.config.set(self.section,'sandbox_bucket',bucket)
    def get_sandbox_bucket(self):
        return self.config.get(self.section,'sandbox_bucket')
    def set_output_sandbox_arc_filename(self, filename):
        self.config.set(self.section,'output_sandbox_arc_filename',filename)
    def get_output_sandbox_arc_filename(self):
        return self.config.get(self.section,'output_sandbox_arc_filename')
    def set_output_sandbox_archive_key(self, key):
        self.config.set(self.section,'output_sandbox_archive_key',key)
    def get_output_sandbox_archive_key(self):
        return self.config.get(self.section,'output_sandbox_archive_key')
    def set_input_sandbox_archive_key(self, key):
        self.config.set(self.section,'input_sandbox_archive_key',key)
    def get_input_sandbox_archive_key(self):
        return self.config.get(self.section,'input_sandbox_archive_key')
    def set_job_msg_creation_time(self, timestr):
        self.config.set(self.section,'job_msg_creation_time',timestr)
    def get_job_msg_creation_time(self):
        return self.config.get(self.section,'job_msg_creation_time')
    def set_output_sandbox_files(self, comma_sep_files):
        self.config.set(self.section,'output_sandbox_files',comma_sep_files)
    def get_output_sandbox_files(self):
        return self.config.get(self.section,'output_sandbox_files')
    def set_job_owner(self, owner):
        self.config.set(self.section,'job_owner',owner)
    def get_job_owner(self):
        return self.config.get(self.section,'job_owner')
    def set_production_system_job_id(self, id):
        self.config.set(self.section,'production_system_job_id', id)
    def get_production_system_job_id(self):
        return self.config.get(self.section,'production_system_job_id')

def parseargs():
    """
    Parse commandlineoptions using the optparse module and check them for
    logical consistence. Generate help- and usage-output.

    @return: optparse `options`-object containing the commandline options
    """
    version = '0'
    description = ("Clobi Job Agent")
    usage = ("try -h, --help and --version")
    parser = optparse.OptionParser(
        usage=usage,
        version=version,
        description=description)

    parser.add_option('-u', '--userdatafile', dest='userdata_file_path',
        help='file containing userdata string')
    #parser.add_option('-i', '--instanceidfile', dest='instanceid_file_path',
    #    help='file containing instanceid string')

    # now read in the given arguments (from sys.argv by default)
    (options, args) = parser.parse_args()

    # now check the logical consistence...
    # if not options.userdata_file_path or not options.instanceid_file_path:
        # parser.error('both, --userdatafile and --instanceidfile must be set!')
    if not options.userdata_file_path :
        parser.error('--userdatafile must be set!')
    return options

if __name__ == "__main__":
    # create module logger
    logger = logging.getLogger("jobagent.py")
    main()
