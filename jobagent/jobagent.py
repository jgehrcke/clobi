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

from components.cfg_parse_strzip import SafeConfigParserStringZip
from components.utils import (check_dir, check_file, timestring, backup_file,
    Tee, Object, alarm)

sys.path.append("components")
import boto


def main():
    logdir = "jobagent_logs"
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    # log stderr to stderr and to a real file
    stderr_logfile_path = os.path.join(logdir, timestring() + "_JA_stderr.log")
    stderr_log_fd = open(stderr_logfile_path,'w')
    sys.stderr = Tee(sys.stderr, stderr_log_fd)

    # configure logger
    rootlog = JobAgentLogger(logdir)

    try:
        logger.debug("parse commandline arguments..")
        start_options = parseargs()
        jobagent = JobAgent(start_options)
        jobagent.init_sdb()
        jobagent.init_sqs()
        # I (the JobAgent) now tell to SimpleDB that I've started up :-)
        jobagent.sdb.register_ja_started()
        jobagent.main_loop()
    except:
        logger.critical("Job Agent ended exceptionally. Try to save some logs.")
        logger.critical("Traceback:\n%s"%traceback.format_exc())
    finally:
        # some other cleanup
        stderr_log_fd.close()
        # shut down VM?


class SimpleDB(object):
    """
    Set up SimpleDB for this job agent. Provide an interface to SDB (all
    job agent communication with SDB is done via this class)
    """
    def __init__(self, initial_jobagent_config):
        self.logger = logging.getLogger("jobagent.py.SimpleDB")
        self.logger.debug("initialize SimpleDB object")

        self.inicfg = initial_jobagent_config

        # init boto objects
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
        Call self.create_domain() in resume mode for all domains this session
        needs.
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
        Get HighestPriority flag from SDB
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
                    " Means that something went very wrong. It must be set by RM"
                    " before any VM is started up. I'll exit now"))
                sys.exit(1)
        else:
            self.logger.critical(("No 'session_props' item set in SDB! This"
                " means that something went very wrong. It must be set by RM"
                " before any VM is started up. I'll exit now"))
            sys.exit(1)

    def register_ja_started(self):
        item = self.boto_domainobj_session.get_item(self.inicfg.vm_id)
        timestr = timestring()
        if item is not None:
            self.logger.info(("updating SDB item %s with 'status=JA_running' "
                "and 'JA_startuptime=%s'" % (self.inicfg.vm_id, timestr)))
            item['status'] = 'JA_running'
            item['JA_startuptime'] = timestr
            item.save()
        else:
            self.logger.critical(("No %s item set in SDB! This"
                " means that something went very wrong. It must be set by RM"
                " before any VM is started up. I'll exit now"
                % self.inicfg.vm_id))
            sys.exit(1)

    def poll_vm_softkill_flag(self):
        """
        Query SDB for VM status. 'RM_kill_after_job_ordered' is evaluated as
        the "VM softkill flag". Return True if set; False otherwise
        """
        try:
            self.logger.debug(("Check if VM status is set to "
                "'RM_kill_after_job_ordered' in SDB..."))
            item = self.boto_domainobj_session.get_attributes(
                item_name=self.inicfg.vm_id,
                attribute_name='status')
            self.logger.debug(("SDB VM status: %s" % item['status']))
            if item['status'] == 'RM_kill_after_job_ordered':
                return True
        except:
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
        return False


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
            attr = queue.get_attributes(attributes="ApproximateNumberOfMessages")
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


class JobAgent(object):
    def __init__(self, start_options):
        """
        Evaluate given userdata. Read instance ID. Get number of cores.
        """
        self.logger = logging.getLogger("jobagent.py.JobAgent")
        self.logger.debug("initialize JobAgent object")

        userdata_file_path = check_file(start_options.userdata_file_path)
        #instanceid_file_path = check_file(start_options.instanceid_file_path)

        startconfig = self.get_startconfig_from_userdata(userdata_file_path)
        logger.debug("startconfig: \n%s" % self.config_to_string(startconfig))
        # The startconfig was created by the ResourceManager:
        # config.add_section('userdata')
        # config.set('userdata','sessionid',self.session_id)
        # config.set('userdata','vmid',vm_id)
        # config.set('userdata','accesskey',self.inicfg.aws.accesskey)
        # config.set('userdata','secretkey',self.inicfg.aws.secretkey)
        self.logger.debug("process ConfigParser config delivered by userdata")
        self.inicfg = Object()
        self.inicfg.sessionid = startconfig.get('userdata','sessionid')
        self.inicfg.vm_id = startconfig.get('userdata','vmid')
        self.inicfg.aws_accesskey = startconfig.get('userdata','accesskey')
        self.inicfg.aws_secretkey = startconfig.get('userdata','secretkey')

        #self.logger.debug("read instance ID from %s" % instanceid_file_path)
        #self.inicfg.instance_id = open(instanceid_file_path).read()
        #self.logger.info("instance ID: %s" % self.inicfg.instance_id)

        self.logger.debug("get number of cores for this VM..")
        nbr_cores = self.get_number_of_cores()
        if nbr_cores:
            self.nbr_cores = nbr_cores
            self.logger.info("number of cores: %s" % nbr_cores)
        else:
            self.logger.error(("number of cores could not be determined. Set "
                "it to 1."))
            self.nbr_cores = 1

        # in seconds, the higher the value, the cheaper the process..
        self.sqs_poll_job_interval = 30
        self.sdb_poll_softkill_flag_interval = 30
        self.sdb_poll_highestprio_interval = 30


        # to be populated / changed during runtime
        self.sdb = None
        self.sqs = None
        self.highest_priority = None
        self.jobs = None
        self.sqs_jobs_last_polled = 0
        self.sdb_softkill_flag_last_polled = 0
        self.sdb_highestprio_last_polled = 0
        self.softkill_flag = False
        self.jobs = []

    def check_highest_priority(self):
        """
        Receive HP from SDB. If changed, set self.highest_priority to new
        value AND invoke self.sqs.update_highest_priority(new_hp_value)
        """
        if alarm(
        self.sdb_highestprio_last_polled,
        self.sdb_poll_highestprio_interval):
            self.logger.debug(("SDB HighestPriority update triggered"))
            new_highest_priority = self.sdb.get_highest_priority()
            self.sdb_highestprio_last_polled = time.time()
            if new_highest_priority is None:
                self.logger.critical(("The value for HighestPriority could"
                    " not be updated SDB. Hopefully it works in the next turn."))
                return
            if new_highest_priority != self.highest_priority:
                self.highest_priority = new_highest_priority
                self.sqs.update_highest_priority(new_highest_priority)

    def start_new_job_if_available(self):
        """
        Receive new job from SQS. But, only if it's time to. This is defined
        by self.sqs_poll_job_interval. If we've got a new job, then
        initialize and process it.
        """
        self.logger.debug("start new jobs if available..")
        if alarm(self.sqs_jobs_last_polled, self.sqs_poll_job_interval):
            self.logger.debug(("SQS poll job triggered"))
            self.check_highest_priority()
            boto_sqsmsg = self.sqs.get_job()
            self.sqs_jobs_last_polled = time.time()
            # if sqsmsg:
                # self.logger.debug("Received a new SQS job message. Init job..")
                # self.jobs.append(Job(
                    # boto_sqsmsg,
                    # self.sdb,
                    # self.sqs))

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
                self.logger.debug(("Job %s is done. Mark it for deletion."
                    % job.job_id))
                # this job is done, mark it for deletion
                delete_indices.append(idx)

        # delete job objects that contain finished jobs
        for idx in reversed(delete_indices):
            self.logger.debug(("delete job object with job id %s from job list"
                % job.job_id))
            del self.jobs[idx]

    def check_vm_softkill_flag(self):
        """
        Query SimpleDB for the soft kill flag. But don't do it every time this
        method is called. Instead, consider self.sdb_poll_softkill_flag_interval
        """
        if alarm(
        self.sdb_softkill_flag_last_polled,
        self.sdb_poll_softkill_flag_interval):
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
            if len(self.jobs) < self.nbr_cores:
                # should the VM be killed after finishing current job(s)?
                if self.check_vm_softkill_flag():
                    if len(self.jobs) == 0:
                        # all current jobs done, shut JobAgent and VM down!
                        self.shutdown()
                else:
                    self.start_new_job_if_available()

            # check out what's up with the running jobs. Are subprocesses
            # finished? What's the returncode? -> Update job status in SDB,
            # upload output sandboxes, etc...
            self.check_and_manage_running_jobs()

            # sleep for a while, before starting the next turn..
            time.sleep(3)

    def shutdown(self):
        """
        This is the last method executed by JobAgent, because it invokes a
        shutdown of this VM. But before:
        - upload logs
        - set JobAgent/ VM status in SDB to 'JA_kill_triggered'
        """
        self.upload_log()
        self.sdb.set_jobagent_shutdown()
        #os.system("shutdown -h now")

    def init_sdb(self):
        """
        Initialize SimpleDB domains for this Resource Session. Receive
        HighestPriority setting from SDB.
        """
        self.sdb = SimpleDB(self.inicfg)
        # check and init domains
        self.sdb.check_domains()
        self.highest_priority = self.sdb.get_highest_priority()
        if self.highest_priority is None:
            self.logger.critical(("The initial value for HighestPriority could"
                " not be retrieved from SDB. But it's needed to go on. Exit."))
            sys.exit(1)

    def init_sqs(self):
        """
        Initializes SQS queues. SDB has to be initialized before
        (self.highest_priority must be set):
        """
        if not self.highest_priority :
            self.logger.error(("highest priority must be set before "
                "initializing SQS!"))
            return
        self.sqs = SQS(self.inicfg, self.highest_priority)
        if not self.sqs.check_queues():
            self.logger.critical(("SQS queue initialization error! Exit!"))
            sys.exit(1)

    def get_startconfig_from_userdata(self, userdata_file_path):
        """
        This function inverts the actions done in
        ResourceManager.Session.generate_userdata()
        This means:
            - decode strong with b64
            - make ConfigParser config out of zipped string using
              SafeConfigParserStringZip
        """
        logger.debug("read userdata file %s ..." % userdata_file_path)
        b64zipcfg = open(userdata_file_path).read()
        zipcfg = base64.b64decode(b64zipcfg)
        config = SafeConfigParserStringZip()
        logger.debug("re-create ConfigParser config from zipped string..")
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
            logger.error("'grep -c processor /proc/cpuinfo' error: %s" % stderr)
            return False
        try:
            nbr_cpus = int(stdout)
        except ValueError:
            logger.error(("stdout of 'grep -c processor /proc/cpuinfo' was not a"
                " number: %s" % stdout))
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
        log_filename_prefix = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        rm_log_file_path = os.path.join(logdir,log_filename_prefix+"_JobAgent.log")
        boto_log_file_path = os.path.join(logdir,log_filename_prefix+"_boto.log")

        # set up main/root logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        # file handler for a real file with level DEBUG
        self.fh = logging.FileHandler(rm_log_file_path, encoding="UTF-8")
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


def parseargs():
    """
    Parse commandlineoptions using the optparse module and check them for
    logical consistence. Generate help- and usage-output.

    @return: optparse `options`-object containing the commandline options
    """
    version = '0'
    description = ("Clobi Job Agent")
    usage = ("%prog --userdatafile path --instanceidfile path \n"
             "try -h, --help and --version")
    parser = optparse.OptionParser(
        usage=usage,
        version=version,
        description=description)

    parser.add_option('-u', '--userdatafile', dest='userdata_file_path',
                      help='file containing userdata string')
    #parser.add_option('-i', '--instanceidfile', dest='instanceid_file_path',
    #                  help='file containing instanceid string')

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

