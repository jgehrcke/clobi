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
    Tee, Object)

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

        # init SimpleDB domains
        self.check_domains()

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
        item = self.boto_domainobj_session.get_item('session_props')
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

        self.prefix = self.inicfg.sessionid
        self.suffix = "_P"

        # init boto objects
        self.logger.debug("create SQS connection object")
        self.sqsconn = boto.connect_sqs(self.inicfg.aws_accesskey,
                                        self.inicfg.aws_secretkey)

        # to be populated..
        self.queues_priorities_botosqsqueueobjs = None
        self.queues_priorities_names = None
        self.highest_priority = None

        self.generate_queues_priorities_names(initial_highest_priority)

    def generate_queues_priorities_names(self, highest_priority):
        """
        Populate self.queues_priorities_names:
        For each priority put together a string: the queue name. Then put it
        into a dict with the priority (integer) as key.
        """
        self.highest_priority = int(highest_priority)
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
        existing_queues = self.sqsconn.get_all_queues(prefix=self.prefix)
        self.queues_priorities_botosqsqueueobjs = {}
        for prio, queue_name in self.queues_priorities_names.items():
            self.logger.info(("check queue %s for priority %s"
                % (queue_name,prio)))
            found = False
            for q in existing_queues:
                if q.url.endswith(queue_name):
                    self.logger.info(("SQS queue %s for priority %s is"
                        " existing" % (q.url,prio)))
                    found = True
                    self.queues_priorities_botosqsqueueobjs[prio] = q
                    break
            if not found:
                self.logger.critical(("SQS queue for priority %s is not "
                    "existing, but it must! Exit!" % prio))
                sys.exit(1)
            self.logger.info(("SQS queue for priority "+str(prio)
                             +" is now available: "+ str(q.url)))


class JobAgent(object):
    def __init__(self, start_options):
        """
        Evaluate given userdata. Read instance ID. Get number of cores.
        """
        self.logger = logging.getLogger("jobagent.py.JobAgent")
        self.logger.debug("initialize JobAgent object")

        userdata_file_path = check_file(start_options.userdata_file_path)
        #instanceid_file_path = check_file(start_options.instanceid_file_path)

        startconfig = get_startconfig_from_userdata(userdata_file_path)
        logger.debug("startconfig: \n%s" % config_to_string(startconfig))
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
        nbr_cores = get_number_of_cores()
        if nbr_cores:
            self.inicfg.nbr_cores = nbr_cores
            self.logger.info("number of cores: %s" % nbr_cores)
        else:
            self.inicfg.nbr_cores = None

        # to be populated..
        self.sdb = None
        self.sqs = None
        self.highest_priority = None

    def init_sdb(self):
        self.sdb = SimpleDB(self.inicfg)
        self.highest_priority = self.sdb.get_highest_priority()

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
        self.sqs.check_queues()




def config_to_string(configparserconfig):
    configstringlist = []
    for section in configparserconfig.sections():
        for option in configparserconfig.options(section):
            val = configparserconfig.get(section,option)
            configstringlist.append("%s:%s=%s" % (section,option,val))
    return '\n'.join(configstringlist)


def get_number_of_cores():
    sp = subprocess.Popen(
        args=['grep -c processor /proc/cpuinfo'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True)
    stdout, stderr = sp.communicate()
    if stderr:
        logger.error("'grep -c processor /proc/cpuinfo' error: %s" % stderr)
        return False
    try:
        nbr_cpus = int(stdout)
    except TypeError:
        logger.error(("stdout of 'grep -c processor /proc/cpuinfo' was not a"
            " number: %s" % stdout))
        return False
    return nbr_cpus


def get_startconfig_from_userdata(userdata_file_path):
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

