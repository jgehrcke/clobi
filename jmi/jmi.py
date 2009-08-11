# -*- coding: UTF-8 -*-
#
#   ::::::::> Clobi Job Management Interface (reference client) <::::::::
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

import logging
import time
import sys
import base64
import optparse
import ConfigParser

from components.cfg_parse_strzip import SafeConfigParserStringZip
from components.utils import *

sys.path.append("components")
import boto


def main():
    # define and create JMI's log dir (for stdout / stderr of this script)
    jmi_logdir = "jmi_log"
    jmi_logdir = os.path.abspath(jmi_logdir)
    if not os.path.exists(jmi_logdir):
        os.makedirs(jmi_logdir)

    # log stderr to stderr and to a real file
    stderr_logfile_path = os.path.join(
        jmi_logdir,
        "%s_JMI_stderr.log" % utc_timestring())
    stderr_log_fd = open(stderr_logfile_path,'w')
    sys.stderr = Tee(sys.stderr, stderr_log_fd)

    # set up logger
    rootlog = JMILogger(jmi_logdir)
    logger.debug("parse commandline arguments..")
    start_options = parseargs()

    jmi = JobManagementInterface(start_options)
    jmi.act()



# job_id = 'job-01'
# input_sandbox_arc_filename = "input_sandbox_arc_%s.tar.bz2" % job_id
# output_sandbox_arc_filename = "output_sandbox_arc_%s.tar.bz2" % job_id
# input_sandbox_archive_key = "%s/jobs/%s" % (session_id,input_sandbox_arc_filename)
# output_sandbox_archive_key = "%s/jobs/%s" % (session_id,output_sandbox_arc_filename)

    # jobmsg = SQSJobMessage()
    # jobmsg.init_write()
    # jobmsg.set_job_id(job_id)
    # jobmsg.set_executable('execute.sh')

    # jobmsg.set_storage_service('S3')
    # jobmsg.set_sandbox_archive_bucket('atlassessions')
    # jobmsg.set_input_sandbox_archive_key(input_sandbox_archive_key)
    # jobmsg.set_output_sandbox_arc_filename(output_sandbox_arc_filename)
    # jobmsg.set_output_sandbox_archive_key(output_sandbox_archive_key)

    # jobmsg.set_job_msg_creation_time(utc_timestring())
    # jobmsg.set_output_sandbox_files("pillepalle.blub;stuff.txt")
    # jobmsg.set_job_owner("jgehrcke")
    # jobmsg.set_production_system_job_id(str(1))
    # jobmsg_str = jobmsg.string()
    # jobmsg_zip_str = jobmsg.zip_string()
    # logger.info("SQS Job message:\n%s\nlength:%s" % (jobmsg_str,len(jobmsg_str)))
    # logger.info("zip(SQS Job message):\n%s\nlength:%s"
        # % (repr(jobmsg_zip_str),len(jobmsg_zip_str)))

    # s3_upload_file(
        # file=input_sandbox_arc_filename,
        # bucketname="atlassessions",
        # key=input_sandbox_archive_key)

    # read_jobmsg = SQSJobMessage()
    # read_jobmsg.init_read(jobmsg_zip_str)
    # print read_jobmsg.get_output_sandbox_files()

    # add_message(1, jobmsg_zip_str)

def add_message(prio, msg):
    sqsconn = boto.connect_sqs(aws_accesskey,aws_secretkey)
    queues = sqsconn.get_all_queues()
    for q in queues:
        if q.url.endswith("P%s" % prio):
            # By default, the class boto.sqs.message.Message is used.
            # This does base64 encoding itself
            sqs_msg = q.new_message(body=msg)
            q.write(sqs_msg)
            return True
    logger.info("No queue found for priority %s" % prio)
    return False

def s3_upload_file(file, bucketname, key):
    """
    Upload a file as S3 object to bucket/key.
    """
    conn = boto.connect_s3(aws_accesskey,aws_secretkey)
    bucket = conn.create_bucket(bucketname.lower())
    k = boto.s3.key.Key(bucket)
    k.key = key
    k.set_contents_from_filename(file)
    return True


class JobManagementInterface(object):
    """
    This class provides an interface to Clobi's job scheduling infrastructure.
    Basically, it implements submit, remove, kill, monitor and receive ouput.
    An instance of this class is initialized with specific job information.
    Hence, **this is the Job Management Interface for a specific job**
    """
    def __init__(self, options):
        self.logger = logging.getLogger("jmi.py.JobManagementInterface")
        self.logger.debug("initialize JobManagementInterface object")

        # constructor arguments
        self.jmi_config_file_path = check_file(options.jmi_config_file_path)
        self.job_config_file_path = check_file(options.job_config_file_path)
        self.options = options

        # parse config files
        self.parse_jmi_config_file()
        self.parse_job_config_file()

    def act(self):
        """
        Perform an action on the job as defined in self.options
        """
        if self.options.submit:
            self.submit_job()
        elif self.options.remove:
            self.remove_job()
        elif self.options.kill:
            self.kill_job()
        elif self.options.monitor:
            self.monitor_job()
        elif self.options.rcv_output_sandbox:
            self.receive_output_sandbox_of_job()

    def generate_job_id(self):
        pass

    def submit_job(self):
        pass

    def parse_jmi_config_file(self):
        self.logger.debug(("Parse Clobi's Job Management Interface config"
            " file %s ..." % self.jmi_config_file_path))
        jmi_config = ConfigParser.SafeConfigParser()
        jmi_config.readfp(open(self.jmi_config_file_path))

        self.aws_secretkey = jmi_config.get(
            'JMI_config',
            'jmi_aws_secretkey')
        self.aws_accesskey = jmi_config.get(
            'JMI_config',
            'jmi_aws_accesskey')
        self.sandbox_bucket = jmi_config.get(
            'JMI_config',
            'jmi_sandbox_bucket')
        self.sandbox_storage_service = jmi_config.get(
            'JMI_config',
            'jmi_sandbox_storage_service')
        self.jmi_session_id = jmi_config.get(
            'JMI_config',
            'jmi_session_id')
        self.logger.debug("success!")

    def parse_job_config_file(self):
        self.logger.debug(("Parse Clobi's Job configuration"
            " file %s ..." % self.job_config_file_path))
        job_config = ConfigParser.SafeConfigParser()
        job_config.readfp(open(self.job_config_file_path))

        self.job = Object()
        self.job.executable = job_config.get(
            'job_config',
            'executable')
        self.job.job_owner = job_config.get(
            'job_config',
            'job_owner')
        self.job.output_sandbox_files = job_config.get(
            'job_config',
            'output_sandbox_files')
        self.job.input_sandbox_files = job_config.get(
            'job_config',
            'input_sandbox_files')
        self.job.input_sandbox_files = job_config.get(
            'job_config',
            'input_sandbox_files')
        self.job.input_sandbox_files = job_config.get(
            'job_config',
            'production_system_job_id')
        self.logger.debug("success!")

    def submit(self):
        pass
        # generated on submission:
        # job ID
# output_sandbox_arc_filename = output_sandbox_arc_job-01.tar.bz2
# input_sandbox_archive_key = 0907210728-testsess-0c7e/jobs/input_sandbox_arc_job-01.tar.bz2
# output_sandbox_archive_key = 0907210728-testsess-0c7e/jobs/output_sandbox_arc_job-01.tar.bz2
# job_msg_creation_time = UTC20090811-051014

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
    def set_storage_service(self, storage_service):
        self.config.set(self.section,'storage_service',storage_service)
    def get_storage_service(self):
        return self.config.get(self.section,'storage_service')
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
    def set_sandbox_archive_bucket(self, bucket):
        self.config.set(self.section,'sandbox_archive_bucket',bucket)
    def get_sandbox_archive_bucket(self):
        return self.config.get(self.section,'sandbox_archive_bucket')
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

class JMILogger(object):
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
            log_filename_prefix+"_JMI.log")
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

def parseargs():
    """
    Parse commandlineoptions using the optparse module and check them for
    logical consistence. Generate help- and usage-output.

    @return: optparse `options`-object containing the commandline options
    """
    version = '%prog 0'
    description = ("Clobi Job Management Interface reference client")
    usage = ("\n%prog --submit --jmicfg path/jmi.cfg --jobfg path/job.cfg\n"
        "%prog [--kill,--monitor,...] --jmicfg path/jmi.cfg --jobid JobID\n"
        "try -h, --help and --version")
    parser = optparse.OptionParser(
        usage=usage,
        version=version,
        description=description)

    parser.add_option('--jmicfg', dest='jmi_config_file_path',
        help=('path to Job Management Interface configuration file.'
            ' Always required.'))
    parser.add_option('--jobcfg', dest='job_config_file_path',
        help=('path to Job description/configuration file.'
            ' Required for Job submission.'))
    parser.add_option('--jobid', dest='job_id',
        help=('Job ID (returned after submission). Required for remove/kill/'
            'monitor/recv-output-sandbox'))
    parser.add_option('--submit', action='store_true', dest='submit',
        default=False, help='Submit a new Job.')
    parser.add_option('--remove', action='store_true', dest='remove',
        default=False, help='Try to remove a Job.')
    parser.add_option('--kill', action='store_true', dest='kill',
        default=False, help="Kill a Job, even when it's already running.")
    parser.add_option('--monitor', action='store_true', dest='monitor',
        default=False, help='Get monitoring data for a specific Job.')
    parser.add_option('--rcv-output-sandbox', action='store_true',
        dest='rcv_output_sandbox', default=False,
        help='Receive output sandbox if Job is completed.')

    # now read in the given arguments (from sys.argv by default)
    (options, args) = parser.parse_args()

    # now check the logical consistence...
    if (int(options.submit)+int(options.remove)+int(options.kill)
    +int(options.monitor)+int(options.rcv_output_sandbox)) is not 1:
        parser.error(("Exactly one of [--submit, --monitor, --kill, "
            "--remove, --rcv-output-sandbox] must be set!"))
    if options.jmi_config_file_path is None:
        parser.error('--jmicfg path/to/jmi.cfg is always required!')
    if options.submit:
        if options.job_config_file_path is None:
            parser.error('--jobfg path/to/job.cfg is required for submission!')
    elif options.job_id is None:
        parser.error('--jobid JobID is required for all except --submit!')
    return options


if __name__ == "__main__":
    logger = logging.getLogger("jmi.py")
    main()
