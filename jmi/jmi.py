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
import random
import hashlib
import traceback
import subprocess

from components.cfg_parse_strzip import SafeConfigParserStringZip
from components.utils import *

sys.path.append("components")
import boto


def main():
    # define and create JMI's log dir (for stdout / stderr of this script)
    jmi_log_dir = "jmi_log"
    jmi_log_dir = os.path.abspath(jmi_log_dir)
    if not os.path.exists(jmi_log_dir):
        os.makedirs(jmi_log_dir)

    # here, the input and output sandbox archives of the jobs will be stored
    jmi_sandboxarc_dir = "jmi_sandboxarc_dir"
    jmi_sandboxarc_dir = os.path.abspath(jmi_sandboxarc_dir)
    if not os.path.exists(jmi_sandboxarc_dir):
        os.makedirs(jmi_sandboxarc_dir)

    # here, a file will be written for each successfully submitted job. the file
    # has the job ID in its name and contains it.
    jmi_jobid_dir = "jmi_jobid_dir"
    jmi_jobid_dir = os.path.abspath(jmi_jobid_dir)
    if not os.path.exists(jmi_jobid_dir):
        os.makedirs(jmi_jobid_dir)

    # log stderr to stderr and to a real file
    stderr_logfile_path = os.path.join(
        jmi_log_dir,
        "%s_JMI_stderr.log" % utc_timestring())
    stderr_log_fd = open(stderr_logfile_path,'w')
    sys.stderr = Tee(sys.stderr, stderr_log_fd)

    # set up logger
    rootlog = JMILogger(jmi_log_dir)
    logger.debug("parse commandline arguments..")
    start_options = parseargs()

    jmi = JobManagementInterface(start_options,jmi_sandboxarc_dir,jmi_jobid_dir)
    jmi.act()


class JobManagementInterface(object):
    """
    This class provides an interface to Clobi's job scheduling infrastructure.
    Basically, it implements submit, remove, kill, monitor and receive ouput.
    An instance of this class is initialized with specific job information.
    Hence, **this is the Job Management Interface for a specific job**
    """
    def __init__(self, options,jmi_sandboxarc_dir, jmi_jobid_dir):
        self.logger = logging.getLogger("jmi.py.JobManagementInterface")
        self.logger.debug("initialize JobManagementInterface object")

        # constructor arguments
        self.jmi_sandboxarc_dir = check_dir(jmi_sandboxarc_dir)
        self.jmi_jobid_dir = check_dir(jmi_jobid_dir)
        self.jmi_config_file_path = check_file(options.jmi_config_file_path)
        self.options = options

        # parse JMI config file
        self.parse_jmi_config_file()

        # to be populated
        self.job = Object()

    def act(self):
        """
        Perform an action on the job as defined in self.options
        """
        if self.options.submit:
            self.job_config_file_path = check_file(
                self.options.job_config_file_path)
            self.parse_job_config_file()
            self.submit_job()
        else:
            # get Job ID from commandline
            self.job.id = self.options.job_id
        if self.options.remove:
            self.remove_job()
        elif self.options.kill:
            self.kill_job()
        elif self.options.monitor:
            self.monitor_job()
        elif self.options.rcv_output_sandbox:
            self.receive_output_sandbox_of_job()

    def kill_job(self):
        """
        Init SDB and mark job with the kill flag. This simply sets the kill flag
        without checking job's state. If the job is not initialized by a Job
        Agent, the kill flag has the same effect as the 'removed' status: The
        job will be rejected and the SQS msg is deleted by the Job Agent. If the
        job is already initialized, the Job Agent will periodically check the
        kill flag and -- in case it is set -- kill the job.
        """
        if not self.init_sdb_jobs_domain():
            self.logger.info("SDB jobs domain initialization error.")
            return False
        self.logger.info(("Mark job item %s in domain %s with kill flag."
            % (self.job.id,self.sdb_domainobj_jobs.name)))
        try:
            item = self.sdb_domainobj_jobs.put_attributes(
                item_name=self.job.id,
                attributes=dict(kill_flag='1'))
            self.logger.info("kill flag set.")
            return True
        except:
            self.logger.critical("SDB error")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False

    def remove_job(self):
        """
        Init SDB and try to mark job as removed. If the job item is alredy
        existing in the SDB jobs domain, then a Job Agent is already working on
        the job and it cannot be removed anymore.
        """
        if not self.init_sdb_jobs_domain():
            self.logger.info("SDB jobs domain initialization error.")
            return False
        self.logger.info(("Try to remove job with item %s in domain %s..."
            % (self.job.id,self.sdb_domainobj_jobs.name)))
        try:
            item = self.sdb_domainobj_jobs.get_item(self.job.id)
        except:
            self.logger.critical("SDB error")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False
        if item is not None:
            if 'kill_flag' in item and item['kill_flag'] == '1':
                self.logger.error(("Job %s is already marked to be killed."
                    % self.job.id))
            if 'status' in item:
                if item['status'] == 'removed':
                    self.logger.error(("Job %s is already marked to be removed."
                        % self.job.id))
                elif (item['status'] == 'initialized' or
                item['status'] == 'running'):
                    self.logger.error(("Job %s is already initialized/running."
                        " You can set the kill flag if you like."%self.job.id))
                elif (item['status'] == 'save_output' or
                item['status'].startswith('completed') or
                item['status'] == 'run_error'):
                    self.logger.error(("Job %s is already completed or near"
                        " completion." % self.job.id))
            return False
        try:
            self.logger.info(("Item %s did not exist (no Job Agent working on"
                " this job). Create item and set 'status' to 'removed'..."
                % self.job.id))
            item = self.sdb_domainobj_jobs.new_item(self.job.id)
            item['status'] = 'removed'
            item['removedtime'] = utc_timestring()
            item.save()
            self.logger.info("job marked to be removed.")
            return True
        except:
            self.logger.critical("SDB error")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False

    def monitor_job(self):
        """
        Initialize SDB and receive and return complete Job item
        """
        if not self.init_sdb_jobs_domain():
            self.logger.info("SDB jobs domain initialization error.")
            return False
        self.logger.info("SDB jobs domain successfully initialized.")
        self.logger.debug(("Retrieve item %s from SDB domain %s"
            % (self.job.id,self.sdb_domainobj_jobs.name)))
        try:
            item = self.sdb_domainobj_jobs.get_item(self.job.id)
        except:
            self.logger.error("Error while retrieving item.")
            self.logger.error("Traceback:\n%s"%traceback.format_exc())
            return False
        if item is not None:
            self.logger.info("\n\nCONTENT OF ITEM %s:" % item.name)
            for key in item.keys():
                self.logger.info("%s: %s" % (key, item[key]))
            return item
        else:
            self.logger.error("Item does not exist.")
            return False

    def submit_job(self):
        """
        Generate Job ID. Generate Inout/Output sandbox archive filenames and
        storage service keys. Build SQSJobMessage. Build/Upload input sandbox.
        Send SQSJobMessage. Return Job ID (and even write it to a file in a JMI
        subdirectory: submitted_jobs/job_id.id).
        """
        self.generate_job_id()
        self.in_sandbox_arc_filename = self.gen_in_sandbox_arc_filename()
        self.in_sandbox_arc_file_path = os.path.join(
            self.jmi_sandboxarc_dir, self.in_sandbox_arc_filename)
        self.out_sandbox_arc_filename = self.gen_out_sandbox_arc_filename()
        self.out_sandbox_arc_file_path = os.path.join(
            self.jmi_sandboxarc_dir, self.out_sandbox_arc_filename)
        in_sandbox_arc_key = self.gen_in_sandbox_archive_key()
        out_sandbox_arc_key = self.gen_out_sandbox_archive_key()
        self.logger.debug(("Generated in_sandbox_arc_filename:%s ;"
            " out_sandbox_arc_filename:%s ; out_sandbox_arc_key:%s ;"
            " in_sandbox_arc_key:%s" % (
                self.in_sandbox_arc_filename,
                self.out_sandbox_arc_filename,
                out_sandbox_arc_key,
                in_sandbox_arc_key)))

        jobmsg = SQSJobMessage()
        jobmsg.init_write()
        jobmsg.set_job_id(self.job.id)
        jobmsg.set_executable(self.job.executable)

        jobmsg.set_sandbox_storage_service(self.sandbox_storage_service)
        jobmsg.set_sandbox_bucket(self.sandbox_bucket)
        jobmsg.set_input_sandbox_archive_key(in_sandbox_arc_key)
        jobmsg.set_output_sandbox_arc_filename(self.out_sandbox_arc_filename)
        jobmsg.set_output_sandbox_archive_key(out_sandbox_arc_key)

        jobmsg.set_job_msg_creation_time(utc_timestring())
        jobmsg.set_output_sandbox_files(self.job.output_sandbox_files)
        jobmsg.set_job_owner(self.job.owner)
        jobmsg.set_production_system_job_id(self.job.production_system_job_id)
        jobmsg_str = jobmsg.string()
        jobmsg_zip_str = jobmsg.zip_string()
        logger.info(("SQS Job message:\n%s\nlength:%s"
            % (jobmsg_str,len(jobmsg_str))))
        logger.info("zip(SQS Job message):\n%s\nlength:%s"
            % (repr(jobmsg_zip_str),len(jobmsg_zip_str)))

        if self.build_input_sandbox_arc():
            self.logger.info("Input sandbox archive successfully built")
            if self.upload_file(
            file=self.in_sandbox_arc_file_path,
            bucketname=self.sandbox_bucket,
            key=in_sandbox_arc_key):
                self.logger.info("Input sandbox archive successfully uploaded.")
                if self.submit_sqs_message(self.job.priority, jobmsg_zip_str):
                    self.logger.info("Job message successfully sent to SQS.")
                    job_id_file_path = os.path.join(
                        self.jmi_jobid_dir,
                        self.job.id+".id")
                    fd = open(job_id_file_path,'w')
                    fd.write(self.job.id)
                    fd.close()
                    self.logger.info(("Job with ID %s successfully submitted."
                        " Wrote ID to %s" % (self.job.id,job_id_file_path)))

    def parse_jmi_config_file(self):
        """
        Parse Clobi's Job Management Interface configuration file using
        ConfigParser.
        """
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
        """
        Parse job configuration file using ConfigParser. If `priority` or
        `job_owner` are not given, use default values.
        """
        self.logger.debug(("Parse Clobi's Job configuration"
            " file %s ..." % self.job_config_file_path))
        job_config = ConfigParser.SafeConfigParser()
        job_config.readfp(open(self.job_config_file_path))

        self.job.executable = job_config.get(
            'job_config',
            'executable')
        try:
            self.job.owner = job_config.get(
                'job_config',
                'job_owner')
        except:
            self.job.owner = 'none'
        self.job.output_sandbox_files = job_config.get(
            'job_config',
            'output_sandbox_files')
        self.job.input_sandbox_files = job_config.get(
            'job_config',
            'input_sandbox_files')
        self.job.production_system_job_id = job_config.get(
            'job_config',
            'production_system_job_id')
        try:
            self.job.priority = job_config.getint(
                'job_config',
                'priority')
        except:
            self.job.priority = 1
        self.logger.debug("success!")

    def generate_job_id(self):
        """
        Generate job ID from current time, job owner name and random string
        -> Should be unique  ;-)
        """
        timestr = time.strftime("%y%m%d%H%M%S",time.gmtime())
        ownerhash = hashlib.sha1(self.job.owner).hexdigest()[:4]
        rndstring = "%s%s%s" % (timestr, random.random(), self.job.owner)
        rndhash = hashlib.sha1(rndstring).hexdigest()[:4]
        job_id = "job-%s-%s-%s" % (timestr,ownerhash,rndhash)
        self.logger.info("generated job ID: %s " % job_id)
        self.job.id = job_id

    def submit_sqs_message(self, priority, msg):
        """
        Submit a new message to SQS. Choose the queue that corresponds to the
        given priority.
        """
        try:
            sqsconn = boto.connect_sqs(self.aws_accesskey,self.aws_secretkey)
            queues = sqsconn.get_all_queues()
        except:
            self.logger.error("Error while receiving all available queues.")
            self.logger.error("Traceback:\n%s"%traceback.format_exc())
            return False
        for q in queues:
            if q.url.endswith("P%s" % priority):
                # By default, the class boto.sqs.message.Message is used.
                # This does base64 encoding itself
                sqs_msg = q.new_message(body=msg)
                try:
                    q.write(sqs_msg)
                except:
                    self.logger.error("Error while writing message.")
                    self.logger.error("Traceback:\n%s"%traceback.format_exc())
                    return False
                return True
        logger.info("No queue found for priority %s" % priority)
        return False

    def upload_file(self, file, bucketname, key):
        """
        Upload file to `self.sandbox_storage_service`. Currently, only
        S3 is supported; Cumulus follows.
        Return True in case of success
        """
        self.logger.info(("send %s to %s"
            % (file,self.sandbox_storage_service)))
        if self.sandbox_storage_service.lower() == 's3':
            try:
                conn = boto.connect_s3(self.aws_accesskey,self.aws_secretkey)
                bucket = conn.lookup(bucket_name=bucketname.lower())
                k = boto.s3.key.Key(bucket)
                k.key = key
                self.logger.info(("store file %s as key %s to bucket %s"
                    % (file, k.key, bucket.name)))
                k.set_contents_from_filename(file)
                return True
            except:
                self.logger.critical("Error while uploading.")
                self.logger.critical("Traceback:\n%s"%traceback.format_exc())
        else:
            self.logger.error("unkown storage service")
        return False

    def download_file(self, outfile, bucketname, key):
        """
        Download key from `self.sandbox_storage_service`. Currently, only
        S3 is supported; Cumulus follows.
        Return True in case of success.
        """
        if os.path.exists(outfile):
            self.logger.error("outfile %s already exists." % outfile)
            return False
        self.logger.info(("download %s/%s from %s"
            % (bucketname,key,self.sandbox_storage_service)))
        if self.sandbox_storage_service.lower() == 's3':
            try:
                conn = boto.connect_s3(self.aws_accesskey,self.aws_secretkey)
                bucket = conn.lookup(bucket_name=bucketname.lower())
                k = boto.s3.key.Key(bucket)
                k.key = key
                self.logger.info(("store key %s as file %s from bucket %s"
                    % (k.key, file, bucket.name)))
                k.get_contents_to_filename(outfile)
                return True
            except:
                self.logger.critical("Error while downloading.")
                self.logger.critical("Traceback:\n%s"%traceback.format_exc())
                self.logger.error(("Download failed. 404 Not Found?"
                    " -> means that the requested archive wasn't found. Did"
                    " the job already run? Did it succeed?"))
                if os.path.exists(outfile):
                    self.logger.debug(("%s exists and"
                        " is deleted now." % outfile))
                    os.remove(outfile)
        else:
            self.logger.error("unkown storage service")
        return False

    def build_input_sandbox_arc(self):
        """
        Start subprocess  tar cjf arc.tar.gz  x x x' to
        compress all desired input files into an archive.
        """
        # at first process the filenames that were given in the job config
        filename_list = self.job.input_sandbox_files.split(";")
        tar_files_list = ' '.join(filename_list)

        # build change directory string to cd to dir of job cfg file
        cd = "-C %s" % os.path.abspath(os.path.dirname(
            self.job_config_file_path))

        # build up tar cmd
        cmd = ("tar cjf %s --verbose %s %s"
            % (self.in_sandbox_arc_file_path, cd, tar_files_list))
        self.logger.info(("run input sandbox compression as subprocess:"
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
                # existing stderr does not necessarily mean that the archive
                # wasn't created.
                if os.path.exists(self.in_sandbox_arc_file_path):
                    self.logger.debug(("error during archiving. %s exists and"
                        " is deleted now." % self.in_sandbox_arc_file_path))
                    os.remove(self.in_sandbox_arc_file_path)
                return False
        except:
            self.logger.critical("Error while compressing output sandbox")
            self.logger.critical("Traceback:\n%s"%traceback.format_exc())
            return False
        return True

    def init_sdb_jobs_domain(self):
        """
        Perform all necessary steps to get a working boto SDB domain object.
        If this method is successfull, after run the following attributes are
        set: `self.sdbconn` and `self.sdb_domainobj_jobs`
        """
        sdbconn = self.connect_sdb()
        if sdbconn:
            self.sdbconn = sdbconn
            # hard coded SDB domain name convention for the jobs' domain
            domainobj = self.lookup_domain("%s_jobs"%self.jmi_session_id)
            if domainobj:
                self.sdb_domainobj_jobs = domainobj
                return True
        return False

    def connect_sdb(self):
        """
        Connect to SDB and return boto connection object (or False).
        """
        self.logger.debug("connect to SDB...")
        try:
            sdbconn = boto.connect_sdb(
                self.aws_accesskey,
                self.aws_secretkey)
            return sdbconn
        except:
            self.logger.error("Error while connecting to SDB.")
            self.logger.error("Traceback:\n%s"%traceback.format_exc())
        return False

    def lookup_domain(self, domainname):
        """
        Look up domain. Return boto SDB domain object or False.
        """
        self.logger.debug("look up SDB domain "+domainname+" ...")
        try:
            domainobj = self.sdbconn.lookup(domainname)
        except:
            self.logger.error("Error while SDB domain lookup.")
            self.logger.error("Traceback:\n%s"%traceback.format_exc())
            return False
        if domainobj is None:
            self.logger.error(("SDB domain does not exist: %s." % domainname))
            return False
        self.logger.info("SDB domain %s is now available." % domainname)
        return domainobj

    def receive_output_sandbox_of_job(self):
        """
        Re-generate output sandbox archive location from JMI cfg and job ID.
        Download archive to `self.jmi_sandboxarc_dir`.
        """
        # reassemble output sandbox archive location from JMI cfg (session ID,
        # sandbox bucket) and Job ID
        out_sandbox_arc_key = self.gen_out_sandbox_archive_key()
        self.out_sandbox_arc_filename = self.gen_out_sandbox_arc_filename()
        self.out_sandbox_arc_file_path = os.path.join(
            self.jmi_sandboxarc_dir, self.out_sandbox_arc_filename)
        if self.download_file(
        outfile=self.out_sandbox_arc_file_path,
        bucketname=self.sandbox_bucket,
        key=out_sandbox_arc_key):
            self.logger.info("Download of output sandbox archive successfull.")

    def gen_in_sandbox_arc_filename(self):
        """
        Generate input sandbox archive filename from Job ID
        """
        return "in_sndbx_%s.tar.bz2" % self.job.id

    def gen_out_sandbox_arc_filename(self,):
        """
        Generate output sandbox archive filename from Job ID
        """
        return "out_sndbx_%s.tar.bz2" % self.job.id

    def gen_in_sandbox_archive_key(self):
        """
        Generate input sandbox archive key for the storage service. Build the
        key from the input sandbox archive filename.
        """
        return os.path.join(
            self.jmi_session_id,
            "jobs",
            self.gen_in_sandbox_arc_filename())

    def gen_out_sandbox_archive_key(self):
        """
        Generate output sandbox archive key for the storage service. Build the
        key from the output sandbox archive filename.
        """
        return os.path.join(
            self.jmi_session_id,
            "jobs",
            self.gen_out_sandbox_arc_filename())


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
