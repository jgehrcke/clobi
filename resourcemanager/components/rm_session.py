﻿# -*- coding: UTF-8 -*-
#
#   ::::::::> RESOURCE MANAGER <::::::::
#   Resource Manager Session
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

import ConfigParser
import os
import sys
import logging
import hashlib
import random
import time
import subprocess
import shutil

from components.rm_nimbus_clntwrppr import NimbusClientWrapper
from components.cfg_parse_strzip import SafeConfigParserStringZip

sys.path.append("components")
import boto


class SQSSession(object):
    """
    Set up SQS for this session. Provide an interface to SQS (all
    ResourceManager communication with SQS is done via this class)
    """
    def __init__(self, initial_session_config, session_save_dir):
        self.logger = logging.getLogger("RM.MainLoop.SQSSession")
        self.logger.debug("initialize SQSSession object")

        self.inicfg = initial_session_config
        self.default_visibility_timeout = self.inicfg.sqs.default_visibility_timeout
        self.save_dir = session_save_dir
        self.save_config_file_path = os.path.join(self.save_dir,"save.session.sqs.config")

        # init boto objects
        self.logger.debug("create SQS connection object")
        self.sqsconn = boto.connect_sqs(initial_session_config.aws.accesskey,
                                        initial_session_config.aws.secretkey)

        self.queues_priorities_botosqsqueueobjs = None
        self.queues_priorities_names = None
        self.prefix = None
        self.suffix = "_P"
        self.highest_priority = None
        self.queue_jobnbrs_laststate = {}

    def set_prefix(self, prefix):
        """
        Set SQS queue name prefix (called from outside).
        """
        self.prefix = prefix
        self.logger.debug("SQS queue name prefix stored: "+self.prefix)

    def generate_queues_priorities_names(self, highest_priority):
        """
        Populate self.queues_priorities_names:
        For each priority put together a string: the queue name. Then put it
        into a dict with the priority (integer) as key.
        """
        self.highest_priority = int(highest_priority)
        self.queues_priorities_names = {}
        for p in range(1,self.highest_priority+1):
            self.queues_priorities_names[p] = "%s%s%s" % (self.prefix, self.suffix, p)
        outputstring = '\n'.join(
                      ["         queue name for priority %s: %s" % (key,value)
                       for key, value in self.queues_priorities_names.items()])
        self.logger.info(("generated SQS priority/queuename pairs:\n"
                          + outputstring))

    def query_queues(self):
        self.logger.debug("get attributes for all SQS queues...")
        for prio, queue  in self.queues_priorities_botosqsqueueobjs.items():
            attr = queue.get_attributes()
            jobnbr = attr['ApproximateNumberOfMessages']
            last_modified = attr['LastModifiedTimestamp']
            self.queue_jobnbrs_laststate[prio] = jobnbr
            self.logger.info(("queue for priority %s:\napproximate nbr of jobs: %s"
                " ; last modified: %s" % (prio, jobnbr, time.strftime("%Y%m%d-%H%M%S",
                    time.localtime(float(last_modified))))))

    def create_queues(self, resume = False):
        """
        Populate self.queues_priorities_botosqsqueueobjs with boto Queue objects
        Create queues in several steps:
            - Retrieve all existing SQS queues with this session prefix
            - take self.queues_priorities_names (containing priority/namy pairs
              this session NEEDS)
                - for each name in there look if a corresponding queue
                  exists online
                    yes: get attributes: empty?
                        no: resume mode?
                            no: must not happen-> EXIT
                    all else: store boto queue object to
                              self.queues_priorities_botosqsqueueobjs
        """
        self.logger.info("check queues with prefix "+self.prefix)
        existing_queues = self.sqsconn.get_all_queues(prefix=self.prefix)
        self.queues_priorities_botosqsqueueobjs = {}
        for prio, queue_name in self.queues_priorities_names.items():
            if resume:
                self.logger.info("continue with queue %s for priority %s" % (queue_name,prio))
            else:
                self.logger.info("create queue %s for priority %s" % (queue_name,prio))
            found = False
            for q in existing_queues:
                if q.url.endswith(queue_name):
                    self.logger.info(("SQS queue "+q.url+" for priority "+str(prio)
                                     +" is already existing -> explore queue attributes..."))
                    attr = q.get_attributes()
                    attrstr = '\n'.join(["         %s: %s" % (k,v) for k,v in attr.items()])
                    self.logger.info("attributes: \n"+attrstr)
                    if not resume:
                        self.logger.critical(("This *unique* SQS queue should "
                                      "not have existed before. Now we see "
                                      "it's even not empty. Please check what "
                                      "has happened! Exiting."))
                        #sys.exit()
                    found = True
                    self.queues_priorities_botosqsqueueobjs[prio] = q
                    break
            if not found:
                q = self.sqsconn.create_queue(queue_name, int(self.default_visibility_timeout))
                self.queues_priorities_botosqsqueueobjs[prio] = q
            self.logger.info(("SQS queue for priority "+str(prio)
                             +" is now available: "+ str(q.url)))

    def load_sqs_session_config_from_file(self):
        """
        Save current SQS session information from a file using ConfigParser.
        It actually could be generated from the session ID. But this method is
        here for completeness and potential future use.
        """
        config = ConfigParser.SafeConfigParser()
        config.readfp(open(check_file(self.save_config_file_path)))
        self.prefix = config.get('SQS_sessionconfig', 'prefix')
        self.suffix = config.get('SQS_sessionconfig', 'suffix')
        self.logger.info("loaded session SQS config from "+self.save_config_file_path + " .")

    def save_sqs_session_config_to_file(self):
        """
        Save current SQS session information to a file using ConfigParser.
        Currently, there is not much config to write. It actually would be
        enough to generate all from session ID. But this method is here for
        completeness and potential future use.
        """
        config = ConfigParser.SafeConfigParser()
        config.add_section('SQS_sessionconfig')
        config.set('SQS_sessionconfig', 'prefix', self.prefix)
        config.set('SQS_sessionconfig', 'suffix', self.suffix)
        fd = open(self.save_config_file_path,'w')
        config.write(fd)
        fd.close()
        self.logger.info("session SQS config written to "+self.save_config_file_path + " .")


class SimpleDBSession(object):
    """
    Set up SimpleDB for this session. Provide an interface to SDB (all
    ResourceManager communication with SDB is done via this class)
    """
    def __init__(self, initial_session_config, session_save_dir):
        self.logger = logging.getLogger("RM.MainLoop.SimpleDBSession")
        self.logger.debug("initialize SimpleDBSession object")

        self.inicfg = initial_session_config
        self.initial_highest_priority = initial_session_config.sqs.initial_highest_priority
        self.save_dir = session_save_dir
        self.save_config_file_path = os.path.join(self.save_dir,"save.session.sdb.config")


        # init attributes to be set in SDB
        self.highest_priority = None

        # init boto objects
        self.logger.debug("create SimpleDB connection object")
        self.sdbconn = boto.connect_sdb(initial_session_config.aws.accesskey,
                                        initial_session_config.aws.secretkey)
        self.boto_domainobj_session = None
        self.boto_domainobj_jobs = None

        # init domain names for session data (session & VM data) and job data
        self.domain_name_session = None
        self.domain_name_jobs = None

        self.prefix = None

    def set_prefix(self, prefix):
        """
        Set SDB domain name prefix. (called from outside)
        """
        self.prefix = prefix
        self.logger.debug("SDB domain name prefix stored: "+self.prefix)

    def generate_domain_names(self):
        """
        Generate domain names for all SDB domains this session needs
        (just strings).
        """
        self.domain_name_session = self.prefix+"_sess"
        self.domain_name_jobs = self.prefix+"_jobs"

    def create_domains(self):
        """
        Call self.create_domain() in non-resume mode for all domains this
        session needs.
        """
        self.logger.info("create SDB domain for session & VM data...")
        self.boto_domainobj_session = self.create_domain(self.domain_name_session)
        self.logger.info("create SDB domain for job data...")
        self.boto_domainobj_jobs = self.create_domain(self.domain_name_jobs)

    def continue_with_domains(self):
        """
        Call self.create_domain() in resume mode for all domains this session
        needs.
        """
        self.logger.info("continue with SDB domain for session & VM data...")
        self.boto_domainobj_session = self.create_domain(self.domain_name_session, resume=True)
        self.logger.info("continue with SDB domain for job data...")
        self.boto_domainobj_jobs = self.create_domain(self.domain_name_jobs, resume=True)

    def create_domain(self,domainname, resume=False):
        """
        Create a domain in several steps:
            - look up domain (is it already there?)
                no: create it and return boto domain object
                yes: get metadata. is domain empty?
                    no: is this a resumed session?
                        no: domain must not exist and is even populated->exit
                        yes: return boto domain object
                    yes: return boto domain object
        """
        self.logger.debug("look up SDB domain "+domainname+" ...")
        domainobj = self.sdbconn.lookup(domainname)
        if domainobj is None:
            self.logger.info("SDB domain "+domainname+" seems not to exist -> create...")
            domainobj = self.sdbconn.create_domain(domainname)
        else:
            self.logger.info(("SDB domain "+domainname
                                +" is already existing -> explore metadata..."))
            md = domainobj.get_metadata()
            self.logger.info((
            "metadata:"
            +"\n         item_count: " + str(md.item_count)
            +"\n         item_names_size: " + str(md.item_names_size)
            +"\n         attr_name_count: " + str(md.attr_name_count)
            +"\n         attr_names_size: " + str(md.attr_names_size)
            +"\n         attr_value_count: " + str(md.attr_value_count)
            +"\n         attr_values_size: " + str(md.attr_values_size)))
            if (md.item_count is not 0) and not resume:
                self.logger.critical(("This *unique* SimpleDB domain should "
                                      "not have existed before. Now we see "
                                      "it's even not empty. Please check what "
                                      "has happened! Exiting."))
                #sys.exit(("Simple DB domain for new session already existed "
                #          "and was not empty"))
        self.logger.info("SDB domain "+domainname+" is now available.")
        return domainobj

    def get_highest_priority(self):
        """
        Get HighestPriority flag from SDB
        """
        item = self.boto_domainobj_session.get_item('session_props')
        self.highest_priority = item['HighestPriority']
        self.logger.info("got HighestPriority from SDB: "+str(self.highest_priority))

    def create_session_props_item(self):
        """
        Set "Session Properties Item" in the SDB domain for for session data
        (session & VM data). The item contains e.g. the HighestPriority flag.
        """
        item = self.boto_domainobj_session.new_item('session_props')
        self.logger.info("setting HighestPriority to "+str(self.initial_highest_priority))
        item['HighestPriority'] = self.initial_highest_priority
        item.save()

    def load_sdb_session_config_from_file(self):
        """
        Save current SDB session information from a file using ConfigParser.
        It actually could be generated from the session ID. But this method is
        here for completeness and potential future use.
        """
        self.logger.info("load session SDB config from "+self.save_config_file_path + "...")
        config = ConfigParser.SafeConfigParser()
        config.readfp(open(check_file(self.save_config_file_path)))
        self.domain_name_session = config.get('SDB_sessionconfig', 'domain_name_session')
        self.domain_name_jobs = config.get('SDB_sessionconfig', 'domain_name_jobs')

    def save_sdb_session_config_to_file(self):
        """
        Save current SDB session information to a file using ConfigParser.
        Currently, there is not much config to write. It actually could be
        generated from the session ID. But this method is here for completeness
        and potential future use.
        """
        config = ConfigParser.SafeConfigParser()
        config.add_section('SDB_sessionconfig')
        config.set('SDB_sessionconfig', 'domain_name_session', self.domain_name_session)
        config.set('SDB_sessionconfig', 'domain_name_jobs', self.domain_name_jobs)
        fd = open(self.save_config_file_path,'w')
        config.write(fd)
        fd.close()
        self.logger.info("session SDB config written to "+self.save_config_file_path)


class Session(object):
    """
    This class represents and manages a whole ResourceManager Session. Therefore
    it instantiates and controles instances of other classes representing
    sub-components (SDB, SQS, EC2, Nimbus,..)
    """
    def __init__(self, start_options, session_dirs):
        self.logger = logging.getLogger("RM.MainLoop.Session")
        self.logger.debug("initialize Session object")

        if start_options.start:
            self.logger.info("MODE: START SESSION")
        elif start_options.resume:
            self.logger.info("MODE: RESUME SESSION")

        self.resume = start_options.resume
        self.session_config_file_path = start_options.session_config_file_path
        self.run_dir = session_dirs['session_run_dir']
        self.log_dir = session_dirs['session_log_dir']
        self.save_dir = session_dirs['session_save_dir']
        self.save_config_file_path = os.path.join(self.save_dir,"save.session.config")
        self.save_vms_file_path = os.path.join(self.save_dir,"save.session.vms")
        self.save_last_vm_id_file_path = os.path.join(self.save_dir,"save.session.last_vm_id")
        self.save_backup_dir = os.path.join(self.save_dir,"backup")
        if not os.path.exists(self.save_backup_dir):
            os.makedirs(self.save_backup_dir)

        # populated / changed during runtime
        self.session_id = None
        self.sdb_session = None
        self.sqs_session = None
        self.inicfg = None
        self.nimbus_clouds = None
        self.ec2 = None

    def generate_userdata(self, vm_id):
        """
        Generate userdata string for VM ID. This is the string delivered with
        the request. Hence, it must be small (zipped) and encoded for reliable
        transmission within Query URL (EC2) or SOAP message (Nimbus) -> Base64.
        """
        
        self.logger.info("generated userdata: '%s'" % vm_id)
        return vm_id

    def append_save_vms_file(self, savestring):
        """
        Append data to VMs save file. like: "vm_id;cloud;prepared".
        Backup before.
        """
        backup_file(self.save_vms_file_path, self.save_backup_dir)
        fd = open(self.save_vms_file_path,'a')
        fd.write(savestring)
        fd.close()
        self.logger.debug(("appended to file %s : %s" %
                           (self.save_vms_file_path, savestring)))

    def generate_vm_ids(self, number):
        """
        Create and return a bunch of new VM IDs in form of a list.
        To securely keep track of generated IDs, always save the last generated
        ID number to file. Load it from there on each call, too.
        """
        self.logger.debug(("read last VM ID number from file "+
                            self.save_last_vm_id_file_path))
        try:
            last_vm_id_number = int(open(self.save_last_vm_id_file_path,'r').read())
            self.logger.debug("read last VM ID: %s" % last_vm_id_number)
        except IOError:
            if self.resume:
                self.logger.critical(("session in resume mode and IOError on last "
                                      " VM ID file. Smells bad; exit!"))
                sys.exit(1)
            else:
                self.logger.debug("could not be opened. Start at 0")
                last_vm_id_number = 0

        vm_id_list = []
        for foo in xrange(number):
            last_vm_id_number += 1
            vm_id_list.append("vm-"+unicode(last_vm_id_number).zfill(4))

        self.logger.debug(("write last VM ID number (%s) to file %s"%
                           (last_vm_id_number,self.save_last_vm_id_file_path)))
        try:
            fd = open(self.save_last_vm_id_file_path,'w') # (over-)write mode
            fd.write(str(last_vm_id_number))
            fd.close()
        except IOError:
            self.logger.critical(("last VM ID number (%s) could not be saved: "
                                  "IOError. Exit!" % last_vm_id_number))
            sys.exit(1)
        return vm_id_list

    def run_vms_ec2(self, nbr):
        self.logger.info("instructed to run %s VMs on EC2" % nbr)

    def run_vms_nimbus(self, idx, nbr):
        for nbcloud in self.nimbus_clouds:
            if nbcloud.cloud_index == idx:
                self.logger.info("order to run %s VMs on Nimbus Cloud %s" % (nbr,idx))
                nbcloud.run_vms(number=nbr)
                return

    # def run_vm(self, args):
        # # where to run? at first, check Nimbus clouds. Then EC2.
        # for nimbus_cloud in self.nimbus_clouds:
            # if nimbus_cloud.number_of_running_vms < nimbus_cloud.inicfg.max_instances:
                # nimbus_cloud.run(args)

    def load_initial_session_config(self):
        """
        Load initial session config (from user-given file). This includes
        creating Nimbus cloud object(s) and loading their config files.
        These processes include validation of user-given information.
        """
        self.logger.info(("load initial session config from %s"
                           % self.session_config_file_path))
        isc = InitialSessionConfig(
            session_config_file_path=self.session_config_file_path,
            resume_session=self.resume)
        self.logger.debug(("initial session config: \n"
                        + isc.print_initial_session_config()))
        if isc.ec2.use:
            self.logger.info("EC2 is used within this session.")
        if isc.nimbus.use:
            self.logger.info("Nimbus is used within this session.")
            self.nimbus_clouds = []
            for idx, cfg in isc.nimbus.sorted_configfile_dict.items():
                nimbus_cloud = NimbusCloud(
                    session = self,
                    nimbus_config_file_abspath = cfg,
                    nimbus_cloud_index = idx)
                nimbus_cloud.load_initial_nimbus_cloud_config()
                nimbus_cloud.check_cloud_client_exists()
                self.nimbus_clouds.append(nimbus_cloud)
        self.inicfg = isc

    def finish_setup(self):
        """
        Central method to accomplish setup after loading inititial session
        config. At first, initialize all missing components with initial session
        config (SQS, SDB, ..).
        Then, distinguish NEW and RESUMED session to accomplish setup/configu-
        ration in every detail.
        """
        self.sdb_session = SimpleDBSession(self.inicfg,self.save_dir)
        self.sqs_session = SQSSession(self.inicfg,self.save_dir)
        if self.inicfg.resume_session:
            self.logger.info("resumed session: load missing config from files...")
            self.load_extended_config_from_file()
            self.set_up_simple_db_from_file()
            self.set_up_sqs_from_file()
        else:
            self.logger.info("new session: set up missing stuff...")
            self.generate_session_id()
            self.set_up_simple_db_from_scratch()
            self.set_up_sqs_from_scratch()
            self.logger.info("save all generated config to files...")
            self.save_extended_config_to_file()
            self.sdb_session.save_sdb_session_config_to_file()
            self.sqs_session.save_sqs_session_config_to_file()
        self.logger.info("create grid proxy for each Nimbus cloud...")
        for nimbus_cloud in self.nimbus_clouds:
            nimbus_cloud.grid_proxy_init()

    def generate_session_id(self):
        """
        Generate session ID from current time, session name (short description)
        and random string -> Should be unique  ;-)
        """
        sfx = hashlib.sha1(str(random.random())+str(time.time())).hexdigest()[:4]
        #self.session_id = (time.strftime("%y%m%d%H%M",time.localtime())
         #                  + "-" + self.inicfg.session.name[:8] + "-" + sfx)
        self.session_id = "0907210728-testsess-0c7e"
        self.logger.info("generated session ID: "+self.session_id+ " .")

    def set_up_simple_db_from_scratch(self):
        """
        Set SDB domain prefix. Build up domain names. Create SDB domains.
        Create session properties item in the SDB domain containing general
        session data. Within this item, e.g. the HighestPriority flag gets set.
        Requires self.sdb_session to be initialized before.
        """
        self.sdb_session.set_prefix(prefix=self.session_id)
        self.sdb_session.generate_domain_names()
        self.logger.info("create SDB domains...")
        self.sdb_session.create_domains()
        self.logger.info("set session properties in SDB (highest priority, ...)")
        self.sdb_session.create_session_props_item()

    def set_up_simple_db_from_file(self):
        """
        Load SDB session config from file and set up SDB domains in resume mode.
        Read HighestPriority flag the SDB domain that contains general session
        data. This value is then stored in self.sdb_session.highest_priority
        Requires self.sdb_session to be initialized before.
        """
        self.sdb_session.load_sdb_session_config_from_file()
        self.sdb_session.continue_with_domains()
        self.sdb_session.get_highest_priority()

    def set_up_sqs_from_scratch(self):
        """
        Set SQS queue name prefix.
        Create new SQS queues.
        Requires self.sqs_session to be initialized before.
        """
        self.sqs_session.set_prefix(prefix=self.session_id)
        self.sqs_session.generate_queues_priorities_names(
                    highest_priority=self.inicfg.sqs.initial_highest_priority)
        self.sqs_session.create_queues()

    def set_up_sqs_from_file(self):
        """
        Load SQS session config from file and set up SQS queues in resume mode.
        Requires self.sqs_session to be initialized before.
        Requires self.sdb_session.highest_priority set before (run
            e.g. set_up_simple_db_from_file() before)
        """
        self.sqs_session.load_sqs_session_config_from_file()
        self.sqs_session.generate_queues_priorities_names(
            highest_priority=self.sdb_session.highest_priority)
        self.sqs_session.create_queues(resume=True)

    def save_extended_config_to_file(self):
        """
        Save generated session information to a file using ConfigParser.
        """
        config = ConfigParser.SafeConfigParser()
        config.add_section('extended_sessionconfig')
        config.set('extended_sessionconfig', 'session_id', self.session_id)
        fd = open(self.save_config_file_path,'w')
        config.write(fd)
        fd.close()
        self.logger.info("extended session config written to "+self.save_config_file_path)

    def load_extended_config_from_file(self):
        """
        Load session information that is not provided by initial session config
        """
        self.logger.info("load extended session config from "+self.save_config_file_path + "...")
        config = ConfigParser.SafeConfigParser()
        config.readfp(open(check_file(self.save_config_file_path)))
        self.session_id = config.get('extended_sessionconfig', 'session_id')
        self.logger.info("loaded session ID: "+self.session_id)


class InitialSessionConfig(object):
    """
    An instance of this class provides initial session config, as it
    is delivered by the session config file (user-given).
    """
    def __init__(self, session_config_file_path, resume_session = False):
        # create logger
        self.logger = logging.getLogger("RM.MainLoop.InitialSessionConfig")
        self.logger.debug("initialize InitialSessionConfig object")

        # constructor arguments
        self.logger.debug("check session config file")
        self.session_config_file_abspath = check_file(session_config_file_path)
        self.session_directory = os.path.dirname(self.session_config_file_abspath)
        self.save_dir = os.path.join(self.session_directory,"save")
        self.resume_session = resume_session

        # others
        self.session_configparser_object = None

        # init variables that will be read from initial config file
        self.session = Object()
        self.session.name = None
        self.nimbus = Object()
        self.nimbus.use = None
        self.nimbus.sorted_configfile_dict = None
        self.aws = Object()
        self.aws.secretkey = None
        self.aws.accesskey = None
        self.ec2 = Object()
        self.ec2.use = None
        self.ec2.instancetype = None
        self.ec2.ami_id = None
        self.ec2.max_instances = None
        self.sdb = Object()
        self.sdb.monitor_vms_pollinterval = None
        self.sqs = Object()
        self.sqs.monitor_queues_pollinterval = None
        self.sqs.initial_highest_priority = None
        self.sqs.default_visibility_timeout = None

        self.parse_session_config_file()

    def print_initial_session_config(self):
        return (
          "\n* session_config_file_abspath: " + self.session_config_file_abspath
        + "\n* resume_session: " + unicode(self.resume_session)
        + "\n* session.name: " + self.session.name
        + "\n* nimbus.use: " + unicode(self.nimbus.use)
        + "\n* aws.accesskey: " + self.aws.accesskey
        + "\n* ec2.use: " + unicode(self.ec2.use)
        + "\n* ec2.instancetype: " + self.ec2.instancetype
        + "\n* ec2.ami_id: " + self.ec2.ami_id
        + "\n* ec2.max_instances: " + unicode(self.ec2.max_instances)
        + "\n* sdb.monitor_vms_pollinterval: " + unicode(self.sdb.monitor_vms_pollinterval)
        + "\n* sqs.monitor_queues_pollinterval: " + unicode(self.sqs.monitor_queues_pollinterval)
        + "\n* sqs.initial_highest_priority: " + unicode(self.sqs.initial_highest_priority)
        + "\n* sqs.default_visibility_timeout: "+ unicode(self.sqs.default_visibility_timeout)
        + "\n"
        )

    def parse_session_config_file(self):
        """
        Parse config file, set variables for this session
        """
        self.logger.debug("parse session config file with SafeConfigParser")
        session_config = ConfigParser.SafeConfigParser()
        session_config.readfp(open(self.session_config_file_abspath))

        self.session.name = session_config.get('session', 'name').decode('UTF-8')
        self.aws.secretkey = session_config.get('AWS', 'aws_secret_key')
        self.aws.accesskey = session_config.get('AWS', 'aws_access_key')

        self.sdb.monitor_vms_pollinterval = session_config.getfloat('SimpleDB',
                                                'monitor_VMs_pollinterval')
        self.sqs.monitor_queues_pollinterval = session_config.getfloat('SQS',
                                                'monitor_queues_pollinterval')
        self.sqs.initial_highest_priority = session_config.getint('SQS',
                                                'initial_highest_priority')
        self.sqs.default_visibility_timeout = session_config.getint('SQS',
                                                'default_visibility_timeout')
        self.ec2.use = False
        if session_config.has_section('EC2'):
            self.ec2.instancetype = session_config.get('EC2', 'instancetype').decode('UTF-8')
            self.ec2.ami_id = session_config.get('EC2', 'ami_id').decode('UTF-8')
            self.ec2.max_instances = session_config.getint('EC2', 'max_instances')
            self.ec2.use = True

        self.nimbus.use = False
        if session_config.has_section('Nimbus'):
            # get all Nimbus cloud option names.
            # (their values are pointing to Nimbus cloud config files)
            nimbuscloudoptions = session_config.options('Nimbus')
            if len(nimbuscloudoptions) > 0:
                self.logger.debug("populated [Nimbus] section found")
                self.nimbus.use = True
                # these option names are of the type 'nimbus_cloud_N' -> sort it.
                nimbuscloudoptions.sort()
                # dict will contain cloud index as key, config file as value
                self.nimbus.sorted_configfile_dict = {}
                # initialize a NimbusCloudConfig instance for each value
                cloud_index = 1
                for option in nimbuscloudoptions:
                    # build absolute path of nimbus config file (whose path may
                    # be given relatively to the session config file's path)
                    nimbus_config_file_abspath = check_file(os.path.join(
                        os.path.dirname(self.session_config_file_abspath),
                        session_config.get('Nimbus', option).decode('UTF-8')))
                    self.nimbus.sorted_configfile_dict[cloud_index] =                                                      nimbus_config_file_abspath
                    self.logger.debug(("config file for Nimbus cloud with index"
                                       " %s: %s") % (cloud_index,
                                       nimbus_config_file_abspath))
                    cloud_index += 1
        self.session_configparser_object = session_config

    def print_parsed_session_config_file(self):
        if self.session_configparser_object is not None:
            print  "\nconfig read from " + self.session_config_file_abspath + " :"
            self.session_configparser_object.write(sys.stdout)
        else:
            print "\n config from " + self.session_config_file_abspath + " not parsed or defective"


class InitialNimbusCloudConfig(object):
    """
    An instance of this class provides the Nimbus cloud config, as it is
    delivered by the Nimbus cloud config files (user-given).
    """
    def __init__(self, nimbus_config_file_abspath):
        self.logger = logging.getLogger("RM.MainLoop.InitialNimbusCloudConfig")
        self.logger.debug("initialize InitialNimbusCloudConfig object")

        # constructor arguments
        self.logger.debug("check Nimbus cloud config file")
        self.nimbus_config_file_abspath = check_file(nimbus_config_file_abspath)

        # others
        self.nimbus_configparser_object = None

        # init variables that will be read from initial config file
        self.name = None
        self.service_url = None
        self.service_identity = None
        self.request_file_path = None
        self.metadata_file_path = None
        self.certificate_file_path = None
        self.privkey_file_path = None
        self.privkey_pwd_file_path = None
        self.ssh_pubkey_file_path = None
        self.nimbus_cloud_client_root = None
        self.grid_proxy_hours = None
        self.max_instances = None

        self.parse_cloud_config_file()

    def parse_cloud_config_file(self):
        """
        Parse config file, set variables for this nimbus cloud
        """
        self.logger.debug("parse Nimbus cloud config file with SafeConfigParser")
        nimbus_config = ConfigParser.SafeConfigParser()
        nimbus_config.readfp(open(self.nimbus_config_file_abspath))

        self.request_file_path = check_file(os.path.join(
            os.path.dirname(self.nimbus_config_file_abspath),
            nimbus_config.get('nimbuscloudconfig', 'request_file_path')))
        self.metadata_file_path = check_file(os.path.join(
            os.path.dirname(self.nimbus_config_file_abspath),
            nimbus_config.get('nimbuscloudconfig', 'metadata_file_path')))
        self.certificate_file_path = check_file(os.path.join(
            os.path.dirname(self.nimbus_config_file_abspath),
            nimbus_config.get('nimbuscloudconfig', 'certificate_file_path')))
        self.privkey_file_path = check_file(os.path.join(
            os.path.dirname(self.nimbus_config_file_abspath),
            nimbus_config.get('nimbuscloudconfig', 'privkey_file_path')))
        self.privkey_pwd_file_path = check_file(os.path.join(
            os.path.dirname(self.nimbus_config_file_abspath),
            nimbus_config.get('nimbuscloudconfig', 'privkey_pwd_file_path')))
        self.ssh_pubkey_file_path = check_file(os.path.join(
            os.path.dirname(self.nimbus_config_file_abspath),
            nimbus_config.get('nimbuscloudconfig', 'ssh_pubkey_file_path')))
        self.nimbus_cloud_client_root = check_dir(os.path.join(
            os.path.dirname(self.nimbus_config_file_abspath),
            nimbus_config.get('nimbuscloudconfig', 'nimbus_cloud_client_root')))
        self.grid_proxy_hours = nimbus_config.getint('nimbuscloudconfig', 'grid_proxy_hours')
        self.max_instances = nimbus_config.getint('nimbuscloudconfig', 'max_instances')
        self.name = nimbus_config.get('nimbuscloudconfig', 'name')[:8]
        self.service_url = nimbus_config.get('nimbuscloudconfig', 'service_url')
        self.service_identity = nimbus_config.get('nimbuscloudconfig', 'service_identity')

        self.nimbus_configparser_object = nimbus_config

    def print_parsed_cloud_config_file(self):
        if self.nimbus_configparser_object is not None:
            print  "\nconfig read from " + self.nimbus_config_file_abspath + " :"
            self.nimbus_configparser_object.write(sys.stdout)
        else:
            print ("\n config from " + self.nimbus_config_file_abspath
                   + " not parsed or defective")

    def print_cloud_config(self):
        return (
          "\n* name: " + self.name
        + "\n* service_url: " + self.service_url
        + "\n* service_identity: " + self.service_identity
        + "\n* nimbus_config_file_abspath: " + self.nimbus_config_file_abspath
        + "\n* ssh_pubkey_file_path: " + self.ssh_pubkey_file_path
        + "\n* request_file_path: " + self.request_file_path
        + "\n* metadata_file_path: " + self.metadata_file_path
        + "\n* certificate_file_path: " + self.certificate_file_path
        + "\n* privkey_file_path: " + self.privkey_file_path
        + "\n* grid_proxy_hours: " + str(self.grid_proxy_hours)
        + "\n* privkey_pwd_file_path: " + self.privkey_pwd_file_path
        + "\n* nimbus_cloud_client_root: " + self.nimbus_cloud_client_root
        + "\n* max_instances: " + str(self.max_instances)
        + "\n" )


class NimbusCloud(object):
    """
    An instance of this class represents a Nimbus Cloud within the session.
    """
    def __init__(self,  session,
                        nimbus_config_file_abspath,
                        nimbus_cloud_index):
        """
        nimbus_cloud_index: importance/priority of this Nimbus cloud in
                            comparision with other Nimbus clouds. The cloud with
                            index 1 is the first one used.
        """

        self.logger = logging.getLogger("RM.MainLoop.NimbusCloud.%s" % nimbus_cloud_index)
        self.logger.debug("initialize NimbusCloud object")

        # constructor arguments
        self.session = session
        self.nimbus_config_file_abspath = nimbus_config_file_abspath
        self.cloud_index = nimbus_cloud_index

        # The cloud client will run in subfolders of this directory
        self.nb_clcl_main_work_dir = os.path.join(self.session.run_dir,
            "nimbus_cloud_client_runs")
        if not os.path.exists(self.nb_clcl_main_work_dir):
            os.makedirs(self.nb_clcl_main_work_dir)

        # arguments populated during runtime
        self.inicfg = None
        self.grid_proxy_file_path = None
        self.grid_proxy_create_timestamp = None
        self.number_of_running_vms = None

        # this is a list of cloudclient_run_order dicts, containing pairs of
        # 1) NimbusClientWrapper instance 2) VM ID
        # only working Cloud Clients have an entry here. finished->deleted
        self.cloudclient_run_orders = []

    def run_vms(self, number):
        """
        1) At first, grab VM ID's from our instructor, the session instance. It
        will give us some unique (within this session) VM IDs for the new VMs
        to start.
        2) Save "prepared" state to sessions VM save file.
        3) check grid proxy
        4) Per request only *ONE* VM to be able to deliver individual user-data
           containint the VM ID for each VM.
            -->Do for each VM ID (each cloud client run):
                - Generate a cloud client wrapper run ID
                - modify general userdata: append VM ID
                - create cloudclient_run_order dict
                - create NimbusClientWrapper instance
        """
        # grab VM IDs and add data to save.session.vms
        self.logger.debug("request %s VM ID(s)" % number)
        vm_ids=self.session.generate_vm_ids(number)
        self.logger.info("generated VM IDs: %s " % str(vm_ids))
        savedata = [(vm_id+";"+"Nb"+str(self.cloud_index)+";"+"prepared")
                        for vm_id in vm_ids ]
        savedata = '\n'.join(savedata) + '\n'
        self.session.append_save_vms_file(savedata)

        # renew grid proxy if necessary
        if self.expires_grid_proxy():
            self.grid_proxy_init()

        # for each VM ID generate cloud client run ID and run the stuff!
        for vm_id in vm_ids:
            run_id = self.generate_clclwrapper_run_id(vm_id)
            cloudclient_run_order = {}
            cloudclient_run_order['vm_id'] = vm_id
            cloudclient_run_order['clclwrapper'] = NimbusClientWrapper(
                run_id = run_id,
                gridproxyfile=self.grid_proxy_file_path,
                exe=os.path.join(self.inicfg.nimbus_cloud_client_root, "lib/workspace.sh"),
                action="deploy",
                workdir=os.path.join(self.nb_clcl_main_work_dir,run_id),
                userdata=self.session.generate_userdata(vm_id),
                eprfile=os.path.join(self.nb_clcl_main_work_dir,run_id,vm_id+".epr"),
                sshfile=self.inicfg.ssh_pubkey_file_path,
                metadatafile=self.inicfg.metadata_file_path,
                serviceurl=self.inicfg.service_url,
                requestfile=self.inicfg.request_file_path,
                serviceidentity=self.inicfg.service_identity,
                displayname="cloud-%s-%s" % (self.cloud_index,vm_id),
                exitstate="Running",
                polldelay="5000")
            self.cloudclient_run_orders.append(cloudclient_run_order)
            #cloudclient_run_order['clclwrapper'].run()
            
            # nimbus_cloud=self,
            # action="deploy",
            # vm_id="0001",
            # session_run_dir=self.session.run_dir)

        # clclwrapper.set_up_cmdline_params()
        # clclwrapper.set_up_env_vars()
        #clclwrapper.run()

    def generate_clclwrapper_run_id(self, vm_id):
        """
        Generate nimbus cloud client wrapper run ID from VM ID, cloud index,
        current time and random string -> Should be unique  ;-)
        The run ID represents one call of the cloud client. It is e.g. used
        as directory name for workspace.sh logfiles and EPR file(s).
        """
        timestr = time.strftime("%y%m%d%H%M%S",time.localtime())
        run_id = ("%s-nb%s-%s" %(vm_id,self.cloud_index,timestr))
        self.logger.info("generated Nimbus Cloud Client run ID: "+run_id)
        return run_id

    def load_initial_nimbus_cloud_config(self):
        self.logger.info(("load initial config for Nimbus cloud %s from %s"
                           % (self.cloud_index, self.nimbus_config_file_abspath)))
        self.inicfg = InitialNimbusCloudConfig(self.nimbus_config_file_abspath)
        self.logger.debug(("initial config for Nimbus cloud %s: %s"
                            % (self.cloud_index, self.inicfg.print_cloud_config())))

    def check_cloud_client_exists(self):
        """
        Check if Nimbus cloud client exists at given root directory: check
        two important files. check_file() invokes sys.exit() if file isn't found
        """
        self.logger.debug("check if cloud client for Nimbus cloud %s exists under %s"
            % (self.cloud_index, self.inicfg.nimbus_cloud_client_root))
        check_file(os.path.join(self.inicfg.nimbus_cloud_client_root,
                                "lib/workspace.sh"))
        check_file(os.path.join(self.inicfg.nimbus_cloud_client_root,
                                "bin/grid-proxy-init.sh"))

    def expires_grid_proxy(self):
        hour_diff = abs(time.time()-self.grid_proxy_create_timestamp)/3600
        security_delta = 2 # in hours
        agestring = ("grid proxy age: %s h / %s(-%s) h"
            % (round(hour_diff,5),self.inicfg.grid_proxy_hours,security_delta))
        if hour_diff >= self.inicfg.grid_proxy_hours - security_delta:
            self.logger.info("%s -> expires." % agestring)
            return True
        self.logger.info("%s -> still valid." % agestring)
        return False

    def grid_proxy_init(self):
        # define proxy file and grid-proxy-init logfile
        timestring = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        gpi_run_dir = os.path.join(self.session.run_dir, "grid-proxy-init")
        if not os.path.exists(gpi_run_dir):
            os.mkdir(gpi_run_dir)
        gp_file_name = "cloud_%s_%s.proxy" % (self.cloud_index, timestring)
        grid_proxy_file_path = os.path.join(gpi_run_dir,gp_file_name)
        # define log file for grid-proxy-init program
        gpi_stdouterr_file_path = os.path.join(gpi_run_dir,gp_file_name+".log")
        gpi_stdouterr_file = open(gpi_stdouterr_file_path,'w')

        # read private key password
        gp_priv_key_pwd = open(self.inicfg.privkey_pwd_file_path).read()

        # build grid-proxy-init command (without password)
        gpi_executable = os.path.join(self.inicfg.nimbus_cloud_client_root,
            "bin/grid-proxy-init.sh")
        gpi_shellcommand = (
            "/bin/sh %s -debug -pwstdin -cert %s -key %s -out %s -hours %s"
                % (gpi_executable,
                self.inicfg.certificate_file_path,
                self.inicfg.privkey_file_path,
                grid_proxy_file_path,
                self.inicfg.grid_proxy_hours))
        self.logger.debug("invoke 'echo PWD | %s as subprocess" % gpi_shellcommand)

        # add password (stdin via echo) and execute command as subprocess
        gpi_shellcommand_pw = "echo %s | %s" % (gp_priv_key_pwd, gpi_shellcommand)
        gpi_sp = subprocess.Popen(
            args=gpi_shellcommand_pw,
            stdout=gpi_stdouterr_file,
            stderr=subprocess.STDOUT,
            cwd=gpi_run_dir,
            shell=True)
        self.logger.debug("wait for subprocess to return...")
        returncode = gpi_sp.wait() # quite fast (1-5 seconds)
        self.logger.debug("grid-proxy-init returned with code %s" % returncode)

        # check output
        if returncode is not 0:
            self.logger.critical(("grid-proxy-init returncode was "
                                  "not 0. Check %s" % gpi_stdouterr_file_path))
        elif os.path.exists(grid_proxy_file_path):
            self.logger.info("valid grid proxy written to %s" % grid_proxy_file_path)
            # the penultimate line in log file contains "valid until..." info
            infomessage = open(gpi_stdouterr_file_path).readlines()[-1]
            self.logger.info(infomessage)
            self.grid_proxy_file_path = grid_proxy_file_path
            self.grid_proxy_create_timestamp = time.time()
        else:
            self.logger.critical("grid proxy file was not created.")


class ResourceManagerLogger:
    """
    Configuration class for logging with the logging module.
    """
    def __init__(self, pipe_write, logdir):
        # create logdir
        if not os.path.exists(logdir):
            os.mkdir(logdir)

        # create log filenames (with prefix from time)
        log_filename_prefix = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        rm_log_file_path = os.path.join(logdir,log_filename_prefix + "_RM.log")
        boto_log_file_path = os.path.join(logdir,log_filename_prefix + "_boto.log")

        # set up main/root logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        # file handler for a real file with level DEBUG
        self.fh = logging.FileHandler(rm_log_file_path, encoding="UTF-8")
        self.fh.setLevel(logging.DEBUG)
        self.formatter_file = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s")
        self.fh.setFormatter(self.formatter_file)

        # # "console" handler (to stderr by default) with level ERROR
        # self.ch = logging.StreamHandler()
        # self.ch.setLevel(logging.ERROR)
        # self.formatter_console = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s")
        # self.ch.setFormatter(self.formatter_console)

        # pipe handler to GUI
        # about encoding:
        # http://mail.python.org/pipermail/python-bugs-list/2004-March/022258.html
        # -> "Notice that UTF-8 is only used if a UnicodeError is detected.
        # By default, "%s\n" % msg is written to the stream using the
        # stream's write(). If the stream can handle this without raising
        # a UnicodeError, then UTF-8 will not be used."
        self.ph = logging.StreamHandler(os.fdopen(pipe_write,'a',0))
        self.ph.setLevel(logging.INFO)
        self.formatter_pipe = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
            "%H:%M:%S")
        self.ph.setFormatter(self.formatter_pipe)

        # add handler
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ph)
        # self.logger.addHandler(self.ch)

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


# create module logger
logger = logging.getLogger("rm_session.py")


class Object:
    """
    This class is just for convenient config hierarchy in other classes
    """
    pass


class Tee(object):
    """
    Provides a write()-method that writes to two filedescriptors.

    One should be standard-stdout/err and the other should describe a real file.
    If sys.stdout is replaced with an instance of this `Tee`-class and sys.stderr is
    set to sys.stdout, all stdout+stderr of the script is collected to console and
    to file at the same time.
    """
    def __init__(self, stdouterr, file, pipe_write=None):
        self.stdouterr = stdouterr
        self.file = file
        self.pipe_write = pipe_write

    def write(self, data):
        self.stdouterr.write(data)
        try:
          self.file.write(data)
          self.file.flush()
        except:
          pass

        if self.pipe_write is not None:
            os.write(self.pipe_write, data)

    def flush(self):
        self.stdouterr.flush()
        self.file.flush()


def backup_file(file_path, backup_dir_path):
    """
    Backup any file to any dir. Therefore, append the original filename with
    a timestring (if one file is backupped multiple times within one second,
    it's not worth to keep all backups; hence, 1 s resolution is okay here.)
    """
    logger.debug("backup %s to %s ..." %(file_path, backup_dir_path))
    if os.path.exists(file_path) and os.path.exists(backup_dir_path):
        if os.path.isfile(file_path) and os.path.isdir(backup_dir_path):
            timestring = time.strftime("%Y%m%d-%H%M%S", time.localtime())
            filename = os.path.basename(file_path)
            bckp_filename = "%s_%s" % (filename, timestring)
            bckp_file_path = os.path.join(backup_dir_path, bckp_filename)
            shutil.copy(file_path, bckp_file_path)
        else:
            logger.debug(("%s not file and/or %s not dir" %
                                (file_path, backup_dir_path)))
    else:
        logger.debug(("%s and/or %s does not exist" %
                            (file_path, backup_dir_path)))


def check_file(file):
    """
    Check if a given file exists and really is a file (e.g. not a directory)
    In errorcase the script is stopped.

    @return: the absolute path of the file
    """
    logger.debug("check_file("+file+")")
    if not os.path.exists(file):
        logger.critical(file+' does not exist. Exit.')
        sys.exit(file+' does not exist')
    if not os.path.isfile(file):
        logger.critical(file+' is not a file. Exit.')
        sys.exit(file+' is not a file.')
    return os.path.abspath(file)


def check_dir(dir):
    """
    Check if a given dir exists and really is a dir (e.g. not a file)
    In errorcase the script is stopped.

    @return: the absolute path of the directory
    """
    logger.debug("check_dir("+dir+")")
    if not os.path.exists(dir):
        logger.critical(dir+' does not exist. Exit.')
        sys.exit(dir+' does not exist')
    if not os.path.isdir(dir):
        logger.critical(dir+' is not a file. Exit.')
        sys.exit(dir+' is not a directory')
    return os.path.abspath(dir)
