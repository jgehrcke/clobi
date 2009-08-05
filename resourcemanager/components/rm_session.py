# -*- coding: UTF-8 -*-
#
#   ::::::::> Clobi RESOURCE MANAGER <::::::::
#   Resource Manager Session module
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

import ConfigParser
import os
import sys
import logging
import hashlib
import random
import time
import subprocess
import shutil
import base64
import fileinput

from components.rm_nimbus_clntwrppr import NimbusClientWrapper
from components.cfg_parse_strzip import SafeConfigParserStringZip
from components.utils import (check_dir, check_file, timestring, backup_file,
    Tee, Object)

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
        self.save_config_file_path = os.path.join(self.save_dir,
            "save.session.sqs.config")

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
                self.logger.info(("continue with queue %s for priority %s"
                    % (queue_name,prio)))
            else:
                self.logger.info(("create queue %s for priority %s"
                    % (queue_name,prio)))
            found = False
            for q in existing_queues:
                if q.url.endswith(queue_name):
                    self.logger.info(("SQS queue %s for priority %s is already"
                                      " existing -> explore queue attributes..."
                                      % (q.url,prio)))
                    attr = q.get_attributes()
                    attrstr = '\n'.join([("         %s: %s" % (k,v))
                        for k,v in attr.items()])
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
                q = self.sqsconn.create_queue(
                    queue_name,
                    int(self.default_visibility_timeout))
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
        self.logger.info(("loaded session SQS config from %s ."
            % self.save_config_file_path))

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
        self.logger.info(("session SQS config written to %s ."
            % self.save_config_file_path))


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
        self.save_config_file_path = os.path.join(self.save_dir,
            "save.session.sdb.config")

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
        self.boto_domainobj_session = self.create_domain(
            self.domain_name_session,
            resume=True)
        self.logger.info("continue with SDB domain for job data...")
        self.boto_domainobj_jobs = self.create_domain(
            self.domain_name_jobs,
            resume=True)

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
            self.logger.info(("SDB domain %s seems not to exist -> create..."
                % domainname))
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

    def register_vm_started(self, vm_id):
        item = self.boto_domainobj_session.new_item(vm_id)
        timestr = timestring()
        self.logger.info(("creating SDB item %s with 'status=RM_started' and"
            " 'RM_startuptime=%s'" % (vm_id, timestr)))
        item['status'] = 'RM_started'
        item['RM_startuptime'] = timestr
        item.save()

    def get_highest_priority(self):
        """
        Get HighestPriority flag from SDB
        """
        item = self.boto_domainobj_session.get_item('session_props')
        if item is not None:
            self.highest_priority = item['HighestPriority']
            self.logger.info(("got HighestPriority from SDB: %s"
                % self.highest_priority))
        else:
            self.logger.error("HighestPriority item does not exist")

    def create_session_props_item(self):
        """
        Set "Session Properties Item" in the SDB domain for for session data
        (session & VM data). The item contains e.g. the HighestPriority flag.
        """
        item = self.boto_domainobj_session.new_item('session_props')
        self.logger.info(("setting HighestPriority to %s"
            % self.initial_highest_priority))
        item['HighestPriority'] = self.initial_highest_priority
        item.save()

    def load_sdb_session_config_from_file(self):
        """
        Save current SDB session information from a file using ConfigParser.
        It actually could be generated from the session ID. But this method is
        here for completeness and potential future use.
        """
        self.logger.info(("load session SDB config from %s ..."
            % self.save_config_file_path))
        config = ConfigParser.SafeConfigParser()
        config.readfp(open(check_file(self.save_config_file_path)))
        self.domain_name_session = config.get(
            'SDB_sessionconfig',
            'domain_name_session')
        self.domain_name_jobs = config.get(
            'SDB_sessionconfig',
            'domain_name_jobs')

    def save_sdb_session_config_to_file(self):
        """
        Save current SDB session information to a file using ConfigParser.
        Currently, there is not much config to write. It actually could be
        generated from the session ID. But this method is here for completeness
        and potential future use.
        """
        config = ConfigParser.SafeConfigParser()
        config.add_section('SDB_sessionconfig')
        config.set('SDB_sessionconfig', 'domain_name_session',
            self.domain_name_session)
        config.set('SDB_sessionconfig', 'domain_name_jobs',
            self.domain_name_jobs)
        fd = open(self.save_config_file_path,'w')
        config.write(fd)
        fd.close()
        self.logger.info(("session SDB config written to %s ."
            % self.save_config_file_path))


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
        self.save_config_file_path = os.path.join(
            self.save_dir,
            "save.session.config")
        self.save_vms_file_path = os.path.join(self.save_dir,"save.session.vms")
        self.save_last_vm_id_file_path = os.path.join(
            self.save_dir,
            "save.session.last_vm_id")
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

    def nimbus_cloud_indices(self):
        """
        Return list of Cloud Indices of all Nimubs clouds within the session.
        """
        return [nbcld.cloud_index for nbcld in self.nimbus_clouds]

    def generate_userdata(self, vm_id):
        """
        Generate userdata string for VM ID. This is the string delivered with
        the request. It must be small (evtl. zipped) and encoded for reliable
        transmission within Query URL (EC2) or SOAP message (Nimbus) -> Base64.
        """
        config = SafeConfigParserStringZip()
        config.add_section('userdata')
        config.set('userdata','sessionid',self.session_id)
        config.set('userdata','vmid',vm_id)
        config.set('userdata','accesskey',self.inicfg.aws.accesskey)
        config.set('userdata','secretkey',self.inicfg.aws.secretkey)
        cfg = config.write_to_string()
        zipcfg = config.write_to_zipped_string()
        b64zipcfg = base64.b64encode(zipcfg)
        #self.logger.info(("generated userdata: \n%s\n(length: %s)"
        #    % (cfg,len(cfg))))
        #self.logger.debug(("zip(generated userdata): %s\n(length: %s)"
        #    % (repr(zipcfg),len(zipcfg))))
        #self.logger.debug(("base64(zip(generated userdata)): %s\n(length: %s)"
        #    % (b64zipcfg,len(b64zipcfg))))
        self.logger.debug("generated userdata for %s." % vm_id)
        return b64zipcfg

    def nbx(self, string):
        """
        Analyse string if it has the form 'nbX'.
        Return valid Nimbus Cloud Index X or False. Therefore, it needs a list
        `nbcldidcs` of valid Nimbus Cloud Indices.
        """
        parts = string.lower().split('nb')
        if len(parts) == 2:
            if parts[1].isdigit():
                if int(parts[1]) in self.nimbus_cloud_indices():
                    return int(parts[1])
        return False

    def get_vm_info_from_file(self,vm_id):
        """
        Parse save.session.vms file and look for vm id.
        If found, return a normed data dictionary.
        If not found, return False.
        """
        fd = open(self.save_vms_file_path)
        for line in fd:
            if line.startswith(vm_id):
                # this is the entire dict to be populated with data
                vm_info = {}
                vm_info['vm_id'] = None
                vm_info['Nimbus'] = None
                vm_info['cloudindex'] = None
                vm_info['EC2'] = None
                vm_info['status'] = None
                vm_info['eprfile'] = None
                vm_info['instanceid'] = None
                vm_info['reservationid'] = None

                # assume the following data format:
                # nimbus: vmid;nbX;status[;eprfile]
                # ec2:    vmid;ec2;status[;reservationid;instanceid]
                data = line.rstrip().split(";")
                if len(data) >= 3:
                    vm_id = data[0]
                    cloudname = data[1]
                    status = data[2]

                    vm_info['vm_id'] = vm_id
                    vm_info['status'] = status
                    if self.nbx(cloudname):
                        vm_info['Nimbus'] = True
                        vm_info['cloudindex'] = self.nbx(cloudname)
                        vm_info['EC2'] = False
                        if len(data) == 4:
                            vm_info['eprfile'] = data[3]
                    elif cloudname.lower() == "ec2":
                        vm_info['Nimbus'] = False
                        vm_info['cloudindex'] = False
                        vm_info['EC2'] = True
                        if len(data) == 5:
                            vm_info['instanceid'] = data[4]
                            vm_info['reservationid'] = data[3]
                    else:
                        self.logger.error("no valid nimbus cloud name found")
                        return False
                    return vm_info
                else:
                    self.logger.error(("get_vm_info_from_file(): There should "
                        " be at least 3 data fields. Found %s" % len(data)))
                    return False
        self.logger.error(("get_vm_info_from_file():couldn't find VM ID %s in"
            " file %s" % (vm_id, self.save_vms_file_path)))
        return False

    def append_save_vms_file(self, savestring):
        """
        Append data to VMs save file. like: "vm_id;cloud;prepared".
        Backup before.
        """
        backup_file(self.save_vms_file_path, self.save_backup_dir, 50)
        fd = open(self.save_vms_file_path,'a')
        fd.write(savestring)
        fd.close()
        self.logger.debug(("appended to file %s : %s" %
                           (self.save_vms_file_path, savestring)))

    def update_save_vms_file_entry(self, vm_id, new_state, append = None):
        """
        Update the line in the file that corresponds to vm_id: Record a new
        state. Backup before!.
        Assume data format: 'vm_id;cloudname;state;other;stuff'
        Search line by vm_id. Modify field 3 by `new_state`. If `append`
        data is available and a list, these items are appended to the line.
        """
        backup_file(self.save_vms_file_path, self.save_backup_dir, 50)
        self.logger.info(("update_save_vms_file_entry(): "
                          "VM ID: %s; new state: %s" % (vm_id, new_state)))
        found = False
        for line in fileinput.input(self.save_vms_file_path, inplace=1):
            if line.startswith(vm_id):
                # found the line! erase trailing whitespace (incl \n) and split!
                found = True
                words = line.rstrip().split(";")
                if len(words) < 3:
                    self.logger.error(("Line %d ('%s') in %s has less than three"
                        " data fields" % (fileinput.filelineno(), line,
                        self.save_vms_file_path)))
                else:
                    words[2] = new_state
                    if append:
                        if isinstance(append,list):
                            words.extend(append)
                        else:
                            self.logger.error(("append (%s) is no list"
                                %repr(append)))
                    # the line is modified. write it to file (incl. newline!)
                    print ';'.join(words)
            else:
                # write this line to file as is (w/o additional \n from print)
                print line,
        fileinput.close()
        if not found:
            self.logger.error(("No line in %s started with %s" %
                (self.save_vms_file_path, vm_id)))

    def generate_vm_ids(self, number):
        """
        Create and return a bunch of new VM IDs in form of a list.
        To securely keep track of generated IDs, always save the last generated
        ID number to file. Load it from there on each call, too.
        """
        self.logger.debug(("read last VM ID number from file "+
                            self.save_last_vm_id_file_path))
        backup_file(self.save_last_vm_id_file_path, self.save_backup_dir, 50)
        try:
            last_vm_id_number = int(open(self.save_last_vm_id_file_path,'r').read())
            self.logger.debug("read last VM ID: %s" % last_vm_id_number)
        except IOError:
            if self.resume:
                self.logger.critical(("session in resume mode and IOError on last"
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
        self.logger.info("order to run %s VMs on EC2" % nbr)
        self.ec2.run_vms(number=nbr)

    def run_vms_nimbus(self, cloud_index, nbr):
        for nbcloud in self.nimbus_clouds:
            if nbcloud.cloud_index == cloud_index:
                self.logger.info(("order to run %s VMs on Nimbus Cloud %s"
                    % (nbr,cloud_index)))
                nbcloud.run_vms(number=nbr)
                return

    def nimbus_query_factory_rp(self, cloud_index):
        for nbcloud in self.nimbus_clouds:
            if nbcloud.cloud_index == cloud_index:
                self.logger.info(("order to query Factory RP on Nimbus Cloud %s"
                    % cloud_index))
                nbcloud.query_factory_rp()
                return

    def nimbus_query_workspace(self, cloud_index, eprfile, vm_id=None):
        for nbcloud in self.nimbus_clouds:
            if nbcloud.cloud_index == cloud_index:
                self.logger.info(("order to query Workspace with EPR file %s"
                    " on Nimbus Cloud %s" % (eprfile, cloud_index)))
                nbcloud.query_workspace(eprfile_path=eprfile,vm_id=vm_id)
                return

    def nimbus_destroy_workspace(self, cloud_index, eprfile, vm_id=None):
        for nbcloud in self.nimbus_clouds:
            if nbcloud.cloud_index == cloud_index:
                self.logger.info(("order to destroy Workspace with EPR file %s"
                    " on Nimbus Cloud %s" % (eprfile, cloud_index)))
                nbcloud.destroy_workspace(eprfile_path=eprfile,vm_id=vm_id)
                return

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
        self.inicfg = isc
        if isc.ec2.use:
            self.logger.info("EC2 is used within this session.")
            self.ec2 = EC2(session=self)
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
            #self.set_up_sqs_from_scratch()
            #self.logger.info("save all generated config to files...")
            #self.save_extended_config_to_file()
            #self.sdb_session.save_sdb_session_config_to_file()
            #self.sqs_session.save_sqs_session_config_to_file()
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
        self.logger.info(("extended session config written to %s ."
            % self.save_config_file_path))

    def load_extended_config_from_file(self):
        """
        Load session information that is not provided by initial session config
        """
        self.logger.info(("load extended session config from %s..."
            % self.save_config_file_path))
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
        self.ec2.instance_state_pollinterval = None
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
        + "\n* ec2.instance_state_pollinterval: " + unicode(self.ec2.instance_state_pollinterval)
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

        self.sdb.monitor_vms_pollinterval = session_config.getfloat(
            'SimpleDB',
            'monitor_VMs_pollinterval')
        self.sqs.monitor_queues_pollinterval = session_config.getfloat(
            'SQS',
            'monitor_queues_pollinterval')
        self.sqs.initial_highest_priority = session_config.getint(
            'SQS',
            'initial_highest_priority')
        self.sqs.default_visibility_timeout = session_config.getint(
            'SQS',
            'default_visibility_timeout')
        self.ec2.use = False
        if session_config.has_section('EC2'):
            self.ec2.instancetype = session_config.get(
                'EC2',
                'instancetype').decode('UTF-8')
            self.ec2.ami_id = session_config.get(
                'EC2',
                'ami_id').decode('UTF-8')
            self.ec2.max_instances = session_config.getint(
                'EC2',
                'max_instances')
            self.ec2.instance_state_pollinterval = session_config.getint(
                'EC2',
                'instance_state_pollinterval')
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
                    self.nimbus.sorted_configfile_dict[cloud_index] = (
                        nimbus_config_file_abspath)
                    self.logger.debug(("config file for Nimbus cloud with index"
                                       " %s: %s") % (cloud_index,
                                       nimbus_config_file_abspath))
                    cloud_index += 1
        self.session_configparser_object = session_config

    def print_parsed_session_config_file(self):
        if self.session_configparser_object is not None:
            print  "\nconfig read from " +self.session_config_file_abspath+ " :"
            self.session_configparser_object.write(sys.stdout)
        else:
            print ("\n config from %s not parsed or defective"
                % self.session_config_file_abspath)


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
        self.grid_proxy_hours = nimbus_config.getint(
            'nimbuscloudconfig',
            'grid_proxy_hours')
        self.max_instances = nimbus_config.getint(
            'nimbuscloudconfig',
            'max_instances')
        self.name = nimbus_config.get('nimbuscloudconfig', 'name')[:8]
        self.service_url = nimbus_config.get('nimbuscloudconfig', 'service_url')
        self.service_identity = nimbus_config.get(
            'nimbuscloudconfig',
            'service_identity')

        self.nimbus_configparser_object = nimbus_config

    def print_parsed_cloud_config_file(self):
        if self.nimbus_configparser_object is not None:
            print  "\nconfig read from " +self.nimbus_config_file_abspath+ " :"
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
        self.logger = logging.getLogger(("RM.MainLoop.NimbusCloud.%s"
            % nimbus_cloud_index))
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
                - run subprocess
                - update state with 'run_ordered' and EPR file
        """
        # grab VM IDs and add data to save.session.vms
        self.logger.debug("request %s VM ID(s)" % number)
        vm_ids=self.session.generate_vm_ids(number)
        self.logger.info("generated VM ID(s): %s " % ', '.join(vm_ids))
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
            cloudclient_run_order['action'] = "deploy"
            cloudclient_run_order['run_id'] = run_id
            cloudclient_run_order['vm_id'] = vm_id
            eprfile=os.path.join(self.nb_clcl_main_work_dir,run_id,vm_id+".epr")
            cloudclient_run_order['clclwrapper'] = NimbusClientWrapper(
                run_id = run_id,
                gridproxyfile=self.grid_proxy_file_path,
                exe=os.path.join(
                    self.inicfg.nimbus_cloud_client_root,
                    "lib/workspace.sh"),
                action="deploy",
                workdir=os.path.join(self.nb_clcl_main_work_dir,run_id),
                userdata=self.session.generate_userdata(vm_id),
                eprfile=eprfile,
                sshfile=self.inicfg.ssh_pubkey_file_path,
                metadatafile=self.inicfg.metadata_file_path,
                serviceurl=self.inicfg.service_url,
                requestfile=self.inicfg.request_file_path,
                serviceidentity=self.inicfg.service_identity,
                displayname="cloud-%s-%s" % (self.cloud_index,vm_id),
                exitstate="Running",
                polldelay="5000")
            if cloudclient_run_order['clclwrapper'].run():
                self.cloudclient_run_orders.append(cloudclient_run_order)
                self.session.update_save_vms_file_entry(
                    vm_id=vm_id,
                    new_state="run_ordered",
                    append=[eprfile])
            else:
                self.logger.error("subprocess did not start")

    def destroy_workspace(self, eprfile_path, vm_id=None):
        """
        Invoke Workspace RP Query, only using a given EPR file. "vm-xxxx"
        context must not be given here.
        """
        # renew grid proxy if necessary
        if self.expires_grid_proxy():
            self.grid_proxy_init()

        # generate cloudclient_run_order
        cloudclient_run_order = {}
        cloudclient_run_order['action'] = "destroy"
        if vm_id:
            cloudclient_run_order['vm_id'] = vm_id

        timestr = time.strftime("%y%m%d%H%M%S",time.localtime())
        run_id = ("workspace_destroy-nb%s-%s" %(self.cloud_index,timestr))
        cloudclient_run_order['run_id'] = run_id
        workdir = os.path.join(self.nb_clcl_main_work_dir,run_id)
        cloudclient_run_order['workdir'] = workdir

        cloudclient_run_order['clclwrapper'] = NimbusClientWrapper(
                        exe=os.path.join(
                            self.inicfg.nimbus_cloud_client_root,
                            "lib/workspace.sh"),
                        workdir=workdir,
                        gridproxyfile=self.grid_proxy_file_path,
                        action="destroy",
                        run_id=run_id,
                        eprfile=eprfile_path)

        if cloudclient_run_order['clclwrapper'].run():
            self.cloudclient_run_orders.append(cloudclient_run_order)
            self.logger.info("subprocess successfully started.")
        else:
            self.logger.error("subprocess did not start")

    def query_workspace(self, eprfile_path, vm_id=None):
        """
        Invoke Workspace RP Query, only using a given EPR file. No "vm-xxxx"
        context given here.
        """
        # renew grid proxy if necessary
        if self.expires_grid_proxy():
            self.grid_proxy_init()

        # generate cloudclient_run_order
        cloudclient_run_order = {}
        cloudclient_run_order['action'] = "rpquery"
        if vm_id:
            cloudclient_run_order['vm_id'] = vm_id

        timestr = time.strftime("%y%m%d%H%M%S",time.localtime())
        run_id = ("workspace_rp_query-nb%s-%s" %(self.cloud_index,timestr))
        cloudclient_run_order['run_id'] = run_id
        workdir = os.path.join(self.nb_clcl_main_work_dir,run_id)
        cloudclient_run_order['workdir'] = workdir

        cloudclient_run_order['clclwrapper'] = NimbusClientWrapper(
                        exe=os.path.join(
                            self.inicfg.nimbus_cloud_client_root,
                            "lib/workspace.sh"),
                        workdir=workdir,
                        gridproxyfile=self.grid_proxy_file_path,
                        action="rpquery",
                        run_id=run_id,
                        eprfile=eprfile_path)

        if cloudclient_run_order['clclwrapper'].run():
            self.cloudclient_run_orders.append(cloudclient_run_order)
            self.logger.info("subprocess successfully started.")
        else:
            self.logger.error("subprocess did not start")

    def query_factory_rp(self):
        """
        Invoke "Factory RP Query" on Nimbus cloud via Nimbus Cloud Client
        wrapper.
        """
        # renew grid proxy if necessary
        if self.expires_grid_proxy():
            self.grid_proxy_init()

        # generate cloudclient_run_order
        cloudclient_run_order = {}
        cloudclient_run_order['action'] = "factoryrp"

        timestr = time.strftime("%y%m%d%H%M%S",time.localtime())
        run_id = ("factory_rp_query-nb%s-%s" %(self.cloud_index,timestr))
        cloudclient_run_order['run_id'] = run_id
        workdir = os.path.join(self.nb_clcl_main_work_dir,run_id)
        cloudclient_run_order['workdir'] = workdir

        cloudclient_run_order['clclwrapper'] = NimbusClientWrapper(
            run_id = run_id,
            gridproxyfile=self.grid_proxy_file_path,
            exe=os.path.join(
                self.inicfg.nimbus_cloud_client_root,
                "lib/workspace.sh"),
            action="factoryrp",
            workdir=workdir,
            serviceurl=self.inicfg.service_url)

        if cloudclient_run_order['clclwrapper'].run():
            self.cloudclient_run_orders.append(cloudclient_run_order)
            self.logger.info("subprocess successfully started.")
        else:
            self.logger.error("subprocess did not start")

    def check_nimbus_cloud_client_wrappers(self):
        """
        Check state of Cloud Client runs. Update save.session.vms file with
        new state information.
        e.g. on deply success: new state 'started',
        on deploy error (returncode != 0): new state 'deploy_error'
        Delete cloutclient_run_order list item after analysis (only if success
        or error; do nothing of subprocess is not returned).
        """
        def get_logfilepath_and_log(clclrunorder):
            path = clclrunorder['clclwrapper'].stdouterr_file_path
            if os.path.exists(path):
                log = open(path).read()
            else:
                log = "error: logfile does not exist"
            return (path, log)

        delete_indices = []
        vm_new_state = None
        for idx, clclrunorder in enumerate(self.cloudclient_run_orders):
            success = clclrunorder['clclwrapper'].was_successfull()
            if success is not None:                     # subprocess ended
                if not success:                         # returncode != 0
                    self.logger.error(("Returncode of Cloud Client run order %s"
                        " wasn't 0." % clclrunorder['run_id']))
                    if clclrunorder['action'] == "deploy":
                        vm_new_state = 'deploy_error'
                    elif clclrunorder['action'] == "destroy":
                        vm_new_state = 'destroy_error'
                        self.logger.error("destroy log file %s: %s"
                            % get_logfilepath_and_log(clclrunorder))
                    elif clclrunorder['action'] == "rpquery":
                        vm_new_state = 'rpquery_error'
                        self.logger.error("rpquery log file %s: %s"
                            % get_logfilepath_and_log(clclrunorder))
                else:
                    if clclrunorder['action'] == "deploy":
                        vm_new_state = 'started'
                        self.session.sdb_session.register_vm_started(
                            clclrunorder['vm_id'])
                    elif clclrunorder['action'] == "destroy":
                        vm_new_state = 'manually_destroyed'
                        self.logger.info(("Workspace destroy subprocess"
                            " successfully ended. Content of destroy log file"
                            "  %s: %s"
                            % get_logfilepath_and_log(clclrunorder)))
                    elif clclrunorder['action'] == "rpquery":
                        self.logger.info(("Workspace RP Query subprocess"
                            " successfully ended. Content of rpquery log file"
                            "  %s: %s"
                            % get_logfilepath_and_log(clclrunorder)))
                    elif clclrunorder['action'] == "factoryrp":
                        self.logger.info(("Factory RP Query subprocess"
                            " successfully ended. Content of factoryrp log file"
                            "  %s: %s"
                            % get_logfilepath_and_log(clclrunorder)))

                # when there was an action that changed the state of *any*
                # workspace *AND* this workspace is attributive to a VM ID,
                # THEN change state of the VM in save.session.vms (e.g. when
                # destroy wasn't only called with an EPR file, but also with a
                # VM ID).
                if vm_new_state and clclrunorder['vm_id']:
                    self.session.update_save_vms_file_entry(
                        vm_id=clclrunorder['vm_id'],
                        new_state=vm_new_state)

                # this subprocess returned and was analyzed. We don't need the
                # cloud client wrapper instance anymore: mark it to delete
                delete_indices.append(idx)

        # iterate in reversed order (otherwise IndexErrors) to delete objects
        for idx in reversed(delete_indices):
            self.logger.debug(("delete cloudclient_run_order object with run id %s"
                % self.cloudclient_run_orders[idx]['run_id']))
            del self.cloudclient_run_orders[idx]

    def generate_clclwrapper_run_id(self, vm_id):
        """
        Generate nimbus cloud client wrapper run ID from VM ID, cloud index,
        current time and random string -> Should be unique  ;-)
        The run ID represents one call of the cloud client. It is e.g. used
        as directory name for workspace.sh logfiles and EPR file(s).
        (Note: only used for deploy; the actions rp query and destroy work in
        already existing directories, factory rp query gets its own dir)
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
        check_file(os.path.join(
            self.inicfg.nimbus_cloud_client_root,
            "lib/workspace.sh"))
        check_file(os.path.join(
            self.inicfg.nimbus_cloud_client_root,
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
        timestr = timestring()
        gpi_run_dir = os.path.join(self.session.run_dir, "grid-proxy-init")
        if not os.path.exists(gpi_run_dir):
            os.mkdir(gpi_run_dir)
        gp_file_name = "cloud_%s_%s.proxy" % (self.cloud_index, timestr)
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
            self.logger.info(("valid grid proxy written to %s"
                % grid_proxy_file_path))
            # the penultimate line in log file contains "valid until..." info
            infomessage = open(gpi_stdouterr_file_path).readlines()[-1]
            self.logger.info(infomessage)
            self.grid_proxy_file_path = grid_proxy_file_path
            self.grid_proxy_create_timestamp = time.time()
        else:
            self.logger.critical("grid proxy file was not created.")


class EC2(object):
    """
    Represents EC2 within a Resource Manager session.
    """
    def __init__(self, session):
        self.logger = logging.getLogger("RM.MainLoop.EC2")
        self.logger.debug("initialize EC2 object")

        self.session = session
        self.instancetype = self.session.inicfg.ec2.instancetype
        self.ami_id = self.session.inicfg.ec2.ami_id
        self.ec2conn = boto.connect_ec2(self.session.inicfg.aws.accesskey,
                                        self.session.inicfg.aws.secretkey)

        # this is a list containing run_order dicts
        self.ec2_run_orders = []

    def run_vms(self, number):
        """
        1) At first, grab VM ID's from our instructor, the session instance. It
        will give us some unique (within this session) VM IDs for the new VMs
        to start.
        2) Save "prepared" state to sessions VM save file.
        4) Per request only *ONE* VM to be able to deliver individual user-data
           containing the VM ID for each VM.
            -->Do for each VM ID:
                - modify general userdata: append VM ID
                - create ec2_run_order dict
                - send request, save reservation object in ec2_run_order dict
                - update  sessions VM save file with "run_ordered" and res ID
        """

        # grab VM IDs and add data to save.session.vms
        self.logger.debug("request %s VM ID(s)" % number)
        vm_ids=self.session.generate_vm_ids(number)
        self.logger.info("generated VM ID(s): %s " % ', '.join(vm_ids))
        savedata = [vm_id+";EC2;prepared" for vm_id in vm_ids]
        savedata = '\n'.join(savedata) + '\n'
        self.session.append_save_vms_file(savedata)

        # for each VM ID generate cloud client run ID and run the stuff!
        for vm_id in vm_ids:
            ec2_run_order = {}
            ec2_run_order['vm_id'] = vm_id
            request_success = True
            try:
                self.logger.info(("send EC2 request: run 1 instance of type %s"
                    " of image %s with specific user-data"
                    % (self.instancetype,self.ami_id)))
                ec2_run_order['boto_rsrvtn_obj'] = self.ec2conn.run_instances(
                    min_count=1,
                    max_count=1,
                    user_data=self.session.generate_userdata(vm_id),
                    image_id=self.ami_id,
                    instance_type=self.instancetype)
            except:
                import traceback
                traceback.print_exc()
                request_success = False

            if request_success:
                self.ec2_run_orders.append(ec2_run_order)
                self.session.update_save_vms_file_entry(
                    vm_id=vm_id,
                    new_state="run_ordered",
                    append=[ec2_run_order['boto_rsrvtn_obj'].id])
                    #append=["some_reservation_id"])
            else:
                self.logger.error("an error appeared while sending the request")

    def check_runinstances_request_states(self):
        """
        Check state of instances that have just beeen started up.
        There are 4 states (http://docs.amazonwebservices.com/AWSEC2/2009-04-04/
        APIReference/ApiReference-ItemType-InstanceStateType.html)
        'pending' -> do nothing
        'running' -> the machine is starting up -> new state 'started'
        'terminated' and 'shutting-down' -> new state 'error'
        Delete ec2_run_order list item after success/error detection.
        """
        delete_indices = []
        for idx, ec2_run_order in enumerate(self.ec2_run_orders):
            instanceobj = ec2_run_order['boto_rsrvtn_obj'].instances[0]
            instanceid = instanceobj.id
            try:
                self.logger.debug(("query EC2 for state of %s ..." %instanceid))
                inststate = instanceobj.update()
                self.logger.info(("instance %s has state: %s"
                    % (instanceid, inststate)))
            except:
                inststate = None
                import traceback
                traceback.print_exc()
            if inststate:
                if inststate != 'pending':
                    if inststate == 'running':
                        new_state = 'started'
                        self.session.sdb_session.register_vm_started(
                            clclrunorder['vm_id'])
                    if inststate == 'terminated' or inststate == 'shutting-down':
                        new_state = 'error'
                    self.session.update_save_vms_file_entry(
                        vm_id=ec2_run_order['vm_id'],
                        new_state=new_state,
                        append=[instanceid])
                    delete_indices.append(idx)
        # iterate in reversed order (otherwise IndexErrors) to delete objects
        for idx in reversed(delete_indices):
            self.logger.debug(("delete ec2_run_order obj with reservation id %s"
                % self.ec2_run_orders[idx]['boto_rsrvtn_obj'].id))
            del self.ec2_run_orders[idx]


class ResourceManagerLogger(object):
    """
    Configuration class for logging with the logging module.
    """
    def __init__(self, pipe_write, logdir):
        # create logdir
        if not os.path.exists(logdir):
            os.mkdir(logdir)

        # create log filenames (with prefix from time)
        log_filename_prefix = timestring()
        rm_log_file_path = os.path.join(logdir,log_filename_prefix+"_RM.log")
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

        # # "console" handler (to stderr by default) with level ERROR
        # self.ch = logging.StreamHandler()
        # self.ch.setLevel(logging.ERROR)
        # self.formatter_console = logging.Formatter(
        #    "%(asctime)s %(levelname)-8s %(name)s: %(message)s")
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
