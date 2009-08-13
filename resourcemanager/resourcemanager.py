# -*- coding: UTF-8 -*-
#
#   ::::::::> Clobi RESOURCE MANAGER <::::::::
#   Resource Manager main module / executable
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

import optparse
import threading
import time
import sys
import os
import Queue
import logging

from components.rm_session import ResourceManagerLogger, check_file, Tee
from components.rm_main_loop import ResourceManagerMainLoop
from components.rm_gui import ResourceManagerGUI


def main():
    """
    - parse commandline arguments
    - get absolute session path (session.cfg directory)
    - define/create RM run dir / session directories (log, save, ..)
    - set up logging: stderr split -> stderr/file; init logging module logger
    - set up thread communication components: os.pipe()s and Queue.Queue
    - start urwid GUI and Resource Manager's main loop
    """
    start_options = parseargs()
    sesspath = os.path.dirname(
        check_file(start_options.session_config_file_path))
    session_run_dir = os.path.join(sesspath,"ResourceManagerRun")
    session_dirs = dict(
        session_run_dir=session_run_dir,
        session_save_dir=os.path.join(session_run_dir,"save"),
        session_log_dir=os.path.join(session_run_dir,"log"))
    for key in session_dirs:
        if not os.path.exists(session_dirs[key]):
            os.mkdir(session_dirs[key])

    # create inter thread communication components
    queue_uicmds = Queue.Queue()
    pipe_log_read, pipe_log_write = os.pipe()
    pipe_cmdresp_read, pipe_cmdresp_write = os.pipe()
    pipe_uiinfo_update_read, pipe_uiinfo_update_write = os.pipe()
    pipe_stderr_read, pipe_stderr_write = os.pipe()
    # set up logging logger
    rootlog = ResourceManagerLogger(
        pipe_log_write,
        session_dirs['session_log_dir'])
    mainlog = logging.getLogger("RM")

    # log stderr to stderr, to a real file and to a pipe (monitored by GUI)
    stderr_logfile_path = os.path.join(session_dirs['session_log_dir'],
        time.strftime("%Y%m%d-%H%M%S", time.localtime()) + "_RM_stderr.log")
    stderr_log_fd = open(stderr_logfile_path,'w')
    sys.stderr = Tee(sys.stderr, stderr_log_fd, pipe_stderr_write)

    # a quick "dont-run-multiple-instances-in-the-same-session-dir" solution
    lockfilepath = os.path.join(session_run_dir,'RM.LOCKFILE')
    mainlog.debug("check lockfile: %s" % lockfilepath)
    if os.path.exists(lockfilepath):
        mainlog.critical(("lockfile exists: %s\n->exit. Is another "
            "instance of Resource Manager running the same session?"
            % lockfilepath))
        sys.exit(1)
    else:
        mainlog.debug("lockfile did not exist. create: %s" % lockfilepath)
        lock_fd = open(lockfilepath,'w')

    # here the crucial part begins
    try:
        rm_mainloop_thread = ResourceManagerMainLoop(
            pipe_cmdresp_write,
            pipe_uiinfo_update_write,
            queue_uicmds, start_options,
            session_dirs)
        mainlog.info("start ResourceManagerMainLoop thread...")
        rm_mainloop_thread.start()
        mainlog.debug(("remove console handler from logger to keep stderr"
            " free while GUI phase."))
        rootlog.remove_console_handler()
        try:
            gui = ResourceManagerGUI(
                pipe_log_read=pipe_log_read,
                pipe_cmdresp_read=pipe_cmdresp_read,
                pipe_uiinfo_update_read=pipe_uiinfo_update_read,
                pipe_stderr_read=pipe_stderr_read,
                queue_uicmds=queue_uicmds)
            mainlog.debug("start ResourceManagerGUI: gui.main()")
            gui.main()
            mainlog.debug("returned from gui.main()")
        except:     # delay exception, at first try to quit the other thread
            mainlog.critical(("GUI ERROR! send ResourceManagerMainLoop thread"
                " signal to QUIT.."))
            queue_uicmds.put("quit")
            raise   # raise exception here
        mainlog.debug("Wait for thread(s) to join..")
        print "Wait for thread(s) to join.."
        rm_mainloop_thread.join()
    finally:
        mainlog.debug("remove lockfile...")
        lock_fd.close()
        os.remove(lockfilepath)
        mainlog.info("shutting down logger...")
        logging.shutdown()


def parseargs():
    """
    Parse commandlineoptions using the optparse module and check them for
    logical consistence. Generate help- and usage-output.

    @return: optparse `options`-object containing the commandline options
    """
    version = '0'
    description = ("Resource Manager")
    usage = ("%prog --start --sessionconfig path/to/config.cfg \n"
             "%prog --resume --sessionconfig path/to/config.cfg \n"
             "try -h, --help and --version")
    parser = optparse.OptionParser(
        usage=usage,
        version=version,
        description=description)

    parser.add_option('-c', '--sessionconfig', dest='session_config_file_path',
        help='initial session configuration file (required for --start)')
    parser.add_option('--start', action='store_true', dest='start',
        default=False,
        help='start new session (requires --sessionconfig to be set)')
    parser.add_option('--resume', action='store_true', dest='resume',
        default=False,
        help='resume existing session (requires --sessionconfig to be set)')
    # now read in the given arguments (from sys.argv by default)
    (options, args) = parser.parse_args()

    # now check the logical consistence...
    if int(options.start)+int(options.resume) is not 1:
        parser.error('exactly one of [--start, --resume, ...] must be set!')
    elif options.start:
        if options.session_config_file_path is None:
            parser.error(('when --start is set, --sessionconfig '
                'path/to/config.cfg is required!'))
    elif options.resume:
        if options.session_config_file_path is None:
            parser.error(('when --resume is set, --sessionconfig '
                'path/to/config.cfg is required!'))
    return options

if __name__ == "__main__":
    main()
