# -*- coding: UTF-8 -*-
#
#   ::::::::> RESOURCE MANAGER <::::::::
#   Resource Manager Main
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
    - get absolute session path (directory) and define/create RM run dir
    - set up session directories (log, save, ..)
    - set up logging: stderr -> split -> stderr/file; init logging
    - set up thread communication components: os.pipe() and Queue.Queue
    - start urwid GUI
    - start Resource Manager's main loop
    """
    start_options = parseargs()
    sesspath = os.path.dirname(check_file(
                               start_options.session_config_file_path))
    session_run_dir = os.path.join(sesspath,"ResourceManagerRun")
    session_dirs = dict(session_run_dir=session_run_dir,
                        session_save_dir=os.path.join(session_run_dir,"save"),
                        session_log_dir=os.path.join(session_run_dir,"log"))
    for key in session_dirs:
        if not os.path.exists(session_dirs[key]):
            os.mkdir(session_run_dir)

    # log stderr to a file (not stdout: with urwid, stdout is very noisy :-))
    # what goes to stderr?
    # 1) tracebacks 2) logger with streamhandler to stderr; debuglevel=ERROR
    stderr_logfile_path = os.path.join(session_dirs['session_log_dir'],
        time.strftime("%Y%m%d-%H%M%S", time.localtime()) + "_RM_stderr.log")
    stderr_log_fd = open(stderr_logfile_path,'w')
    sys.stderr = Tee(sys.stdout, stderr_log_fd)
    # create inter thread communication components
    queue_uicmds = Queue.Queue()
    pipe_log_read, pipe_log_write = os.pipe()
    pipe_cmdresp_read, pipe_cmdresp_write = os.pipe()
    pipe_uiinfo_update_read, pipe_uiinfo_update_write = os.pipe()
    # create root logger
    rootlog = ResourceManagerLogger(pipe_log_write, session_dirs['session_log_dir'])
    mainlog = logging.getLogger("RM")

    lockfilepath = os.path.join(session_run_dir,'RM.LOCKFILE')
    mainlog.debug("check lockfile: "+lockfilepath)
    if os.path.exists(lockfilepath):
        mainlog.critical(("lockfile exists: "+lockfilepath+"\n->exit. Is another "
                          "instance of Resource Manager running the same session?"))
        sys.exit(1)
    else:
        mainlog.debug("lockfile did not exist. create: "+lockfilepath)
        lock_fd = open(lockfilepath,'w')

    try:
        rm_mainloop_thread = ResourceManagerMainLoop(pipe_cmdresp_write,
                                                     pipe_uiinfo_update_write,
                                                     queue_uicmds, start_options,
                                                     session_dirs)
        mainlog.info("start ResourceManagerMainLoop thread...")
        rm_mainloop_thread.start()
        try:
            gui = ResourceManagerGUI(pipe_log_read, pipe_cmdresp_read, pipe_uiinfo_update_read, queue_uicmds)
            mainlog.info("start ResourceManagerGUI...")
            gui.main()
        except:
            mainlog.critical("GUI ERROR! send ResourceManagerMainLoop thread signal to QUIT..")
            queue_uicmds.put("quit")
            raise
        print "Wait for thread(s) to join."
        rm_mainloop_thread.join()
    finally:
        lock_fd.close()
        os.remove(lockfilepath)
        mainlog.info("shut down logger...")
        logging.shutdown()


def parseargs():
    """
    Parse commandlineoptions using the optparse module and check them for
    logical consistence. Generate help- and usage-output.

    @return: optparse `options`-object containing the commandline options
    """
    # the following part configures the OptionParser...
    version = '0'
    description = ("Resource Manager")
    usage = ("%prog --start --sessionconfig path/to/config.cfg \n"
             "try -h, --help and --version")
    parser = optparse.OptionParser(usage=usage,version=version,description=description)

    parser.add_option('-c', '--sessionconfig', dest='session_config_file_path',
                      help='initial session configuration file (required for --start)')

    parser.add_option('--start', action='store_true', dest='start', default=False,
                      help='start new session (requires --sessionconfig to be set)')

    parser.add_option('--resume', action='store_true', dest='resume', default=False,
                      help='resume existing session (requires --sessionconfig to be set)')
    # now read in the given arguments (from sys.argv by default)
    (options, args) = parser.parse_args()

    # now check the logical consistence...
    if int(options.start)+int(options.resume) is not 1:
        parser.error('exactly one of [--start, --resume, ...] must be set!')
    elif options.start:
        if options.session_config_file_path is None:
            parser.error('when --start is set, --sessionconfig path/to/config.cfg is required!')
    elif options.resume:
        if options.session_config_file_path is None:
            parser.error('when --resume is set, --sessionconfig path/to/config.cfg is required!')
    return options

if __name__ == "__main__":
    main()









