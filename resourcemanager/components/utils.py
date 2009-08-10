# -*- coding: UTF-8 -*-
#
#   ::::::::> Clobi utility stuff <::::::::
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
import os
import sys
import tarfile
import time
import shutil


# create module logger
logger = logging.getLogger("utils.py")


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


def backup_file(file_path, backup_dir_path, archive_from = None):
    """
    Backup any file to any dir. Therefore, append the original filename with
    a timestring (if one file is backupped multiple times within one second,
    it's not worth to keep all backups; hence, 1 s resolution is okay here.)
    If `archive_from` is set, all files get archived, if there are equal or more
    than `archive_from`.
    """
    logger.debug("backup %s to %s ..." %(file_path, backup_dir_path))
    timestring = time.strftime("%Y%m%d-%H%M%S", time.localtime())

    # archive files, if there are at least `archive_from` files
    if archive_from:
        # assemble list of files to potentially archive
        filename_list = []
        for filedir in os.listdir(backup_dir_path):
            if os.path.isfile(os.path.join(backup_dir_path,filedir)):
                if not filedir.endswith("tar.gz"):
                    filename_list.append(filedir)
        logger.debug(("found %s files to potentially archive"
            % len(filename_list)))
        # there are at least `archive_from` -> archive, delete
        if len(filename_list) >= archive_from:
            tarpath = os.path.join(backup_dir_path,
                "multifilebckp_"+timestring+".tar.gz")
            tar = tarfile.open(tarpath, "w:bz2")
            for filename in filename_list:
                tar.add(os.path.join(backup_dir_path,filename),filename)
            tar.close()
            logger.debug("created backup archive: %s" % tarpath)
            for filename in filename_list:
                os.remove(os.path.join(backup_dir_path,filename))
            logger.debug("deleted %s backup files" % len(filename_list))

    # do the backup job
    if os.path.exists(file_path) and os.path.exists(backup_dir_path):
        if os.path.isfile(file_path) and os.path.isdir(backup_dir_path):
            filename = os.path.basename(file_path)
            bckp_filename = "%s_%s" % (filename, timestring)
            bckp_file_path = os.path.join(backup_dir_path, bckp_filename)
            shutil.copy(file_path, bckp_file_path)
        else:
            logger.debug(("%s not file and/or %s not dir"
                % (file_path, backup_dir_path)))
    else:
        logger.debug(("%s and/or %s does not exist"
            % (file_path, backup_dir_path)))


def timestring():
    return time.strftime("%Y%m%d-%H%M%S", time.localtime())

def utc_timestring():
    return time.strftime("UTC%Y%m%d-%H%M%S", time.time())

def alarm(last_triggered, interval):
    now = time.time()
    if min(0, (now - (last_triggered + interval))) == 0:
        return True
    return False


def check_file(file):
    """
    Check if a given file exists and really is a file (e.g. not a directory)
    In errorcase the script is stopped.

    @return: the absolute path of the file
    """
    logger.debug("check_file("+file+")")
    if not os.path.exists(file):
        logger.critical(file+' does not exist. Exit.')
        sys.exit(1)
    if not os.path.isfile(file):
        logger.critical(file+' is not a file. Exit.')
        sys.exit(1)
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
        sys.exit(1)
    if not os.path.isdir(dir):
        logger.critical(dir+' is not a file. Exit.')
        sys.exit(1)
    return os.path.abspath(dir)