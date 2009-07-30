# -*- coding: UTF-8 -*-
#
#   ::::::> CONFIGPARSER MODULE WITH ZIP/STRING SUPPORT <::::::
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

import cStringIO
import zlib
import ConfigParser


class SafeConfigParserStringZip(ConfigParser.SafeConfigParser):
    """
    Add write-to- and read-from- string methods to SafeConfigParser.
    Handle zip-compressed strings (better compression for short strings
    than with bz2)
    """

    def write_to_string(self):
        """
        Write current config to StringIO object, return value

        @return: string with current config
        """
        configstring_fd = cStringIO.StringIO()
        self.write(configstring_fd)
        configstring_fd.flush()
        #print ".buf: " + repr(configstring_fd.buf)
        #print ".buflist: " + repr(configstring_fd.buflist)
        configstring = configstring_fd.getvalue()
        return configstring

    def write_to_zipped_string(self):
        """
        @return: zipped string with current config
        """
        return zlib.compress(self.write_to_string())

    def read_from_string(self, string):
        """
        Update current config from string content
        """
        configstring_fd = cStringIO.StringIO()
        configstring_fd.write(string)
        configstring_fd.flush()
        configstring_fd.seek(0)
        self.readfp(configstring_fd)
        configstring_fd.close()

    def read_from_zipped_string(self, zipstring):
        """
        Update current config from zipped string
        """
        self.read_from_string(zlib.decompress(zipstring))
