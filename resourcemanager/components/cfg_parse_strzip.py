# -*- coding: UTF-8 -*-
#
#   ::::::> Clobi CONFIGPARSER MODULE WITH ZIP/STRING SUPPORT <::::::
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
