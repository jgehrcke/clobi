# -*- coding: UTF-8 -*-
#
#   ::::::::> RESOURCE MANAGER <::::::::
#   Resource Manager GUI
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

import sys
import os
import logging

from components.cfg_parse_strzip import SafeConfigParserStringZip

sys.path.append("components")
import urwid
import urwid.raw_display


class ResourceManagerGUI(object):
    """
    urwid userinterface.

    @params:
    - pipe_log_read:        reading os.pipe() filedescriptor. here, the log
                            messages arrive from ResourceManagerMainLoop thread.
                            They are displayed in the body (ListBox) of the UI.
    - pipe_cmdresp_read:    reading os.pipe() filedescriptor. here, responses
                            on entered commands arrive (one-line-messages).
    - pipe_uiinfo_update_read:  reading os.pipe() filedescriptor. here, UI
                            update information arrives.
    - pipe_stderr_read:     stderr is -- among others -- written to this pipe
    - queue_uicmds:         Queue.Queue() to communicate user-given commands to
                            the ResourceManagerMainLoop thread.
    """
    def __init__(self,
                 pipe_log_read,
                 pipe_cmdresp_read,
                 pipe_uiinfo_update_read,
                 pipe_stderr_read,
                 queue_uicmds):
        self.logger = logging.getLogger("RM.GUI")
        self.logger.debug("initialize ResourceManagerGUI object")

        self.pipe_log_read = pipe_log_read
        self.pipe_cmdresp_read = pipe_cmdresp_read
        self.pipe_uiinfo_update_read = pipe_uiinfo_update_read
        self.pipe_stderr_read = pipe_stderr_read
        self.queue_uicmds = queue_uicmds
        self.pipe_uiinfo_update_prefix = ''
        self.pipe_log_prefix = ''

        text_header = ("☺☺☺ CLOUD RESOURCE MANAGER 0.1 ✔ http://gehrcke.de ☺☺☺")

        # UI LAYOUT in two levels
        # top widget:   FRAME with HEADER, BODY, FOOTER
        # sub widgets:  pile in HEADER with pile_header, pile_body
        #               listbox in BODY
        #               edit in FOOTER

        # HEADER
        self.txt_sdb_upd = urwid.Text('', align='left', wrap='any')
        self.txt_sqs_upd = urwid.Text('', align='left', wrap='any')
        self.txt_cloud = urwid.Text('', align='left')
        self.txt_name = urwid.Text('', align='left')
        self.hd_pl_cl_1 = 	urwid.Pile([
            urwid.Text('SESSION INFO', align='center'),
            urwid.Divider("-"),
            self.txt_name,
            self.txt_cloud,
            self.txt_sqs_upd,
            self.txt_sdb_upd
            ])

        self.txt_sqs_jobs = urwid.Text('P01: 27 jobs\nP02: 13 jobs', align='left', wrap='any')
        self.hd_pl_cl_2 = 	urwid.Pile([
            urwid.Text('SQS DATA', align='center'),
            urwid.Divider("-"),
            self.txt_sqs_jobs,
            ])

        self.hd_pl_cl_3 = 	urwid.Pile([
            urwid.Text('SDB DATA', align='center'),
            urwid.Divider("-"),
            urwid.Text('started VMs:\n   EC2:12\n   Nb1:1\n   Nb2:13', align='left', wrap='any'),
            urwid.Text('running VMs:\n   EC2:10\n   Nb1:0\n   Nb2:12', align='left', wrap='any'),
            ])

        self.uiinfo_dict = dict(
            txt_name='Name: ',
            txt_sdb_upd='SDB update: XXX s',
            txt_sqs_upd='SQS update: YYY s',
            txt_cloud='Clouds: ',
            txt_sqs_jobs='')

        self.header_body = urwid.Columns([
            self.hd_pl_cl_1,
            self.hd_pl_cl_2,
            self.hd_pl_cl_3], 1)

        self.header_header = urwid.AttrWrap(urwid.Text(text_header,align='center'),'header')
        self.header = urwid.Pile([
            self.header_header,
            urwid.AttrWrap(self.header_body, 'header_body')])

        # BODY
        self.list_walker = urwid.SimpleListWalker([])
        self.listbox = urwid.ListBox(self.list_walker)

        # FOOTER
        self.edit = urwid.Edit(('editcp',">>> "), wrap='clip')
        self.footer = urwid.AttrWrap(self.edit, 'editbx')

        self.top = urwid.Frame(
            body=urwid.AttrWrap(self.listbox, 'body'),
            header=self.header,
            footer=self.footer,
            focus_part='footer')

    def main(self):
        """
        Set up urwid screen, color palette und MainLoop, including all
        filedescriptors to monitor with SelectEventLoop. Then run MainLoop.
        """
        self.screen = urwid.raw_display.Screen()
        self.screen.set_input_timeouts(max_wait=None)
        self.palette = [
            ('body','black','light blue','standout'),
            ('header','white','dark red','bold'),
            ('header_body','white','dark blue'),
            ('editfc','white','dark blue','bold'),
            ('editbx','light gray','dark blue'),
            ('editcp','light gray', 'dark blue'),
            ('accepted_command','black','light green'),
            ('stderr','light red','light blue'),
            ]
        self.main_loop = urwid.MainLoop(
            widget=self.top,
            handle_mouse=True,
            palette=self.palette,
            screen=self.screen,
            unhandled_input=self.unhandled_input)
        self.main_loop.event_loop.watch_file(
            self.pipe_stderr_read,
            self.stderr_event)
        self.main_loop.event_loop.watch_file(
            self.pipe_log_read,
            self.pipe_log_event)
        self.main_loop.event_loop.watch_file(
            self.pipe_cmdresp_read,
            self.pipe_cmdresp_event)
        self.main_loop.event_loop.watch_file(
            self.pipe_uiinfo_update_read,
            self.pipe_uiinfo_update_event)
        self.logger.debug("run urwid's GUI main loop...")
        self.main_loop.run()

    def stderr_event(self):
        """
        Callback function from urwid.MainLoop(). Called when select.select()
        returns `self.pipe_stderr_read`.
        """
        new_data = os.read(self.pipe_stderr_read,9999999).decode('UTF-8')
        if len(new_data):
            new_text = urwid.Text(('stderr', "STDERR: "+new_data))
            self.listbox_extend([new_text])

    def pipe_log_event(self):
        """
        Callback function from urwid.MainLoop(). Called when select.select()
        returns `self.pipe_log_read` -> Log data is on the pipe for the UI
        to display in the ListBox! Hence, read from pipe, splitlines and make
        a Text widget out of each line. One write may be returned in more than
        one read -> chopped lines -> reassembling post-processing -> listbox
        """
        new_data = self.pipe_log_prefix + os.read(self.pipe_log_read,9999999).decode('UTF-8')
        self.pipe_log_prefix = ''
        if new_data:
            new_lines = new_data.splitlines(True)
            if not new_lines[-1].endswith("\n"):
                self.pipe_log_prefix = new_lines[-1]
                del new_lines[-1]
            new_text_list = [urwid.Text(line.rstrip()) for line in new_lines if line.rstrip()]
            self.listbox_extend(new_text_list)

    def pipe_cmdresp_event(self):
        """
        Callback function from urwid.MainLoop(). Called when select.select()
        returns `self.pipe_cmdresp_read`.
        There is a command response in the pipe!
        Read from pipe, assume one-liners and display them as command response
        """
        new_data = os.read(self.pipe_cmdresp_read,9999999).decode('UTF-8')
        if len(new_data):
            self.logger.debug("cmd response: %s" % new_data)
            new_text = urwid.Text(('accepted_command', new_data))
            self.listbox_extend([new_text])

    def pipe_uiinfo_update_event(self):
        """
        Callback function from urwid.MainLoop(). Called when select.select()
        returns `self.pipe_uiinfo_update_read`.
        Read from pipe, collect UIupdate data. Data comes as "ConfigParserfile".
        Delimiter %% and && to delimit one update data set: %%ConfigString&&
        This is because one data string could come chopped:
                read 1) %%data\nstring\nblabla&&%%datastring\nblubbeginning
                read 2) datastringblubending&&
        """
        pipestring = (self.pipe_uiinfo_update_prefix +
                      os.read(self.pipe_uiinfo_update_read,9999999).decode('UTF-8'))
        self.pipe_uiinfo_update_prefix = ''
        datasets = []
        for string in pipestring.split("%%"):
            if string:
                if string.endswith("&&"):
                    datasets.append(string.rstrip("&&"))
                else:
                    self.pipe_uiinfo_update_prefix = "%%"+string
        for dataset in datasets:
            config = SafeConfigParserStringZip()
            config.read_from_string(dataset.encode('UTF-8'))
            # now assemble update-dictionary that can be passed to uiiinfo_update().
            # at this point, the section "uiinfo" is hard coded!!
            update_dict = {}
            for option in config.options('uiinfo'):
                update_dict[option] = config.get('uiinfo',option).decode('UTF-8')
            self.uiinfo_update(update_dict)

    def uiinfo_update(self, update_dict={}):
        """
        Update UI information (session / SQS / SDB / VM information) with
        information that was transmitted via pipe from ResourceManagerMainLoop
        thread.

        @params: update_dict: dictionary containing data to be updated
        """
        self.uiinfo_dict.update(update_dict)

        self.txt_cloud.set_text(self.uiinfo_dict['txt_cloud'])
        self.txt_sqs_upd.set_text(self.uiinfo_dict['txt_sqs_upd'])
        self.txt_sdb_upd.set_text(self.uiinfo_dict['txt_sdb_upd'])
        self.txt_name.set_text(self.uiinfo_dict['txt_name'])
        self.txt_sqs_jobs.set_text(self.uiinfo_dict['txt_sqs_jobs'])
        self.main_loop.draw_screen()

    def listbox_extend(self, extension):
        """
        Extend listbox by `extension` (which must be *list* of urwid widgets).
        Limit overall length of listbox. Scroll automatically down, but only if
        the focus is somewhere at the bottom of the list (tolerance interval
        of about 20 items away from the bottom).
        Draw screen to show changes.
        """
        scroll = True
        if self.list_walker.get_focus()[1] is not None:
            if self.list_walker.get_focus()[1]+20 < len(self.list_walker.contents):
                scroll = False
        self.list_walker.contents.extend(extension)
        del self.list_walker.contents[0:-10000]
        if scroll: self.list_walker.set_focus(len(self.list_walker.contents))
        self.main_loop.draw_screen()

    def unhandled_input(self, key):
        """
        Callback function which gets called from urwid's MainLoop, when there is
        user-input left over after all screen control related input processing.
        """
        if key == 'enter':
            instring = self.edit.get_edit_text()
            self.logger.debug("command entered: %s" % instring)
            self.queue_uicmds.put(instring)
            self.edit.set_edit_text('')
            if instring == 'quit':
                self.logger.debug("raise urwid.ExitMainLoop()")
                raise urwid.ExitMainLoop()
        if key == 'up' or key == 'down' or key == 'page up' or key == 'page down':
            self.top.set_focus('body')
            self.top.keypress(self.main_loop.screen_size,key)
            self.top.set_focus('footer')
