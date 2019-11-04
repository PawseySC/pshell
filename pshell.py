#!/usr/bin/env python

import os
import re
import cmd
import sys
import glob
import math
import time
import getpass
import urllib2
import zipfile
import argparse
import urlparse
import datetime
import itertools
import posixpath
import ConfigParser
import xml.etree.ElementTree as ET
import mfclient
# no readline on windows
try:
    import readline
except:
    pass

# standard lib python command line client for mediaflux
# Author: Sean Fleming
build= "Latest"
delegate_default = 7
delegate_min = 1
delegate_max = 365

# NB: we want errors from server on failure so we can produce a non 0 exit code, if required
class parser(cmd.Cmd):
    config = None
    config_name = None
    config_filepath = None
    mf_client = None
    cwd = '/projects'
    interactive = True
    need_auth = True
    transfer_processes = 1
    terminal_height = 20

# --- initial setup of prompt
    def preloop(self):
        if self.need_auth:
            self.prompt = "%s:offline>" % self.config_name
        else:
            self.prompt = "%s:%s>" % (self.config_name, self.cwd)

# --- not logged in -> don't even attempt to process remote commands
    def precmd(self, line):
        if self.need_auth:
            if self.requires_auth(line):
                print "Not logged in."
                return cmd.Cmd.precmd(self, "")
        return cmd.Cmd.precmd(self, line)

# --- prompt refresh (eg after login/logout)
    def postcmd(self, stop, line):
        if self.need_auth:
            self.prompt = "%s:offline>" % self.config_name
        else:
            self.prompt = "%s:%s>" % (self.config_name, self.cwd)
        return cmd.Cmd.postcmd(self, stop, line)

# --- helper: attempt to complete a namespace
    def complete_namespace(self, partial_ns, start):

# extract any partial namespace to use as pattern match
        match = re.match(r".*/", partial_ns)
        if match:
            offset = match.end()
            pattern = partial_ns[offset:]
        else:
            offset = 0
            pattern = partial_ns

# namespace fragment prefix (if any) to include in the returned candidate
        prefix = partial_ns[start:offset]
# offset to use when extracting completion string from candidate matches
        xlat_offset = max(0, start-offset)

# special case - we "know" .. is a namespace
        if pattern == "..":
            return [partial_ns[start:]+"/"]

# construct an absolute namespace (required for any remote lookups)
        target_ns = self.absolute_remote_filepath(partial_ns[:offset])
        self.mf_client.log("DEBUG", "cn seek: target_ns: [%s] : prefix=[%r] : pattern=[%r] : start=%r : xlat=%r" % (target_ns, prefix, pattern, start, xlat_offset), level=2)

# generate listing in target namespace for completion matches
        result = self.mf_client.aterm_run('asset.namespace.list :namespace "%s"' % target_ns)

        ns_list = []
        for elem in result.iter('namespace'):
            if elem.text is not None:
# namespace matches the pattern we're looking for?
                item = None
                if len(pattern) != 0:
                    if elem.text.startswith(pattern):
                        item = posixpath.join(prefix, elem.text[xlat_offset:]+"/")
                else:
                    item = posixpath.join(prefix, elem.text[xlat_offset:]+"/")

                if item is not None:
                    ns_list.append(item)

        self.mf_client.log("DEBUG", "cn found: %r" % ns_list, level=2)

        return ns_list

# --- helper: attempt to complete an asset
    def complete_asset(self, partial_asset_path, start):

# construct an absolute namespace (required for any remote lookups)
        candidate_ns = self.absolute_remote_filepath(partial_asset_path)

        if self.mf_client.namespace_exists(candidate_ns):
# candidate is a namespace -> it's our target for listing
            target_ns = candidate_ns
# no pattern -> add all namespaces
            pattern = None
# replacement prefix for any matches
            prefix = partial_asset_path[start:]
        else:
# candidate not a namespace -> set the parent as the namespace target
            match = re.match(r".*/", candidate_ns)
            if match:
                target_ns = match.group(0)
# extract pattern to search and prefix for any matches
                pattern = candidate_ns[match.end():]
                prefix = partial_asset_path[start:-len(pattern)]
            else:
                return None

        target_ns = self.escape_single_quotes(target_ns)
        self.mf_client.log("DEBUG", "ca seek: target_ns: [%s] : pattern = %r : prefix = %r" % (target_ns, pattern, prefix), level=2)

        if pattern is not None:
            result = self.mf_client.aterm_run("asset.query :where \"namespace='%s' and name ='%s*'\" :action get-values :xpath -ename name name" % (target_ns, pattern))
        else:
            result = self.mf_client.aterm_run("asset.query :where \"namespace='%s'\" :action get-values :xpath -ename name name" % target_ns)

#       ALT? eg for elem in result.findall(".//name")
        asset_list = []
        for elem in result.iter("name"):
            if elem.text is not None:
                asset_list.append(posixpath.join(prefix, elem.text))

        self.mf_client.log("DEBUG", "ca found: %r" % asset_list, level=2)

        return asset_list

# NB: if the return result is ambigious (>1 option) it'll require 2 presses to get the list
# turn off DEBUG -> gets in the way of commandline completion
# NB: index offsets are 1 greater than the command under completion

# ---
    def complete_get(self, text, line, start_index, end_index):
        candidate_list = self.complete_asset(line[4:end_index], start_index-4)
        candidate_list += self.complete_namespace(line[4:end_index], start_index-4)
        return candidate_list

# ---
    def complete_rm(self, text, line, start_index, end_index):
        candidate_list = self.complete_asset(line[3:end_index], start_index-3)
        candidate_list += self.complete_namespace(line[3:end_index], start_index-3)
        return candidate_list

# ---
    def complete_file(self, text, line, start_index, end_index):
        candidate_list = self.complete_asset(line[5:end_index], start_index-5)
        candidate_list += self.complete_namespace(line[5:end_index], start_index-5)
        return candidate_list

# ---
    def complete_publish(self, text, line, start_index, end_index):
        candidate_list = self.complete_asset(line[8:end_index], start_index-8)
        candidate_list += self.complete_namespace(line[8:end_index], start_index-8)
        return candidate_list

# ---
    def complete_ls(self, text, line, start_index, end_index):
        candidate_list = self.complete_asset(line[3:end_index], start_index-3)
        candidate_list += self.complete_namespace(line[3:end_index], start_index-3)
        return candidate_list

# ---
    def complete_cd(self, text, line, start_index, end_index):
        ns_list = self.complete_namespace(line[3:end_index], start_index-3)
        return ns_list

# ---
    def complete_mkdir(self, text, line, start_index, end_index):
        ns_list = self.complete_namespace(line[6:end_index], start_index-6)
        return ns_list

# ---
    def complete_rmdir(self, text, line, start_index, end_index):
        ns_list = self.complete_namespace(line[6:end_index], start_index-6)
        return ns_list

# ---
    def emptyline(self):
        return

# ---
    def default(self, line):
# unrecognized - assume it's an aterm command
        reply = self.mf_client.aterm_run(line)
        self.mf_client.xml_print(reply)
        return

# --- helper
    def requires_auth(self, line):
        local_commands = ["login", "help", "lls", "lcd", "lpwd", "debug", "version", "exit", "quit"]

# only want first keyword (avoid getting "not logged in" on input like "help get")
        try:
            primary = line.strip().split()[0]
            if primary in local_commands:
                return False
        except:
            pass

        return True

# --- helper
    def human_size(self, nbytes):
        suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']

        if nbytes:
            rank = int((math.log10(nbytes)) / 3)
            rank = min(rank, len(suffixes) - 1)
            human = nbytes / (1000.0 ** rank)
            f = ("%.2f" % human).rstrip('0').rstrip('.')
        else:
            f = "0"
            rank = 0

        return "%6s %-2s" % (f, suffixes[rank])

# --- helper
    def human_time(self, nseconds):
        if nseconds:
            if nseconds < 120:
                text = "%d secs" % nseconds
            else:
                value = float(nseconds) / 60.0
                text = "%.1f mins" % value

        return text

# --- helper
    def ask(self, text):
# if script, assumes you know what you're doing
        if self.interactive is False:
            return True
        response = raw_input(text)
        if response == 'y' or response == 'Y':
            return True
        return False

# --- helper
    def escape_single_quotes(self, namespace):
        return namespace.replace("'", "\\'")

# --- helper: convert a relative/absolute mediaflux namespace/asset reference to minimal (non-quoted) absolute form
    def absolute_remote_filepath(self, line):

        if line.startswith('"') and line.endswith('"'):
            line = line[1:-1]

        if not posixpath.isabs(line):
            line = posixpath.join(self.cwd, line)

        fullpath = posixpath.normpath(line)

        return fullpath

# --- helper: strip any flags as well as get the path
    def split_flags_filepath(self, line):

        flags = ""
        if line.startswith('-'):
            match = re.match(r"\S*", line) 
            if match is not None:
                flags = match.group(0)
                line = line[match.end(0)+1:]
#        print "flags=[%s] line=[%s]" % (flags, line)
        return flags, self.absolute_remote_filepath(line)

# --- version tracking 
    def help_version(self):
        print "\nReturn the current version build identifier\n"
        print "Usage: build\n"

    def do_version(self, line):
        global build
        print " VERSION: %s" % build

# --- file info
    def help_file(self):
        print "\nReturn metadata information on a remote file\n"
        print "Usage: file <filename>\n"

    def do_file(self, line):
# get asset metadata
        output_list = []
        result = self.mf_client.aterm_run('asset.get :id "path=%s"' % self.absolute_remote_filepath(line))
        elem = result.find(".//asset")
        output_list.append("%-10s : %s" % ('asset ID', elem.attrib['id']))
        xpath_list = [".//asset/path", ".//asset/ctime", ".//asset/type", ".//content/size", ".//content/csum"]
        for xpath in xpath_list:
            elem = result.find(xpath)
            if elem is not None:
                output_list.append("%-10s : %s" % (elem.tag, elem.text))
# get content status 
# TODO - migrating direction etc
        result = self.mf_client.aterm_run('asset.content.status :id "path=%s"' % self.absolute_remote_filepath(line))
        elem = result.find(".//asset/state")
        if elem is not None:
            output_list.append("%-10s : %s" % (elem.tag, elem.text))

# published (public URL)
        result = self.mf_client.aterm_run('asset.label.exists :id "path=%s" :label PUBLISHED' % self.absolute_remote_filepath(line))
        elem = result.find(".//exists")
        if elem is not None:
            output_list.append("published  : %s" % elem.text)

# output info
        for line in output_list:
            print line

# --- helper
# immediately return any key pressed as a character
    def wait_key(self):
#        import select

        result = None
        if self.interactive is False:
            return result

# TODO - can we use something like this to replace the ugly platform specific stuff ???
#        while True:
#            if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
#                line = raw_input()
#                print "got [%s]" % line
#                return line

        if os.name == 'nt':
            import msvcrt
            result = msvcrt.getch()
        else:
            import termios
            fd = sys.stdin.fileno()

            oldterm = termios.tcgetattr(fd)
            newattr = termios.tcgetattr(fd)
            newattr[3] = newattr[3] & ~termios.ICANON & ~termios.ECHO
            termios.tcsetattr(fd, termios.TCSANOW, newattr)

            try:
                result = sys.stdin.read(1)
            except IOError:
                pass
            finally:
                termios.tcsetattr(fd, termios.TCSAFLUSH, oldterm)

        return result

# display prompt and return pagination specific directives (next page, quit)
    def pagination_controller(self, prompt):
        result = None
        if prompt is not None:
            if self.interactive:
                sys.stdout.write(prompt)
                result = ""
                while True:
                    key = self.wait_key()
#                    print "got [%r]" % key
                    sys.stdout.write(key)
# end pagination immediately on q press
                    if key == 'q':
                        result += key
                        print
                        return result
# handle backspaces on windows
                    elif key == '\x08':
                        sys.stdout.write(" \b")
                        sys.stdout.flush()
                        result = result[:-1]
# handle backspaces on *nix
                    elif key == '\x7f':
                        sys.stdout.write("\b \b")
                        sys.stdout.flush()
                        result = result[:-1]
# *nix <enter> press
                    elif key == '\n':
                        return result
# windows <enter> press (NB: need to force a newline - hence the print)
                    elif key == '\r':
                        print
                        return result
# concat everything else onto the final result
                    else:
                        result += key
            else:
                print prompt

        return result

# ---
    def help_ls(self):
        print "\nList files stored on the remote server."
        print "Navigation in paginated output can be achieved by entering a page number, [enter] for next page or q to quit.\n"
        print "Usage: ls <file pattern or folder name>\n"

# --- paginated ls
    def remote_ls_print(self, namespace, namespace_list, namespace_count, asset_count, page, page_size, query, show_content_state=False):
        page_count = max(1, 1+int((namespace_count+asset_count-1) / page_size))
#        print "remote_ls(): [%s] nc=%d ac=%d - p=%d pc=%d ps=%d - query=%s" % (namespace, namespace_count, asset_count, page, page_count, page_size, query)

# number of namespaces and assets to show, given current pagination values
        namespace_todo = max(0, min(page_size, namespace_count - (page-1)*page_size))
        asset_todo = min(page_size, asset_count+namespace_count - (page-1)*page_size) - namespace_todo
        namespace_page_count = 1 + int((namespace_count-1) / page_count)

        if namespace_todo > 0:
            namespace_start = (page-1) * page_size
            for i in range(namespace_start,namespace_start+namespace_todo):
                elem = namespace_list[i]
                print "[Folder] %s" % elem.text

        if asset_todo > 0:
            asset_start = abs(min(0, namespace_count - (page-1)*page_size - namespace_todo))+1
            reply = self.mf_client.aterm_run('asset.query :where "%s" :sort < :key name :nulls include > :action get-values :xpath "id" -ename "id" :xpath "name" -ename "name" :xpath "content/type" -ename "type" :xpath "content/size" -ename "size" :xpath "mtime" -ename "mtime" :size %d :idx %d' % (query, asset_todo, asset_start))
            asset_list = reply.findall('.//asset')

# get the content status - this can be slow (timeouts) -> optional 
            if show_content_state is True:
                xml_reply_state = self.mf_client.aterm_run('asset.query :where "%s" :sort < :key name :nulls include > :size %d :idx %d :action pipe :service -name asset.content.status :pipe-generate-result-xml true' % (query, asset_todo, asset_start))

# NEW - for robustness - match asset IDs from the 2 separate queries to populate a single document
            for elem in reply.findall('.//asset'):
                child = elem.find('.//id')
                asset_id = child.text

                elem_state = ET.SubElement(elem, 'state')
                try:
                    xml_state = xml_reply_state.find(".//asset[@id='%s']/state" % asset_id)
                    elem_state.text = xml_state.text
                except:
                    elem_state.text = "?"

# TODO - extract direction from content.status
            asset_name = "?"

            for i, elem in enumerate(asset_list):
                child = elem.find('.//id')
                asset_id = child.text

                child = elem.find('.//name')
                if child.text is not None:
                    asset_name = child.text
                else:
                    asset_name = "?"

                child = elem.find('.//size')
                if child.text is not None:
                    asset_size = self.human_size(int(child.text))
                else:
                    asset_size = self.human_size(0)

                if show_content_state is True:
                    child = elem.find('.//state')
                    if "online" in child.text:
                        asset_state = "online    |"
                    else:
                        asset_state = "%-9s |" % child.text
                    print " %-10s | %s %s | %s" % (asset_id, asset_state, asset_size, asset_name)
                else:
                    print " %-10s | %s | %s" % (asset_id, asset_size, asset_name)

# --- ls with no dependency on www.list
    def do_ls(self, line):
# make candidate absolute path from input line 
        flags, candidate = self.split_flags_filepath(line)
# if flags contains 'l' -> show_content_state
        if 'l' in flags:
            show_more = True
        else:
            show_more = False

        ns_list = []
        try:
# candidate is a namespace reference
            reply = self.mf_client.aterm_run('asset.namespace.list :namespace "%s"' % candidate)
            cwd = candidate
# count namespaces 
            ns_list = reply.findall('.//namespace/namespace')
            namespace_count = len(ns_list)
            query = "namespace='%s'" % cwd

        except Exception as e:
# candidate is not a namespace -> assume input line is a filter
            cwd = posixpath.dirname(candidate)
            name_filter = posixpath.basename(candidate)
            name_filter = name_filter.replace("'", "\'")
# we have a filter -> ignore namespaces
            namespace_count = 0
            query = "namespace='%s' and name='%s'" % (cwd, name_filter)

# count assets 
        reply = self.mf_client.aterm_run("asset.query :where \"%s\" :action count" % query)
        elem = reply.find(".//value")
        asset_count = int(elem.text)
# setup pagination
        page = 1
        page_size = max(1, min(self.terminal_height - 3, 100))
        canonical_last =  1 + int((namespace_count + asset_count - 1) / page_size )
        if canonical_last == 0:
            pagination_complete = True
        else:
            pagination_complete = False

        show_header = True
        while pagination_complete is False:
            pagination_footer = None
            if show_header:
                print "%d items, %d items per page, remote folder: %s" % (namespace_count+asset_count, page_size, cwd)
                show_header = False
            page = max(1, min(page, canonical_last))

# print the current page
            self.remote_ls_print(cwd, ns_list, namespace_count, asset_count, page, page_size, query, show_content_state=show_more)

# auto exit on last page
            if page == canonical_last:
                break

# non-interactive - auto iterate through remaining pages
            if self.interactive is False:
                time.sleep(1)
                page = page + 1
                continue

# pagination control
            pagination_footer = "=== Page %r/%r (enter = next, number = jump, q = quit) === " % (page, canonical_last)
            response = self.pagination_controller(pagination_footer)
            if response is not None:
                try:
                    page = int(response)
                except:
                    if response == 'q' or response == 'quit':
                        pagination_complete = True
                        break
                    else:
                        page = page + 1
                        if page > canonical_last:
                            pagination_complete = True

# --
    def poll_total(self, base_query):
        total = dict()

# enforce these keys are present in the dictionary
        total['online-files'] = 0
        total['offline-files'] = 0

        count_files = 0
        count_bytes = 0

# run in background as this can timeout on larger DMF folders
        self.mf_client.log("DEBUG", "Polling online/offline statistics...") 
        result = self.mf_client.aterm_run('asset.content.status.statistics :where "%s" &' % base_query)

        for elem in result.iter("statistics"):
            state = elem.attrib.get('state', elem.text)

            if state == 'online+offline':
                state = 'online'

            for child in elem:
                if child.tag == "total":
                    count_files += int(child.text)
                    total[state+"-files"] = int(child.text)
                elif child.tag == "size":
                    count_bytes += int(child.text)
                    total[state+"-bytes"] = int(child.text)
                else:
                    total[state+"-"+child.tag] = int(child.text)

# enforce these keys are present in the dictionary
        total['total-files'] = count_files
        total['total-bytes'] = count_bytes

        return total

# prepare state - online + offline init
# return list of (online) files to download
    def get_online_set(self, base_query, base_namespace):

        online = dict()
        list_local_path = {}

# TODO - pending feedback from Arcitecta to solve or fix the issue
# hmmm backgrounding doesnt appear to for iterators ...
# result seems the same background or not ... but MF gives an error when using the background one (no session for iterator => MF bug?)
        self.mf_client.log("DEBUG", "Getting download iterator...")
        result = self.mf_client.aterm_run('asset.query :where "%s and content online" :as iterator :action get-values :xpath -ename id id :xpath -ename namespace namespace :xpath -ename filename name' % base_query)

        elem = result.find(".//iterator")
        iterator = elem.text
        iterate_size = 100

        iterate = True
        while iterate:
            self.mf_client.log("DEBUG", "Online iterator chunk")
# get file list for this sub-set
            result = self.mf_client.aterm_run("asset.query.iterate :id %s :size %d" % (iterator, iterate_size))

            for elem in result.iter("asset"):
                asset_id = None
                filename = None
                path = None
                for child in elem:
                    if child.tag == "id":
                        asset_id = child.text
                    if child.tag == "filename":
                        filename = child.text
                    if child.tag == "namespace":
                        namespace = child.text
# remote = *nix , local = windows or *nix
# the relative path should be computed from the starting namespace
                        remote_relpath = posixpath.relpath(path=namespace, start=base_namespace)
                        relpath_list = remote_relpath.split("/")
                        local_relpath = os.sep.join(relpath_list)
                        path = os.path.join(os.getcwd(), local_relpath)
                        self.mf_client.log("DEBUG", "local=%s : remote=%s" % (local_relpath, remote_relpath))

# add valid download entry
                if asset_id is not None and filename is not None:
                    if path is None:
                        filepath = os.path.join(os.getcwd(), filename)
                    else:
                        filepath = os.path.join(path, filename)
                        list_local_path[path] = 1

                    online[asset_id] = filepath

# check for completion - to avoid triggering a mediaflux exception on invalid iterator
            for elem in result.iter("iterated"):
                state = elem.get('complete')
                if "true" in state:
                    self.mf_client.log("DEBUG", "Asset iteration completed")
                    iterate = False

# TODO - *** split out this from get_online_set() -> call ONCE on ALL files at the start, rather than polling
# create any required local dirs (NB: may get exception if they exist - hence the catch)
        for local_path in list_local_path:
            try:
                self.mf_client.log("DEBUG", "Creating local folder: %s" % local_path)
                os.makedirs(local_path)
            except Exception as e:
# TODO - this is too noisy currently as we're doing this more than we should, but unavoidable until the split out above *** is done
                pass

        return online

# --
    def print_over(self, text):
        sys.stdout.write("\r"+text)
        sys.stdout.flush()

# -- convert XML document to mediaflux shorthand XML markup
    def xml_to_mf(self, xml_root, result=None):
        if xml_root is not None:
            if xml_root.text is not None:
                result = " :%s %s" % (xml_root.tag, xml_root.text)
            else:
                result = " :%s <" % xml_root.tag
                for xml_child in xml_root:
                    if xml_child.text is not None:
                        result += " :%s %s" % (xml_child.tag, xml_child.text)
                    else:
                        result += self.xml_to_mf(xml_child, result)
                result += " >"
        return result

# -- metadata populator
    def import_metadata(self, asset_id, filepath):
        self.mf_client.log("DEBUG","import_metadata() [%s] : [%s]" % (asset_id, filepath))
        try:
            config = ConfigParser.ConfigParser()
            config.read(filepath)
# section -> xmlns
            xml_root = ET.Element(None)
            for section in config.sections():
                xml_child = ET.SubElement(xml_root, section)
                for option in config.options(section):
                    elem_list = option.split('/')
                    xml_item = xml_child
                    xpath = './'
# create any/all intermediate XML nodes in the xpath or merge with existing
                    for elem in elem_list:
                        xpath += "/%s" % elem
                        match = xml_root.find(xpath)
                        if match is not None:
                            xml_item = match
                            continue
                        xml_item = ET.SubElement(xml_item, elem)
# terminate at the final element to populate with the current option data
                    if xml_item is not None:
                        xml_item.text = config.get(section, option)
# DEBUG
#            self.mf_client.xml_print(xml_root)

# construct the command
            xml_command = 'asset.set :id %r' % asset_id
            for xml_child in xml_root:
                if xml_child.tag == 'asset':
                    for xml_arg in xml_child:
                        xml_command += self.xml_to_mf(xml_arg)
                else:
                    xml_command += ' :meta <%s >' % self.xml_to_mf(xml_child)

# DEBUG
#            print xml_command

# update the asset metadata
            self.mf_client.aterm_run(xml_command)
# re-analyze the content - stricly only needs to be done if type/ctype/lctype was changed
# NEW - don't do this by default - it will generate stacktrace in mediaflux for DMF (offline) files
#            self.mf_client.aterm_run("asset.reanalyze :id %r" % asset_id)

        except Exception as e:
            self.mf_client.log("WARNING", "Metadata population failed: %s" % str(e))

# ---
    def help_import(self):
        print "\nUpload files or folders with associated metadata"
        print "For every file <filename.ext> another file called <filename.ext.meta> should contain metadata in INI file format\n"
        print "Usage: import <file or folder>\n"

# ---
    def do_import(self, line):
        self.do_put(line, meta=True)
        return

# --
    def help_get(self):
        print "\nDownload remote files to the current local folder\n"
        print "Usage: get <remote files or folders>\n"

    def do_get(self, line):
# NB: use posixpath for mediaflux namespace manipulation
        line = self.absolute_remote_filepath(line)
# sanitise as asset.query is special
        double_escaped = self.escape_single_quotes(line)
# collapsed namespace
        namespace = posixpath.normpath(posixpath.dirname(double_escaped))
# possible download on asset/pattern
        basename = posixpath.basename(double_escaped)
# possible download on namespace
        candidate = posixpath.join(namespace, basename)

        self.mf_client.log("DEBUG", "do_get(): namespace=[%s] , asset_query=[%s] , candidate_namespace=[%s]" % (namespace, basename, candidate))

# this requires different escaping to an asset.query
        if self.mf_client.namespace_exists(line):
            base_query = "namespace>='%s'" % candidate
            base_namespace = posixpath.normpath(posixpath.join(line, ".."))
        else:
            base_query = "namespace='%s' and name='%s'" % (namespace, basename)
            base_namespace = posixpath.normpath(namespace)

# base_namespace shouldn't have escaping as it's used in direct path compares (not sent to mediaflux)
# base_query should have escaping as it is passed through mediaflux (asset.query etc)
        base_namespace = base_namespace.decode('string_escape')

# get content statistics and init for transfer polling loop
        stats = self.poll_total(base_query)
        self.mf_client.log("DEBUG", str(stats))
        if stats['total-bytes'] == 0:
            print "No data to download"
            return

        current = dict()
        done = dict()
        total_recv = 0
        start_time = time.time()
        elapsed_mins = 0

# we only expect to be able to download files where the content is in a known state
        bad_files = 0
        known_states = ["online-files", "online-bytes", "offline-files", "offline-bytes", "migrating-files", "migrating-bytes", "total-files", "total-bytes"]
        for key in stats.keys():
            if key not in known_states:
                self.mf_client.log("WARNING", "Content %s=%s" % (key, stats[key]))
                if "-files" in key:
                    bad_files += stats[key]
        todo = stats['total-files'] - bad_files

# start kick-off report for user
        user_msg = "Total files=%d" % stats['total-files']
        if bad_files > 0:
            user_msg += ", ignored files=%d" % bad_files

# feedback on files we're still waiting for
        unavailable_files = todo - stats['online-files']
        if unavailable_files > 0:
            user_msg += ", migrating files=%d, please be patient ...  " % unavailable_files
# migration can take a while (backgrounded) so print feedback first
            print user_msg
# recall all offline files
            xml_command = 'asset.query :where "%s and content offline" :action pipe :service -name asset.content.migrate < :destination "online" > &' % base_query
            self.mf_client.aterm_run(xml_command)
        else:
            user_msg += ", transferring ...  "
            self.print_over(user_msg)

# overall transfer loop
# TODO - time expired breakout?
        while todo > 0:
            try:
# wait (if required) and start transfers as soon as possible
                manager = None
                while manager is None:
                    online = self.get_online_set(base_query, base_namespace=base_namespace)
# FIXME - python 2.6 causes compile error on this -> which means the runtime print "you need version > 2.7" isn't displayed
#                     current = {k:v for k,v in online.iteritems() if k not in done}
# CURRENT - this seems to resolve the issue
                    current = dict([(k, v) for (k, v) in online.iteritems() if k not in done])

# is there something to transfer?
                    if len(current) == 0:
                        stats = self.poll_total(base_query)
                        current_pc = int(100.0 * total_recv / stats['total-bytes'])
                        msg = ""
                        if stats.get('offline-bytes') is not None:
                            msg += " offline=" + self.human_size(stats['offline-bytes'])
                        if stats.get('migrating-bytes') is not None:
                            msg += " migrating=" + self.human_size(stats['migrating-bytes'])
# TODO - even small migrations take a while ... make this something like 1,5,10,15 mins? (ie back-off)
                        for i in range(0, 4):
                            elapsed = time.time() - start_time
                            elapsed_mins = int(elapsed/60.0)
                            self.print_over("Progress=%d%%,%s, elapsed=%d mins ...  " % (current_pc, msg, elapsed_mins))
                            time.sleep(60)
                    else:
# CURRENT - test bad session handling
#                        self.mf_client.session = "abc"
                        manager = self.mf_client.get_managed(current.iteritems(), total_bytes=stats['total-bytes'], processes=self.transfer_processes)

# network transfer polling
                while manager is not None:
                    current_recv = total_recv + manager.bytes_recv()
                    current_pc = int(100.0 * current_recv / stats['total-bytes'])
                    self.print_over("Progress=%d%%, rate=%.1f MB/s  " % (current_pc, manager.byte_recv_rate()))
# update statistics after managed pool completes
                    if manager.is_done():
                        done.update(current)
                        todo = stats['total-files'] - bad_files - len(done)
                        total_recv += manager.bytes_recv()
                        break
                    time.sleep(2)

            except KeyboardInterrupt:
                self.mf_client.log("WARNING", "get interrupted by user")
                return

            except Exception as e:
                self.mf_client.log("ERROR", str(e))
                return

            finally:
                if manager is not None:
                    manager.cleanup()

# final report
        fail = 0
        for status, remote_ns, local_filepath in manager.summary:
            if status < 0:
                fail += 1
        if fail != 0:
            raise Exception("\nFailed to download %d file(s)." % fail)
        else:
            elapsed = time.time() - start_time
            average = stats['total-bytes'] / elapsed
            average = average / 1000000.0
            print "\nCompleted at %.1f MB/s" % average

# NB: for windows - total_recv will be 0 as we can't track (the no fork() shared memory variables BS)
# --
    def help_put(self):
        print "\nUpload local files or folders to the current folder on the remote server\n"
        print "Usage: put <file or folder>\n"

    def do_put(self, line, meta=False):
# build upload list pairs
        upload_list = []
        if os.path.isdir(line):
            self.print_over("Walking directory tree...")
            line = os.path.abspath(line)
            parent = os.path.normpath(os.path.join(line, ".."))
            for root, directory_list, name_list in os.walk(line):
                local_relpath = os.path.relpath(path=root, start=parent)
                relpath_list = local_relpath.split(os.sep)
                remote_relpath = "/".join(relpath_list)
                remote = posixpath.join(self.cwd, remote_relpath)

                if meta is False:
                    upload_list.extend([(remote, os.path.normpath(os.path.join(os.getcwd(), root, name))) for name in name_list])
                else:
                    for name in name_list:
                        if name.lower().endswith('.meta'):
                            pass
                        else:
                            upload_list.append((remote, os.path.normpath(os.path.join(os.getcwd(), root, name))))
        else:
            self.print_over("Building file list... ")
            for name in glob.glob(line):
                local_fullpath = os.path.abspath(name)
                if os.path.isfile(local_fullpath):
                    if meta is True:
                        if name.lower().endswith('.meta'):
                            pass
                    upload_list.append((self.cwd, local_fullpath))
# built, now upload
        self.managed_put(upload_list, meta)

# -- wrapper for monitoring an upload
    def managed_put(self, upload_list, meta=False):
        manager = self.mf_client.put_managed(upload_list, processes=self.transfer_processes)
        self.mf_client.log("DEBUG", "Starting transfer...")
        self.print_over("Total files=%d, transferring..." % len(upload_list))
        start_time = time.time()
        try:
            while True:
                if manager.bytes_total > 0:
                    progress = 100.0 * manager.bytes_sent() / float(manager.bytes_total)
                else:
                    progress = 0.0

                self.print_over("Progress: %3.0f%% at %.1f MB/s  " % (progress, manager.byte_sent_rate()))

                if manager.is_done():
                    break
# TODO - could use some of this time to populate metadata for successful uploads (if any)
                time.sleep(2)

        except KeyboardInterrupt:
            self.mf_client.log("WARNING", "put interrupted by user")
            return

        except Exception as e:
            self.mf_client.log("ERROR", str(e))
            return

        finally:
            if manager is not None:
                manager.cleanup()

# TODO - pop some of the metadata imports in the upload cycle if it helps the efficiency (measure!)
# TODO - or include it directly in the assset.set XML ...
        fail = 0
        for asset_id, remote_ns, local_filepath in manager.summary:
            if asset_id < 0:
                fail += 1
            else:
                if meta is True:
                    metadata_filename = local_filepath + ".meta"
                    self.import_metadata(asset_id, metadata_filename)

# final report
        if fail != 0:
            raise Exception("\nFailed to upload %d file(s)." % fail)
        else:
            elapsed = time.time() - start_time
            rate = manager.bytes_sent() / elapsed
            rate = rate / 1000000.0
            print "\nCompleted at %.1f MB/s" % rate

# --
    def help_cd(self):
        print "\nChange the current remote folder\n"
        print "Usage: cd <folder>\n"

    def do_cd(self, line):
        candidate = self.absolute_remote_filepath(line)
        if self.mf_client.namespace_exists(candidate):
            self.cwd = candidate
            print "Remote: %s" % self.cwd
        else:
            raise Exception(" Could not find remote folder: %s" % candidate)
# --
    def help_pwd(self):
        print "\nDisplay the current remote folder\n"
        print "Usage: pwd\n"

# use repr to help figure out issues such as invisible characters in folder names
    def do_pwd(self, line):
        print "Remote: %s" % repr(self.cwd)

# --
    def help_mkdir(self):
        print "\nCreate a remote folder\n"
        print "Usage: mkdir <folder>\n"

    def do_mkdir(self, line, silent=False):
        ns_target = self.absolute_remote_filepath(line)
        try:
            self.mf_client.aterm_run('asset.namespace.create :namespace "%s"' % ns_target.replace('"', '\\\"'))
        except Exception as e:
            if "already exists" in str(e):
                if silent is False:
                    print "Folder already exists: %s" % ns_target
                pass
            else:
                raise e

# --
    def help_rm(self):
        print "\nDelete remote file(s)\n"
        print "Usage: rm <file or pattern>\n"

    def do_rm(self, line):
# build query corresponding to input
        fullpath = self.absolute_remote_filepath(line)
        namespace = posixpath.dirname(fullpath)
        pattern = posixpath.basename(fullpath)
        base_query = "namespace='%s' and name='%s'" % (self.escape_single_quotes(namespace), self.escape_single_quotes(pattern))

# prepare - count matches
        result = self.mf_client.aterm_run('asset.query :where "%s" :action count' % base_query)
# confirm remove
        elem = result.find(".//value")
        count = int(elem.text)
        if count == 0:
            print "No match"
        else:
            if self.ask("Remove %d files: (y/n) " % count):
                self.mf_client.aterm_run('asset.query :where "%s" :action pipe :service -name asset.destroy' % base_query)
            else:
                print "Aborted"

# -- rmdir
    def help_rmdir(self):
        print "\nRemove a remote folder\n"
        print "Usage: rmdir <folder>\n"

# -- rmdir
    def do_rmdir(self, line):
        ns_target = self.absolute_remote_filepath(line)
        if self.mf_client.namespace_exists(ns_target):
            if self.ask("Remove folder: %s (y/n) " % ns_target):
                self.mf_client.aterm_run('asset.namespace.destroy :namespace "%s"' % ns_target.replace('"', '\\\"'))
            else:
                print "Aborted"
        else:
            raise Exception(" Could not find remote folder: %s" % ns_target)

# -- local commands
    def help_debug(self):
        print "\nTurn debugging output on/off\n"
        print "Usage: debug <value>\n"

    def do_debug(self, line):
        match = re.search(r"\d+", line)
        if match:
            self.mf_client.debug = int(match.group(0))
        elif "true" in line or "on" in line:
            self.mf_client.debug = 1
        elif "false" in line or "off" in line:
            self.mf_client.debug = 0
        print "Debug=%r" % self.mf_client.debug

# --
    def help_lpwd(self):
        print "\nDisplay local folder\n"
        print "Usage: lpwd\n"

    def do_lpwd(self, line):
        print "Local: %s" % os.getcwd()

# --
    def help_lcd(self):
        print "\nChange local folder\n"
        print "Usage: lcd <folder>\n"

    def do_lcd(self, line):
        os.chdir(line)
        print "Local: %s" % os.getcwd()

# --
    def help_lls(self):
        print "\nList contents of local folder\n"
        print "Usage: lls <folder>\n"

    def do_lls(self, line):
# convert to absolute path for consistency
        if not os.path.isabs(line):
            path = os.path.normpath(os.path.join(os.getcwd(), line))
        else:
            path = line

# get display folder and setup for a glob style listing
        if os.path.isdir(path) is True:
            display_path = path
            path = os.path.join(path, "*")
        else:
            display_path = os.path.dirname(path)

        print "Local folder: %s" % display_path

# glob these to allow wildcards
        for filename in glob.glob(path):
            if os.path.isdir(filename):
                head, tail = os.path.split(filename)
                print "[Folder] " + tail

        for filename in glob.glob(path):
            if os.path.isfile(filename):
                head, tail = os.path.split(filename)
                print "%s | %-s" % (self.human_size(os.path.getsize(filename)), tail)

# --- working example of PKI via mediaflux
#     def do_mls(self, line):
#         pkey = open('/Users/sean/.ssh/id_rsa', 'r').read()
#         reply = self.mf_client.aterm_run("secure.shell.execute :command ls :host magnus.pawsey.org.au :private-key < :name sean :key \"%s\" >" % pkey)
#         self.mf_client.xml_print(reply)

# --- helper
    def delegate_actor_expiry(self, name):
# NB: can't specify the actor as that requires blanket admin perms ...
        result = self.mf_client.aterm_run("secure.identity.token.describe")
        elem = result.find(".//identity/[actor='%s']/validity/to" % name)
        if elem is not None:
            return elem.text

        return "never"

# ---
    def help_whoami(self):
        print "\nReport the current authenticated user or delegate and associated roles\n"
        print "Usage: whoami\n"

    def do_whoami(self, line):
        result = self.mf_client.aterm_run("actor.self.describe")
# main identity
        for elem in result.iter('actor'):
            user_name = elem.attrib['name']
            user_type = elem.attrib['type']
            if 'identity' in user_type:
                expiry = self.delegate_actor_expiry(user_name)
                print "user = delegate (expires %s)" % expiry
            else:
                print "%s = %s" % (user_type, user_name)
# associated roles
        for elem in result.iter('role'):
            print "  role = %s" % elem.text

# ---
    def help_processes(self):
        print "\nSet the number of concurrent processes to use when transferring files."
        print "If no number is supplied, reports the current value."
        print "Usage: processes <number>\n"

    def do_processes(self, line):
        try:
            p = max(1, min(int(line), 16))
            self.transfer_processes = p
        except:
            pass
        print "Current number of processes: %r" % self.transfer_processes

# -- connection commands
    def help_logout(self):
        print "\nTerminate the current session to the server\n"
        print "Usage: logout\n"

    def do_logout(self, line):
        self.mf_client.logout()
        self.need_auth = True

# ---
    def help_login(self):
        print "\nInitiate login to the current remote server\n"
        print "Usage: login\n"

    def do_login(self, line):
        if self.interactive is False:
            raise Exception(" Manual login not permitted in scripts")
        self.mf_client.log("DEBUG", "Authentication domain [%s]" % self.mf_client.domain)
        user = raw_input("Username: ")
        password = getpass.getpass("Password: ")
        self.mf_client.login(user, password)
        self.need_auth = False
# save the authentication token
        self.config.set(self.config_name, 'session', self.mf_client.session)
        f = open(self.config_filepath, "w")
        self.config.write(f)
        f.close()

# --
    def help_delegate(self):
        print "\nCreate a credential, stored in your local home folder, for automatic authentication to the remote server."
        print "An optional argument can be supplied to set the lifetime, or off to destroy all your delegated credentials.\n"
        print "Usage: delegate <days/off>\n"

    def do_delegate(self, line):
# argument parse
        dt = delegate_default
        destroy_session = False
        if line:
            if line == "off":
                try:
                    self.mf_client.aterm_run("secure.identity.token.all.destroy")
                    self.mf_client.log("DEBUG", "Removed secure tokens from server")
# figure out the current session
                    reply = self.mf_client.aterm_run("actor.self.describe")
                    if 'identity' in reply.find(".//actor").attrib['type']:
                        destroy_session = True
                except:
# probably a bad session (eg generated from an expired token)
                    self.mf_client.log("DEBUG", "Failed to remove secure tokens from server")
                    destroy_session = True
# remove all auth info and update config
                use_token = False
                self.config.remove_option(self.config_name, 'token')
# if current session is delegate based - destroy it too
                if destroy_session:
                    self.config.remove_option(self.config_name, 'session')
                    self.need_auth = True
                f = open(self.config_filepath, "w")
                self.config.write(f)
                f.close()
                print "Delegate credentials removed."
                return
            else:
                try:
                    dt = max(min(float(line), delegate_max), delegate_min)
                except:
                    print "Bad delegate lifetime."
# lifetime setup
        d = datetime.datetime.now() + datetime.timedelta(days=dt)
        expiry = d.strftime("%d-%b-%Y %H:%M:%S")

# query current authenticated identity
        domain = None
        user = None
        name = None
        result = self.mf_client.aterm_run("actor.self.describe")
        elem = result.find(".//actor")
        if elem is not None:
            actor = elem.attrib['name']
            if ":" in actor:
                i = actor.find(":")
                domain = actor[0:i]
                user = actor[i+1:]
        if user is None or domain is None:
            raise Exception(" Delegate identity %r is not allowed to delegate" % actor)

# create secure token (delegate) and assign current authenticated identity to the token
        self.mf_client.log("DEBUG", "Attempting to delegate for: domain=%s, user=%s, until=%r" % (domain, user, expiry))
        result = self.mf_client.aterm_run('secure.identity.token.create :to "%s" :role -type user "%s" :role -type domain "%s" :min-token-length 16' % (expiry, actor, domain))
        print "Delegate valid until: " + expiry

        for elem in result.iter():
            if elem.tag == 'token':
# remove current session ID (real user)
                self.config.remove_option(self.config_name, 'session')
                self.config.set(self.config_name, 'token', elem.text)
                f = open(self.config_filepath, "w")
                self.config.write(f)
                f.close()

# -- helper: recursively get complete list of remote files under a given namespace
    def get_remote_set(self, remote_namespace):
        remote_files = set()
        prefix = len(remote_namespace)
        result = self.mf_client.aterm_run("asset.query :where \"namespace>='%s'\" :as iterator :action get-path" % remote_namespace)
        elem = result.find(".//iterator")
        iterator = elem.text
        iterate_size = 100
        iterate = True
        while iterate:
# get file list for this sub-set
            result = self.mf_client.aterm_run("asset.query.iterate :id %s :size %d" % (iterator, iterate_size))
            for elem in result.iter("path"):
                relpath = elem.text[prefix+1:]
                remote_files.add(relpath)
# check for completion - to avoid triggering a mediaflux exception on invalid iterator
            for elem in result.iter("iterated"):
                state = elem.get('complete')
                if "true" in state:
                    iterate = False

        return remote_files

# --- helper: create the namespace and any intermediate namespaces, if required
    def mkdir_helper(self, namespace):
        self.mf_client.log("DEBUG", "mkdir_helper(): %s" % namespace)
        if self.mf_client.namespace_exists(namespace) is True:
            return
        else:
            head, tail = posixpath.split(namespace)
            self.mkdir_helper(head)
            self.do_mkdir(namespace, silent=True)

# --- compare
    def help_compare(self):
        print "\nCompares a local and a remote folder and reports any differences"
        print "The local and remote folders must have the same name and appear in the current local and remote working directories"
        print "Usage: compare <folder>\n"

# --- compare
# NB: checksum compare is prohibitively expensive in general, so default to file size based comparison
    def do_compare(self, line, checksum=False, filesize=True):
        remote_fullpath = self.absolute_remote_filepath(line)

# check remote
        if self.mf_client.namespace_exists(remote_fullpath) is False:
            print "Could not find remote folder: %s" % remote_fullpath
            return

# no folder specified - compare local and remote working directories 
        if remote_fullpath == self.cwd:
            local_fullpath = os.getcwd()
        else:
            remote_basename = posixpath.basename(remote_fullpath)
            local_fullpath = os.path.join(os.getcwd(), remote_basename)
# check local
        if os.path.exists(local_fullpath) is False:
            print "Could not find local folder: %s" % local_fullpath
            return

# build remote set
        remote_files = set()
        print "Building remote file set under [%s] ..." % remote_fullpath
        remote_files = self.get_remote_set(remote_fullpath)

# build local set
        local_files = set()
        print "Building local file set under [%s] ..." % local_fullpath
        try:
            for (dirpath, dirnames, filenames) in os.walk(local_fullpath):
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    relpath = os.path.relpath(full_path, local_fullpath)
                    local_files.add(relpath)
        except Exception as e:
            self.mf_client.log("ERROR", str(e))

# starting summary
        print "Total remote files = %d" % len(remote_files)
        print "Total local files = %d" % len(local_files)

# remote only count
        count_pull = 0
        print "=== Remote server only ==="
        for item in remote_files - local_files:
            count_pull += 1
            print("%s" % item)
# report 
        count_push = 0
        print "=== Local filesystem only ==="
        for item in local_files - remote_files:
            print("%s" % item)
            count_push += 1

# for common files, report if there are differences
        print "=== Differing files ==="
        count_common = 0
        count_mismatch = 0
        for item in local_files & remote_files:
            remote_filepath = posixpath.join(remote_fullpath, item)
            remote_namespace = posixpath.dirname(remote_filepath)
            local_filepath = os.path.join(local_fullpath, item)
# checksum compare
            if checksum is True:
                local_crc32 = self.mf_client.get_local_checksum(local_filepath)
                remote_crc32 = None
                try:
                    result = self.mf_client.aterm_run('asset.get :id -only-if-exists true "path=%s" :xpath -ename crc32 content/csum' % remote_filepath)
                    elem = result.find(".//crc32")
                    remote_crc32 = int(elem.text, 16)
                except Exception as e:
                    self.mf_client.log("ERROR", "do_compare(): %s" % str(e))
# always report (warning) mismatched files
                if local_crc32 == remote_crc32:
                    count_common += 1
                else:
                    print("s: local crc32=%r, remote crc32=%r" % (item, local_crc32, remote_crc32))
                    count_mismatch += 1
# filesize compare 
            elif filesize is True:
                local_size = os.path.getsize(local_filepath)
                remote_size = 0
                try:
                    result = self.mf_client.aterm_run('asset.get :id -only-if-exists true "path=%s" :xpath -ename size content/size' % remote_filepath)
                    elem = result.find(".//size")
                    remote_size = int(elem.text)
                except Exception as e:
                    self.mf_client.log("ERROR", "do_compare(): %s" % str(e))
# always report (warning) mismatched files
                if local_size == remote_size:
                    count_common += 1
                else:
                    print("%s: local size=%d, remote size=%d" % (item, local_size, remote_size))
                    count_mismatch += 1
# existence compare
            else:
                count_common += 1

# concluding summary
        print "=== Complete ==="
        print "Files found only on local filesystem = %d" % count_push
        print "Files found only on remote server = %d" % count_pull
        print "Identical files = %d" % count_common
        if checksum is True or filesize is True:
            print "Differing files = %d" % count_mismatch

# -- generic operation that returns an unknown number of results from the server, so chunking must be used
    def mf_iter(self, iter_command, iter_callback, iter_size):
# NB: we expect cmd to have ":as iterator" in it
        result = self.mf_client.aterm_run(iter_command)
        elem = result.find(".//iterator")
        iter_id = elem.text
        while True:
            self.mf_client.log("DEBUG", "Online iterator chunk")
            result = self.mf_client.aterm_run("asset.query.iterate :id %s :size %d" % (iter_id, iter_size))
#  action the callback for this iteration
            iter_callback(result)
# if current iteration is flagged as completed -> we're done
            elem = result.find(".//iterated")
            state = elem.get('complete')
            if "true" in state:
                self.mf_client.log("DEBUG", "iteration completed")
                break

# -- callback for do_publish url printing 
    def print_published_urls(self, result):
        for elem in result.iter("path"):
            public_url = '%s://%s/download/%s' % (self.mf_client.protocol, self.mf_client.server, urllib2.quote(elem.text[10:]))
            print public_url

# --
    def help_publish(self):
        print "\nReturn a public, downloadable URL for a file or files\nRequires public sharing to be enabled by the project administrator\n"
        print "Usage: publish <file(s)>\n"

# TODO - publish/unpublish only work on assets ... rework to handle namespaces?
# --
    def do_publish(self, line):
        fullpath = self.absolute_remote_filepath(line)
        pattern = posixpath.basename(fullpath)
        namespace = posixpath.dirname(fullpath)
# publish everything that matches
        self.mf_client.aterm_run('asset.query :where "namespace=\'%s\' and name=\'%s\'" :action pipe :service -name asset.label.add < :label PUBLISHED >' % (namespace, pattern), background=True)
# iterate to display downloadable URLs
        iter_cmd = 'asset.query :where "namespace=\'%s\' and name=\'%s\'" :as iterator :action get-path' % (namespace, pattern)
        self.mf_iter(iter_cmd, self.print_published_urls, 10)

# --
    def help_unpublish(self):
        print "\nRemove public access for a file or files\n"
        print "Usage: unpublish <file(s)>\n"

# --
    def do_unpublish(self, line):
        fullpath = self.absolute_remote_filepath(line)
        pattern = posixpath.basename(fullpath)
        namespace = posixpath.dirname(fullpath)
# un-publish everything that matches
        self.mf_client.aterm_run('asset.query :where "namespace=\'%s\' and name=\'%s\'" :action pipe :service -name asset.label.remove < :label PUBLISHED >' % (namespace, pattern), background=True)

# --
    def help_quit(self):
        print "\nExit without terminating the session\n"
    def do_quit(self, line):
        exit(0)

# --
    def help_exit(self):
        print "\nExit without terminating the session\n"
    def do_exit(self, line):
        exit(0)

# --
    def loop_interactively(self):
        while True:
            try:
                self.cmdloop()
            except KeyboardInterrupt:
                print " Interrupted by user"

# NB: here's where all command failures are caught
            except SyntaxError:
                print " Syntax error: for more information on commands type 'help'"

            except Exception as e:
# exit on the EOF case ie where stdin/file is force fed via command line redirect
# FIXME - this can sometimes occur in some mediaflux error messages
                if "EOF" in str(e):
                    print "Exit: encountered EOF"
                    return
                print str(e)
# TODO - handle via custom exception ?
                if "session is not valid" in str(e):
                    self.need_auth = True

def main():
    global build

    if sys.hexversion < 0x02070000:
        print("Error: requires Python 2.7.x, using: ", sys.version)
        exit(-1)

# server config (section heading) to use
    p = argparse.ArgumentParser(description="pshell help")
    p.add_argument("-c", dest='current', default="pawsey", help="the config name in $HOME/.mf_config to connect to")
    p.add_argument("-i", dest='script', help="input text file containing commands")
    p.add_argument("-v", dest='verbose', default=None, help="set verbosity level (0,1,2)")

    p.add_argument("-u", dest='url', default=None, help="server URL (eg https://mediaflux.org:443")
    p.add_argument("-d", dest='domain', default=None, help="login authentication domain")
    p.add_argument("-s", dest='session', default=None, help="session")

    p.add_argument("command", nargs="?", default="", help="a single command to execute")
    args = p.parse_args()
    current = args.current
    script = args.script
    verbose = 0
    session = ""


    token = None
#    config_changed = False

# ascertain local path for storing the config, fallback to CWD if system gives a dud path for ~
    config_filepath = os.path.expanduser("~/.mf_config")
    try:
        open(config_filepath, 'a').close()
    except:
        config_filepath = os.path.join(os.getcwd(), ".mf_config")
# build config
    config = ConfigParser.ConfigParser()
    config.read(config_filepath)
# use config in ~ if it exists
    try:
        if config.has_section(current):
            pass
        else:
            try:
# config in zip bundle
                me = zipfile.ZipFile(os.path.dirname(__file__), 'r')
                f = me.open('.mf_config')
            except:
# config from pshell install directory (Windows fix)
                f = open(os.path.join(os.path.dirname(__file__), 'data', '.mf_config'))

# read non ~ config as defaults
            config.readfp(f)

# get main config vars
        server = config.get(current, 'server')
        protocol = config.get(current, 'protocol')
        port = config.get(current, 'port')
        domain = config.get(current, 'domain')
# no .mf_config in ~ or zip bundle or cwd => die
    except Exception as e:
        print "Failed to find a valid config file: %s" % str(e)
        exit(-1)

    if config.has_option(current, 'session'):
        session = config.get(current, 'session')
    if config.has_option(current, 'token'):
        token = config.get(current, 'token')

# extract terminal size for auto pagination
    try:
        import fcntl, termios, struct
        size = struct.unpack('hh', fcntl.ioctl(0, termios.TIOCGWINSZ, '1234'))
    except:
# FIXME - make this work with windows
        size = (80, 20)

# command line arguments override; but only if specified ie not none
    if args.url is not None:
        cmd = urlparse.urlparse(args.url)
        protocol = cmd.scheme
        server = cmd.hostname
        port = cmd.port
    if args.domain is not None:
        domain = args.domain
    if args.verbose is not None:
        verbose = args.verbose
    if args.session is not None:
        session = args.session

# establish mediaflux connection
    try:
        mf_client = mfclient.mf_client(protocol=protocol, server=server, port=port, domain=domain, debug=verbose)
        mf_client.session = session
        mf_client.token = token

    except Exception as e:
        print "Failed to connect to: %r://%r:%r" % (protocol, server, port)
        print "Error: %s" % str(e)
        exit(-1)

# auth test - will automatically attempt to use a token (if it exists) to re-generate a valid session
    need_auth = True
    if mf_client.authenticated():
        need_auth = False

# hand control of mediaflux client over to parsing loop
    my_parser = parser()
    my_parser.mf_client = mf_client
    my_parser.config_name = current
    my_parser.config_filepath = config_filepath
    my_parser.config = config
    my_parser.need_auth = need_auth
# just in case the terminal height calculation returns a very low value
    my_parser.terminal_height = max(size[0], my_parser.terminal_height)
# HACK - auto adjust process count based on network capability 
# the main issue is low capability drives being overstressed by too many random requests
# FIXME - ideally we'd sample rw io for disk and net to compute the sweet spot
    if mf_client.encrypted_data:
       my_parser.transfer_processes = 2
    else:
       my_parser.transfer_processes = 4

# TAB completion
# strange hackery required to get tab completion working under OS-X and also still be able to use the b key
# REF - http://stackoverflow.com/questions/7124035/in-python-shell-b-letter-does-not-work-what-the
    try:
        if 'libedit' in readline.__doc__:
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")
    except:
# FIXME - no readline in Windows ...
        mf_client.log("WARNING", "No readline module; tab completion unavailable")

# build non interactive input iterator
    input_list = []
    my_parser.interactive = True
    if script:
        input_list = itertools.chain(input_list, open(script))
        my_parser.interactive = False
# FIXME - stricly, need regex to avoid split on quote protected &&
    if len(args.command) != 0:
        input_list = itertools.chain(input_list, args.command.split("&&"))
        my_parser.interactive = False

# interactive or input iterator (scripted)
    mf_client.log("DEBUG", "PSHELL=%s" % build)
    if my_parser.interactive:
        print " === pshell: type 'help' for a list of commands ==="
        my_parser.loop_interactively()
    else:
        for item in input_list:
            line = item.strip()
            try:
                print "%s:%s> %s" % (current, my_parser.cwd, line)
                my_parser.onecmd(line)
            except KeyboardInterrupt:
                print " Interrupted by user"
                exit(-1)
            except SyntaxError:
                print " Syntax error: for more information on commands type 'help'"
                exit(-1)
            except Exception as e:
                print str(e)
                exit(-1)


if __name__ == '__main__':
    main()
