#!/usr/bin/env python

import os
import re
import cmd
import sys
import glob
import math
import time
# NEW - shell like pattern matching
import fnmatch
import getpass
import argparse
import datetime
import ConfigParser
import mfclient
import posixpath
# no readline on windows
try:
    import readline
except:
    pass

# standard lib python command line client for mediaflux
# Author: Sean Fleming

delegate_default = 7
delegate_min = 1
delegate_max = 365

# NB: handle exceptions at this level
class parser(cmd.Cmd):
    config = None
    config_name = None
    config_filepath = None
    mf_client = None
    cwd = '/projects'
    interactive = True
    need_auth = True
    intro = " === pshell: type 'help' for a list of commands ==="
    transfer_processes = 4
    terminal_height = 25

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

#         print "\ncn: partial [%s] : prefix = [%r] : pattern = [%r] : insertion_start=%r : xlat_offset=%r" % (partial_ns, prefix, pattern, start, xlat_offset)

# special case - we "know" .. is a namespace
        if pattern == "..":
            return [partial_ns[start:]+"/"]

# construct an absolute namespace (required for any remote lookups)
        if posixpath.isabs(partial_ns):
            target_ns = posixpath.normpath(partial_ns[:offset])
        else:
            target_ns = posixpath.normpath(posixpath.join(self.cwd, partial_ns[:offset]))

#         print "cn: target_ns: [%s]" % target_ns

# generate listing in target namespace for completion matches
        result = self.mf_client.run("asset.namespace.list", [("namespace", target_ns)])
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

        return ns_list


# CURRENT - testing helper 
#    def do_test(self, line):

# --- helper: attempt to complete an asset
    def complete_asset(self, partial_asset_path, start):

# construct an absolute namespace (required for any remote lookups)
        if posixpath.isabs(partial_asset_path):
            candidate_ns = posixpath.normpath(partial_asset_path)
        else:
            candidate_ns = posixpath.normpath(posixpath.join(self.cwd, partial_asset_path))

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

#         print "ca: target_ns: [%s] : pattern = %r : prefix = %r" % (target_ns, pattern, prefix)

        if pattern is not None:
            result = self.mf_client.run("asset.query", [("where", "namespace='%s' and name='%s*'" % (target_ns, pattern)), ("action", "get-values"), ("xpath ename=\"name\"", "name") ])
        else:
            result = self.mf_client.run("asset.query", [("where", "namespace='%s'" % target_ns), ("action", "get-values"), ("xpath ename=\"name\"", "name") ])

#         self.mf_client.xml_print(result)

        asset_list = []
        for elem in result.iter('name'):
            if elem.text is not None:
                asset_list.append(posixpath.join(prefix,elem.text))

#         print "ca: ", asset_list

        return asset_list

# NB: if the return result is ambigious (>1 option) it'll require 2 presses to get the list
# turn off DEBUG -> gets in the way of commandline completion
# NB: index offsets are 1 greater than the command under completion

# ---
    def complete_get(self, text, line, start_index, end_index):
        save_state = self.mf_client.debug
        self.mf_client.debug = False
        candidate_list = self.complete_asset(line[4:end_index], start_index-4)
        candidate_list += self.complete_namespace(line[4:end_index], start_index-4)
        self.mf_client.debug = save_state
        return candidate_list

# ---
# NB: taking the approach that rm is for files (assets) only and rmdir is for folders (namespaces)
    def complete_rm(self, text, line, start_index, end_index):
        save_state = self.mf_client.debug
        self.mf_client.debug = False
        candidate_list = self.complete_asset(line[3:end_index], start_index-3)
        return candidate_list

# ---
    def complete_file(self, text, line, start_index, end_index):
        save_state = self.mf_client.debug
        self.mf_client.debug = False
        candidate_list = self.complete_asset(line[5:end_index], start_index-5)
        candidate_list += self.complete_namespace(line[5:end_index], start_index-5)
        self.mf_client.debug = save_state
        return candidate_list

# ---
    def complete_ls(self, text, line, start_index, end_index):
        save_state = self.mf_client.debug
        self.mf_client.debug = False
        candidate_list = self.complete_namespace(line[3:end_index], start_index-3)
        candidate_list += self.complete_asset(line[3:end_index], start_index-3)
        self.mf_client.debug = save_state
        return candidate_list

# ---
    def complete_cd(self, text, line, start_index, end_index):
        save_state = self.mf_client.debug
        self.mf_client.debug = False
        ns_list = self.complete_namespace(line[3:end_index], start_index-3)
        self.mf_client.debug = save_state
        return ns_list

# ---
    def complete_mkdir(self, text, line, start_index, end_index):
        save_state = self.mf_client.debug
        self.mf_client.debug = False
        ns_list = self.complete_namespace(line[6:end_index], start_index-6)
        self.mf_client.debug = save_state
        return ns_list

# ---
    def complete_rmdir(self, text, line, start_index, end_index):
        save_state = self.mf_client.debug
        self.mf_client.debug = False
        ns_list = self.complete_namespace(line[6:end_index], start_index-6)
        self.mf_client.debug = save_state
        return ns_list

# ---
    def emptyline(self):
        return

# ---
    def default(self, line):
# unrecognized - assume it's an aterm command
        reply = self.mf_client._xml_aterm_run(line)
        self.mf_client.xml_print(reply)
        return

# --- helper
    def requires_auth(self, line):
        local_commands = ["login", "help", "lls", "lcd", "lpwd", "debug", "exit", "quit"]

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
# new - if script, assume you know what you're doing
        if self.interactive == False:
            return True
        response = raw_input(text)
        if response == 'y' or response == 'Y':
            return True
        return False

# --- helper: I think this is only required if passing self.cwd through an asset.query
    def safe_cwd(self):
        return(self.cwd.replace("'", "\\'"))

# CURRENT - asset.query with namespaces enclosed by ' - must have ' double escaped ... asset.namespace.exists namespaces - must be just single escaped 
# CURRENT - but asset.namespace.list should have no escaping ... thanks Arcitecta
    def escape_single_quotes(self, namespace):
        return(namespace.replace("'", "\\'"))

# --- helper: convert a relative/absolute mediaflux namespace/asset reference to minimal absolute form
    def absolute_remote_filepath(self, line):
        if not posixpath.isabs(line):
            line = posixpath.join(self.cwd, line)
        return posixpath.normpath(line)

# CURRENT - general method for retrieving an iterator for remote folders
    def remote_namespaces_iter(self, pattern):
        fullpath = self.absolute_remote_filepath(pattern)
        namespace = posixpath.dirname(fullpath)
        pattern = posixpath.basename(fullpath)

        if len(pattern) == 0:
            pattern = "*"

        result = self.mf_client.run("asset.namespace.list", [("namespace", namespace)])

        for elem in result.iter('namespace'):
            if elem.text is not None:
                if fnmatch.fnmatch(elem.text, pattern):
                    yield elem.text

# TODO - general method for retrieving an iterator for remote files
#    def remote_files_get(self, pattern):


# --- file info
    def help_file(self):
        print "\nReturn metadata information on a remote file\n"
        print "Usage: file <filename>\n"

    def do_file(self, line):
        result = self.mf_client.run("asset.get", [("id", "path=%s" % self.absolute_remote_filepath(line))]) 
        self.mf_client.xml_print(result)


# --- helper
# immediately return any key pressed as a character
    def wait_key(self):
#        import select

        result = None
        if self.interactive == False:
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
        print "Navigation in paginated output can be achieved by directly inputing page numbers or /<pattern> or q to quit.\n"
        print "Usage: ls <file pattern or folder name>\n"
        print "Examples: ls /projects/my project/some folder"
        print "          ls *.txt\n"

    def do_ls(self, line):
# page is done by pagination controller
        page = 1
# size is calculated from terminal minus 3 for command + header + footer 
        size = max(1, self.terminal_height - 3)
        asset_filter = None

        if len(line) == 0:
            cwd = self.cwd
        else:
# if absolute path exists as a namespace -> query this, else query via an asset pattern match
            cwd = self.absolute_remote_filepath(line)
            if not self.mf_client.namespace_exists(cwd):
                asset_filter = posixpath.basename(cwd)
                cwd = self.escape_single_quotes(posixpath.dirname(cwd))

#        print "Remote folder: %s" % cwd
# query attempt

        pagination_complete = False
        show_header = True

        while pagination_complete is False:

            pagination_footer = None

# FIXME - something like "ls -al" will give an ugly stack trace
# would prefer a message like "bad syntax - run help ls for assistance" 
            if asset_filter is not None:
                reply = self.mf_client.run("www.list", [("namespace", cwd), ("page", page), ("size", size), ("filter", asset_filter)])
            else:
                reply = self.mf_client.run("www.list", [("namespace", cwd), ("page", page), ("size", size)])

            for elem in reply.iter('parent'):
                for child in elem:
                    if child.tag == "name":
                        canonical_folder = child.text
                    if child.tag == "page":
                        canonical_page = int(child.text)
                    if child.tag == "last":
                        canonical_last = int(child.text)
                    if child.tag == "size":
                        canonical_size = int(child.text)
                    if child.tag == "assets":
                        canonical_assets = int(child.text)
                    if child.tag == "namespaces":
                        canonical_namespaces = int(child.text)

# print header 
# TODO - total files = ?, x files pp, folder:
            if show_header:
                print "%d items, %d items per page, remote folder: %s" % (canonical_assets+canonical_namespaces, canonical_size, canonical_folder)
                show_header = False

            pagination_footer = "Page %r of %r, file filter [%r]: " % (canonical_page, canonical_last, asset_filter)

# for each namespace
            for elem in reply.iter('namespace'):
                for child in elem:
                    if child.tag == "name":
                            print "[Folder] %s" % child.text
# for each asset
            for elem in reply.iter('asset'):
                state = "?"
                asset_id = "?"
                for child in elem:
# NEW - can't really avoid ID interaction in the long run I think ...
                    if child.tag == "id":
                        asset_id = child.text
                    if child.tag == "name":
                        filename = child.text
                    if child.tag == "size":
                        filesize = child.text
# FIXME - size overwrites the size for the www.list argument ...
# hmmm, asset with no content ... 
                        if filesize is None:
                            filesize = 0
                    if child.tag == "state":
                        if "online" in child.text:
                            filestate = "online  |"
                        else:
                            filestate = "%.9s |" % child.text
# file item
#                print "%s |%s%-s" % (self.human_size(int(filesize)), filestate, filename)
# TODO - tidy up
                print " %-10s | %s %s | %s" % (asset_id, filestate, self.human_size(int(filesize)), filename)

# if no pagination is required - we're done, unless a filter is active
            if canonical_last == 1 and asset_filter is None:
                break

# pagination controls
            response = self.pagination_controller(pagination_footer)
#            print "response = [%r]" % response
            if response is not None:
                try:
                    page = int(response)
                except:
                    if response == 'q' or response == 'quit':
                        pagination_complete = True
                        break
                    elif response.startswith("/"):
                        asset_filter = response[1:]
                        if len(asset_filter) == 0:
                            asset_filter = None
                        show_header = True
                        page = 1
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
        result = self.mf_client.run("asset.content.status.statistics", [("where", base_query)])
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
    def get_online_set(self, base_query):

        online = dict()
        list_local_path = {}

        query = [("where", base_query + " and content online"),("as","iterator"),("action","get-values"),("xpath ename=\"id\"","id"),("xpath ename=\"namespace\"","namespace"),("xpath ename=\"filename\"","name")]
        result = self.mf_client.run("asset.query", query)
#         self.mf_client.xml_print(result)

        elem = self.mf_client.xml_find(result, "iterator")
        iterator = elem.text
        iterate_size = 100

        iterate = True
        while iterate:
            self.mf_client.log("DEBUG", "Online iterator chunk")
# get file list for this sub-set
            result = self.mf_client.run("asset.query.iterate", [("id", iterator), ("size", iterate_size)])
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
                        remote_relpath = posixpath.relpath(path=namespace, start=self.cwd)
                        relpath_list = remote_relpath.split("/")
                        local_relpath = os.sep.join(relpath_list)
                        path = os.path.join(os.getcwd(), local_relpath)

# add valid download entry
                if asset_id is not None and filename is not None:
                    if path is None:
                        filepath = os.path.join(os.getcwd(), filename)
                    else:
                        filepath = os.path.join(path, filename)
                        list_local_path[path] = 1

                    online[asset_id] = filepath

# NEW - check for completion - to avoid triggering a mediaflux exception on invalid iterator
            for elem in result.iter("iterated"):
                state = elem.get('complete')
                if "true" in state:
                    self.mf_client.log("DEBUG", "Asset iteration completed")
                    iterate = False

# TODO - *** split out this from the online files call -> call ONCE on ALL files at the start, rather than polling
# create any required local dirs (NB: may get exception if they exist - hence the catch)
# FIXME - permission denied exception left to actual download ... better way to handle?
        for local_path in list_local_path:
            try:
                self.mf_client.log("DEBUG", "Creating local folder: %s" % local_path)
                os.makedirs(local_path)
            except Exception as e:
# TODO - this is too noisy currently as we're doing this more than we should, but unavoidable until the split out above *** is done
#                self.mf_client.log("DEBUG", "%s" % str(e))
                pass

# DEBUG - upload iterate sub-set of files
#        for asset_id, filepath in online.iteritems():
#            print "get [id=%r] => %r" % (asset_id, filepath)

        return online

# --
    def print_over(self, text):
# these clear to end of line codes don't work on windows
#        sys.stdout.write('\x1b[2K')
#        sys.stdout.write("\033[K")
# TODO - would be nice if there was an os independent way of clearing to the end of a line
        sys.stdout.write("\r"+text)
        sys.stdout.flush()

# CURRENT - meta populator
    def import_metadata(self, asset_id, filepath):
#        print "import_metadata() [%s] : [%s]" % (asset_id, filepath)
        try:
            config = ConfigParser.ConfigParser()
            config.read(filepath)
# section -> xmlns
# TODO - how to handle to special case of first class asset properties (eg WGS84 geoshapes)
            for section in config.sections():

                if "geoshape" in section:
#                    print "section = [%s]" % section
                    xml_command = 'asset.set :id %r' % asset_id
                    tmp = section.split('/')
                    for item in tmp:
                        xml_command += " :%s <" % item
                    for option in config.options(section):
                        xml_command += ' :%s %s' % (option, config.get(section, option))
                    for item in tmp:
                        xml_command += ' >'
                else:
#                    print "section = [%s]" % section
                    xml_command = 'asset.set :id %r :meta < :%s <' % (asset_id, section)
                    for option in config.options(section):
                        xml_command += ' :%s %s' % (option, config.get(section, option))
                    xml_command += ' > >'
# DEBUG
#                print "import_metadata(): [%s]" % xml_command
                reply = self.mf_client._xml_aterm_run(xml_command)

        except Exception as e:
            self.mf_client.log("WARNING", "Metadata population failed: %s" % str(e))

# TODO - allow customization of metadata extension file?
    def help_import(self):
        print "\nUpload files or folders with associated metadata"
        print "For every file called <filename.ext> a file called <filename.ext.meta> is treated as containing metadata\n"
        print "Usage: import <file or folder>\n"
        print "Examples: import myfile.jpg"
        print "          import myfolder/\n"

# --- 
    def do_import(self, line):

# build upload list pairs
        upload_list = []
        meta_list = []
        if os.path.isdir(line):
            self.print_over("Walking directory tree...")
# FIXME - handle input of '/'
            line = os.path.abspath(line)
            parent = os.path.normpath(os.path.join(line, ".."))
            for root, directory_list, name_list in os.walk(line):
# convert a local relative path - which could contain either windows or *nix path separators - to a remote path, which must be *nix style
                local_relpath = os.path.relpath(path=root, start=parent)
# split on LOCAL separator (whatever that may be) then join on remote *nix separator
                relpath_list = local_relpath.split(os.sep)
                remote_relpath = "/".join(relpath_list)
                remote = posixpath.join(self.cwd, remote_relpath)
                for name in name_list:
                    if name.lower().endswith('.meta'):
                        meta_list.append((remote, os.path.normpath(os.path.join(os.getcwd(), root, name)))) 
                    else:
                        upload_list.append((remote, os.path.normpath(os.path.join(os.getcwd(), root, name)))) 
        else:
            self.print_over("Building file list... ")
            for name in glob.glob(line):
                if name.lower().endswith('.meta'):
                    meta_list.append((self.cwd, os.path.join(os.getcwd(), name)))
                else:
                    upload_list.append((self.cwd, os.path.join(os.getcwd(), name)))

# TODO  - for all files in meta_list - strip .xml extension and if NOT in upload_list -> treat as normal upload, else -> it's metadata
# DEBUG - window's path 
#        for dest,src in upload_list:
#            print "import: %s -> %s" % (src, dest)
#        for dest,src in meta_list:
#            print "meta: %s -> %s" % (src, dest)

# TODO - duplicate code from do_put()
        start_time = time.time()
        manager = self.mf_client.put_managed(upload_list, processes=self.transfer_processes)
        self.mf_client.log("DEBUG", "Starting transfer...")
        self.print_over("Total files=%d" % len(upload_list))
        print ", transferring...  "
        try:
            while True:
                if manager.bytes_total > 0:
                    progress = 100.0 * manager.bytes_sent() / float(manager.bytes_total)
                else:
                    progress = 0.0

                self.print_over("Progress: %3.0f%% at %.1f MB/s  " % (progress, manager.byte_sent_rate()))

                if manager.is_done():
                    break
                time.sleep(2)
# TODO - use this sleep time to populate metadata (if any?)
# ie manager.summary -> list of completed ... remove from meta_list after update
#done =  (26730, '/projects/Data Team/test', '/Users/sean/dev/mfclient/test/script')
# use last item in summary list? ie extend() is being used so treat it like a stack
# can pop() from the list but this means it won't be available for summary info
#                for item in manager.summary:
#                    print "done = ", item
                    # id , filepath to XML

        except KeyboardInterrupt:
            self.mf_client.log("WARNING", "put interrupted by user")
            return

        except Exception as e:
            self.mf_client.log("ERROR", str(e))
            return

        finally:
            if manager is not None:
                manager.cleanup()

# final summary
# TODO - include info on failures?
        self.print_over("Uploaded files=%d" % len(upload_list))
        elapsed = max(1.0, time.time() - start_time)
        rate = manager.bytes_sent() / (1000000*elapsed)
        print ", average rate=%.1f MB/s  " % rate

# TODO - pop some of these in the upload cycle if it helps the efficiency (measure!)
# TODO - customize extension to use?
        for item in manager.summary:
            metadata_filename = item[2] + ".meta"
            self.import_metadata(item[0], metadata_filename)


# --
    def help_get(self):
        print "\nDownload remote files to the current local folder\n"
        print "Usage: get <remote files or folders>\n"
        print "Examples: get /projects/My Project/images"
        print "          get *.txt\n"

    def do_get(self, line):
        list_asset_filepath = []
        total_bytes = 0

# FIXME - will fail for things like get Data Team/sean or get Data Team/sean/*.zip -> need to do some unix style path analysis 1st ...
# prefix with CWD -> then unix extract path and basename
# NB: use posixpath for mediaflux namespace manipulation
        if not posixpath.isabs(line):
            line = posixpath.join(self.cwd, line)

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
        else:
            base_query = "namespace='%s' and name='%s'" % (namespace, basename)

# get content statistics and init for transfer polling loop
        stats = self.poll_total(base_query)
        self.mf_client.log("DEBUG", str(stats))
        if stats['total-bytes'] == 0:
            print "No data to download"
            return

        current = dict()
        done = dict()
        complete = False
        total_recv = 0
        start_time = time.time()
        dmf_elapsed_mins = 0
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
# recall all offline files 
            xml_command = 'asset.query :where "%s and content offline" :action pipe :service -name asset.content.migrate < :destination "online" >' % base_query
            self.mf_client._xml_aterm_run(xml_command)
        else:
            user_msg += ", transferring ...  "

        print user_msg

# overall transfer loop 
# TODO - time expired breakout?
        while todo > 0:
            try:

# wait (if required) and start transfers as soon as possible
                manager = None
                while manager is None:
                    online = self.get_online_set(base_query)
# FIXME - python 2.6 causes compile error on this -> which means the runtime print "you need version > 2.7" isn't displayed
#                     current = {k:v for k,v in online.iteritems() if k not in done}
# CURRENT - this seems to resolve the issue
                    current = dict([(k,v) for (k,v) in online.iteritems() if k not in done])

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
                        for i in range(0,4):
                            elapsed = time.time() - start_time
                            elapsed_mins = int(elapsed/60.0)
                            self.print_over("Progress=%d%%,%s, elapsed=%d mins ...  " % (current_pc, msg,elapsed_mins))
                            time.sleep(60)
                    else:
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

# CURRENT - enforce cleanup, see if helps KZ issues
            finally:
                if manager is not None:
                    manager.cleanup()

# NB: for windows - total_recv will be 0 as we can't track (the no fork() shared memory variables BS)
        self.print_over("Downloaded files=%d" % len(done))
        elapsed = max(1.0, time.time() - start_time)
        rate = total_recv / (1000000.0*elapsed)
        print ", average rate=%.1f MB/s  " % rate
        return

# --
    def help_put(self):
        print "\nUpload local files or folders to the current folder on the remote server\n"
        print "Usage: put <file or folder>\n"
        print "Examples: put /home/sean/*.jpg"
        print "          put /home/sean/myfolder/\n"

    def do_put(self, line):
# build upload list pairs
        upload_list = []
        if os.path.isdir(line):
            self.print_over("Walking directory tree...")
# FIXME - handle input of '/'
            line = os.path.abspath(line)
            parent = os.path.normpath(os.path.join(line, ".."))
            for root, directory_list, name_list in os.walk(line):
# convert a local relative path - which could contain either windows or *nix path separators - to a remote path, which must be *nix style
                local_relpath = os.path.relpath(path=root, start=parent)
# split on LOCAL separator (whatever that may be) then join on remote *nix separator
                relpath_list = local_relpath.split(os.sep)
                remote_relpath = "/".join(relpath_list)
                remote = posixpath.join(self.cwd, remote_relpath)
                upload_list.extend( [(remote , os.path.normpath(os.path.join(os.getcwd(), root, name))) for name in name_list] )
        else:
            self.print_over("Building file list... ")
            upload_list = [(self.cwd, os.path.join(os.getcwd(), filename)) for filename in glob.glob(line)]

# DEBUG - window's path 
#        for dest,src in upload_list:
#            print "put: %s -> %s" % (src, dest)

        start_time = time.time()
        manager = self.mf_client.put_managed(upload_list, processes=self.transfer_processes)
        self.mf_client.log("DEBUG", "Starting transfer...")
        self.print_over("Total files=%d" % len(upload_list))
        print ", transferring...  "
        try:
            while True:
                if manager.bytes_total > 0:
                    progress = 100.0 * manager.bytes_sent() / float(manager.bytes_total)
                else:
                    progress = 0.0

                self.print_over("Progress: %3.0f%% at %.1f MB/s  " % (progress, manager.byte_sent_rate()))

                if manager.is_done():
                    break
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

# final summary
# TODO - include info on failures?
        self.print_over("Uploaded files=%d" % len(upload_list))
        elapsed = max(1.0, time.time() - start_time)
        rate = manager.bytes_sent() / (1000000*elapsed)
        print ", average rate=%.1f MB/s  " % rate

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
            print "Invalid remote folder: %s" % candidate

# --
    def help_pwd(self):
        print "\nDisplay the current remote folder\n"
        print "Usage: pwd\n"

    def do_pwd(self, line):
        print "Remote: %s" % self.cwd

# --
    def help_mkdir(self):
        print "\nCreate a remote folder\n"
        print "Usage: mkdir <folder>\n"

    def do_mkdir(self, line):
        ns_target = self.absolute_remote_filepath(line)
        try:
            self.mf_client.run("asset.namespace.create", [("namespace", ns_target)])
        except Exception as e:
# don't raise an exception if the namespace already exists - just warn
            if "already exists" in str(e):
                print "Warning: %s" % str(e)
            else:
# other errors (no permission, etc) should still raise an exception - failure
                raise Exception(e)

# --
    def help_rm(self):
        print "\nDelete remote file(s)\n"
        print "Usage: rm <file or pattern>\n"
        print "Examples: rm *.jpg"
        print "          rm /projects/myproject/somefile\n"

    def do_rm(self, line):
# build query corresponding to input
        fullpath = self.absolute_remote_filepath(line)
        namespace = posixpath.dirname(fullpath)
        pattern = posixpath.basename(fullpath)
        base_query = "namespace='%s' and name='%s'" % (self.escape_single_quotes(namespace), self.escape_single_quotes(pattern))

# count
        try:
            result = self.mf_client._xml_aterm_run("asset.query :where %s :action count" % base_query)
        except Exception as e:
            print str(e)
            return

# confirm remove
# not sure why this find doesn't work
#         elem = result.find("value")
        for elem in result.iter():
            if elem.tag == "value":
                count = int(elem.text)
                if count == 0:
                    print "No match"
                    return
                if self.ask("Remove %d files: (y/n) " % count):
                    self.mf_client._xml_aterm_run("asset.query :where %s :action pipe :service -name asset.destroy" % base_query)
                else:
                    print "Aborted"
                return

# --
    def help_rmdir(self):
        print "\nRemove a remote folder\n"
        print "Usage: rmdir <folder>\n"

    def do_rmdir(self, line):
        ns_target = self.absolute_remote_filepath(line)
        if self.mf_client.namespace_exists(ns_target):
            if self.ask("Remove folder: %s (y/n) " % ns_target):
                self.mf_client.run("asset.namespace.destroy", [("namespace", ns_target)])
            else:
                print "Aborted"
        else:
            print "No such folder: %s" % ns_target

# -- local commands
    def help_debug(self):
        print "\nTurn debugging output on/off\n"
        print "Usage: debug <on/off>\n"

    def do_debug(self, line):
        if "true" in line or "on" in line:
            print "Turning DEBUG on"
            self.mf_client.debug = True
        else:
            print "Turning DEBUG off"
            self.mf_client.debug = False

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

# no flags???
#         line = re.sub(r'-\S+', '', line)

# convert to absolute path for consistency
        if not os.path.isabs(line):
            path = os.path.normpath(os.path.join(os.getcwd(), line))
        else:
            path = line

# get display folder and setup for a glob style listing
        if os.path.isdir(path) == True:
            display_path = path 
            path = os.path.join(path, "*")
        else:
            display_path = os.path.dirname(path)

        print "Local folder: %s" % display_path

# NEW - glob these to allow wildcards
        for filename in glob.glob(path):
            if os.path.isdir(filename):
                head,tail = os.path.split(filename)
                print "[Folder] " + tail

        for filename in glob.glob(path):
            if os.path.isfile(filename):
                head,tail = os.path.split(filename)
                print "%s | %-s" % (self.human_size(os.path.getsize(filename)), tail)

# --- working example of PKI via mediaflux
#     def do_mls(self, line):
#         pkey = open('/Users/sean/.ssh/id_rsa', 'r').read()
#         reply = self.mf_client._xml_aterm_run("secure.shell.execute :command ls :host magnus.pawsey.org.au :private-key < :name sean :key \"%s\" >" % pkey)
#         self.mf_client.xml_print(reply)

# --- 
    def help_whoami(self):
        print "\nReport the current authenticated user or delegate and associated roles\n"
        print "Usage: whoami\n"

    def do_whoami(self, line):
        try:
            result = self.mf_client.run("actor.self.describe")
            for elem in result.iter('actor'):
                name = elem.attrib['name']
                if ":" in name:
                    print "actor = %s" % name
                else:
                    print "actor = delegate"
            for elem in result.iter('role'):
                print "  role = %s" % elem.text
        except:
            print "I'm not sure who you are!"

# --- 
    def help_processes(self):
        print ("\nSet the number of concurrent processes to use when transferring files.")
        print ("If no number is supplied, reports the current value.")
        print ("Usage: processes <number>\n")

    def do_processes(self, line):
        try:
            p = max(1, min(int(line), 16))
            self.transfer_processes = p
        except:
            pass
        print("Current number of processes: %r" % self.transfer_processes)

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
        user = raw_input("Username: ")
# NB: special cases
        if user == "manager":
            domain = 'system'
        elif user == "public":
            domain = 'public'
        else:
            domain = 'ivec'
        password = getpass.getpass("Password: ")

        try:
            self.mf_client.login(domain, user, password)
            self.need_auth = False
# save the authentication token
#             print "Writing session to config file: %s" % self.config_filepath
            self.config.set(self.config_name, 'session', self.mf_client.session)
            f = open(self.config_filepath, "w")
            self.config.write(f)
            f.close()

        except Exception as e:
            print str(e)

# --
    def help_delegate(self):
        print "\nCreate a credential, stored in your local home folder, for automatic authentication to the remote server."
        print "An optional argument can be supplied to set the lifetime, or off to destroy all your delegated credentials.\n"
        print "Usage: delegate <days/off>\n"
        print "Examples: delegate"
        print "          delegate 7"
        print "          delegate off\n"

    def do_delegate(self, line):
# argument parse
        dt = delegate_default
        if line:
            if line == "off":
                try:
#                     self.mf_client.run("secure.identity.token.destroy.all")
# wtf arcitecta - we just rename these things on a whim?
                    self.mf_client.run("secure.identity.token.all.destroy")
                    print "Delegate credentials removed."
                except:
                    print "No delegate credentials found."
                use_token = False
# remove all auth info and update config
                self.config.remove_option(self.config_name, 'token')
                self.config.remove_option(self.config_name, 'session')
                f = open(self.config_filepath, "w")
                self.config.write(f)
                f.close()
                self.need_auth = True
                return
            else:
                try:
                    dt = max(min(float(line), delegate_max), delegate_min)
                except:
                    print "Bad delegate lifetime."
# lifetime setup
        d = datetime.datetime.now() + datetime.timedelta(days=dt)
        expiry = d.strftime("%d-%b-%Y %H:%M:%S")
        print "Delegating until: " + expiry
# query current authenticated identity
        try:
            result = self.mf_client.run("actor.self.describe")
            for elem in result.iter():
                if elem.tag == 'actor':
                    actor = elem.attrib.get('name', elem.text)
                    i = actor.find(":")
                    domain = actor[0:i]
                    user = actor[i+1:]
        except:
            raise Exception("Failed to get valid identity")

# create secure token (delegate) and assign current authenticated identity to the token
        result = self.mf_client.run("secure.identity.token.create", [ ("to", expiry), ("role type=\"user\"", actor), ("role type=\"domain\"", domain), ("min-token-length", 16) ])
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
        base_query = "namespace >='%s'" % remote_namespace
        query = [("where", base_query),("as","iterator"),("action","get-path")]
        result = self.mf_client.run("asset.query", query)
        elem = self.mf_client.xml_find(result, "iterator")
        iterator = elem.text
        iterate_size = 100
        iterate = True
        while iterate:
            self.mf_client.log("DEBUG", "Remote iterator chunk")
# get file list for this sub-set
            result = self.mf_client.run("asset.query.iterate", [("id", iterator), ("size", iterate_size)])

            for elem in result.iter("path"):
                relpath = elem.text[prefix+1:]
                remote_files.add(relpath)

# check for completion - to avoid triggering a mediaflux exception on invalid iterator
            for elem in result.iter("iterated"):
                state = elem.get('complete')
                if "true" in state:
                    self.mf_client.log("DEBUG", "Asset iteration completed")
                    iterate = False

        return remote_files

# ---
    def help_compare(self):
        print "\nCompares a local and a remote folder and reports any differences"
        print "The local and remote folders must have the same name and appear in the current local and remote working directories"
        print "Usage: compare <folder>\n"
        print "Examples: compare mystuff\n"

# compare folder tree structure ... how?
    def do_compare(self, line):
        remote_fullpath = self.absolute_remote_filepath(line)
        if self.mf_client.namespace_exists(remote_fullpath) is False:
            print "Could not find remote folder: %s" % remote_fullpath
            return
        remote_basename = posixpath.basename(remote_fullpath)
        local_fullpath = os.path.join(os.getcwd(), remote_basename)
        if os.path.exists(local_fullpath) is False:
            print "Could not find local folder: %s" % local_fullpath
            return

        print "=== Compare start ==="

        local_files = set()
        remote_files = set()

# build remote files
        print "Building remote file set under [%s] ..." % remote_fullpath
        remote_files = self.get_remote_set(remote_fullpath)
        print "Total remote files = %d" % len(remote_files)

# build local files
        print "Building local file set under [%s] ..." % local_fullpath
        try:
            for (dirpath, dirnames, filenames) in os.walk(local_fullpath):
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    relpath = os.path.relpath(full_path, local_fullpath)
                    local_files.add(relpath)

            print "Total local files = %d" % len(local_files)

        except Exception as e:
            print "Error: %s" % str(e)

        print "=== Remote files with no local match ==="
        for item in remote_files - local_files:
            print "%s" % item

        print "=== Local files with no remote match ==="
        for item in local_files - remote_files:
            print "%s" % item

# TODO - checksum compares as well?

        print "=== Compare complete ==="


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
                print "Interrupted, cleaning up   "
                continue

            except Exception as e:
# NEW - handle EOF case where stdin is force fed via command line
                if "EOF" in str(e):
                    return

                print str(e)

def main():

# CURRENT - can include additional data files in the zip bundle (eg CA certs) 
#     import zipfile
#     me = zipfile.ZipFile(os.path.dirname(__file__), 'r')
#     f = me.open('certificate.pem')
#     print f.read()

# TODO - probably should make it compatible with 3.x as well (sigh)
    if sys.hexversion < 0x02070000:
        print("ERROR: requires Python 2.7.x, using: ", sys.version)
        exit(-1)

# server config (section heading) to use
    p = argparse.ArgumentParser(description='pshell help')
    p.add_argument('-c', dest='config', default='pawsey', help='The server in $HOME/.mf_config to connect to')
    p.add_argument('-i', dest='script', help='Input script file containing commands')
    p.add_argument("-d", dest='debug', help="Turn debugging on", action="store_true")

    args = p.parse_args()
    current = args.config
    script = args.script

# use config if exists, else create a dummy one
    config = ConfigParser.ConfigParser()
# hydrographic NAS box gives a dud path for ~
# NEW - test readwrite and if fail -> use CWD
    config_filepath = os.path.expanduser("~/.mf_config")
    try:
        open(config_filepath, 'a').close()
    except:
        print "Bad home [%s] ... falling back to current folder" % config_filepath
        config_filepath = os.path.join(os.getcwd(), ".mf_config")

    config.read(config_filepath)

    encrypt = True
    debug = False
    session = ""
    token = None
    config_changed = False

    if config.has_section(current):
#         print "Reading config [%s]" % config_filepath
        try:
            server = config.get(current, 'server')
            protocol = config.get(current, 'protocol')
            port = config.get(current, 'port')
        except:
            print "ERROR: config file [%s] has insufficiently specified server" % config_filepath
            exit(-1)

        if config.has_option(current, 'encrypt'):
            encrypt = config.getboolean(current, 'encrypt')
        if config.has_option(current, 'debug'):
            debug = config.getboolean(current, 'debug')
        if config.has_option(current, 'session'):
            session = config.get(current, 'session')
        if config.has_option(current, 'token'):
            token = config.get(current, 'token')
    else:
        if current != 'pawsey':
            print "Server configuration for %s not found in %s" % (current, config_filepath)
            exit(-1)

        print "Creating default config [%s]" % config_filepath
        config.add_section(current)
        server = "data.pawsey.org.au"
        protocol = "https"
        port = 443
        config.set(current, 'server', server)
        config.set(current, 'protocol', protocol)
        config.set(current, 'port', port)
        config_changed = True

# new - commandline debug true overrides config
    if args.debug:
        debug = True


# CURRENT - extract size - use this for auto pagination
# won't work for windows (of course)
# TODO - make this work with windows
    try:
        import fcntl, termios, struct  
        size = struct.unpack('hh', fcntl.ioctl(0, termios.TIOCGWINSZ, '1234'))
    except:
        print "Warning: couldn't determine terminal size"
        size = (80,25)


# mediaflux client
    try:
        mf_client = mfclient.mf_client(protocol=protocol, port=port, server=server, session=session, enforce_encrypted_login=encrypt, debug=debug)
    except Exception as e:
        print "Failed to establish network connection to: %s" % current
        print "Error: %s" % str(e)
        exit(-1)

# check session first
    need_auth = True
    if not len(session) == 0:
        if not mf_client.authenticated():
            session = ""
            config.set(current, 'session', session)
            config_changed = True
        else:
            need_auth = False

# missing or invalid session - check the token (if any)
    if len(session) == 0:
        if token:
            try:
                mf_client.login(token=token)
                config.set(current, 'session', mf_client.session)
                config_changed = True
                need_auth = False
                mf_client.log("DEBUG", "Delegate is valid")
            except Exception as e:
                mf_client.log("WARNING", "Delegate authentication failed.")
                mf_client.log("DEBUG", str(e))

# update config to match current state
    if config_changed:
        mf_client.log("DEBUG", "Writing config...")
        f = open(config_filepath, "w")
        config.write(f)
        f.close()

# hand control of mediaflux client over to parsing loop
    my_parser = parser()
    my_parser.mf_client = mf_client
    my_parser.config_name = current
    my_parser.config_filepath = config_filepath
    my_parser.config = config
    my_parser.need_auth = need_auth
    # NEW
    my_parser.terminal_height = size[0]

# TAB completion
# FIXME - no readline in Windows ...
# strange hackery required to get tab completion working under OS-X and also still be able to use the b key
# REF - http://stackoverflow.com/questions/7124035/in-python-shell-b-letter-does-not-work-what-the
    try:
        if 'libedit' in readline.__doc__:
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")
    except:
        mf_client.log("WARNING", "No readline module; tab completion unavailable")

# process script or go interactive
    if script:
        my_parser.interactive = False
        with open(script) as f:
            for line in f:
                try:
                    print "input> %s" % line
                    my_parser.onecmd(line)
                except Exception as e:
                    print str(e)
                    exit(-1)
    else:
        my_parser.loop_interactively()


if __name__ == '__main__':
    main()
