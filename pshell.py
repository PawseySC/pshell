#!/usr/bin/env python3

import sys
# magnus python 3.4 seems broken, module python/3.6.3 works fine
VERSION_MIN = (3, 6)
if sys.version_info < VERSION_MIN:
    sys.exit("ERROR: Python >= %d.%d is required, your version = %d.%d\n" % (VERSION_MIN[0], VERSION_MIN[1], sys.version_info[0], sys.version_info[1]))
import os
import re
import cmd
import glob
import math
import time
import json
import shlex
import urllib
import logging
import argparse
import itertools
import posixpath
import threading
import configparser
import concurrent.futures
import xml.etree.ElementTree as ET
from remote import client
import mfclient
import keystone
import s3client
# no readline on windows
try:
    import readline
except:
    pass

# standard lib python command line client for mediaflux
# Author: Sean Fleming
build= "Latest"

#------------------------------------------------------------
# picklable get()
def jump_get(remote, remote_fullpath, local_fullpath):
    try:
        size = remote.get(remote_fullpath, local_filepath=local_fullpath)
        logging.info("Local file size (bytes) = %r" % size)
    except Exception as e:
        logging.error(str(e))
        size = 0
    return size

#------------------------------------------------------------
# picklable put()
def jump_put(remote, remote_fullpath, local_fullpath):
    try:
        asset_id = remote.put(remote_fullpath, local_fullpath)
        size = os.path.getsize(local_fullpath)
    except Exception as e:
        logging.error(str(e))
        size = 0
    return size

#------------------------------------------------------------
class parser(cmd.Cmd):
    config = None
    config_name = None
    config_filepath = None
    keystone = None
    remotes = {}
    cwd = '/projects'
    interactive = True
    terminal_height = 20
    script_output = None

# TODO - class? or just a dict?
    thread_executor = None
    thread_max = 4
    get_count = 0
    get_bytes = 0
    total_count = 0
    total_bytes = 0
    put_count = 0
    put_bytes = 0


# --- initial setup of prompt
    def preloop(self):
        self.prompt = "pshell:%s>" % self.cwd

# --- not logged in -> don't even attempt to process remote commands
    def precmd(self, line):
        return cmd.Cmd.precmd(self, line)

# --- prompt refresh 
    def postcmd(self, stop, line):
        self.prompt = "pshell:%s>" % self.cwd
        return cmd.Cmd.postcmd(self, stop, line)

# TODO - rename asset->file/object, namespace->folder
# TODO - s3 implementation ... ugh
# ---
    def complete_get(self, text, line, start_index, end_index):
        remote = self.remotes_get(line)
        candidate_list = remote.complete_asset(line[4:end_index], start_index-4)
        candidate_list += remote.complete_namespace(line[4:end_index], start_index-4)
        return candidate_list

# ---
    def complete_rm(self, text, line, start_index, end_index):
        remote = self.remotes_get(line)
        candidate_list = remote.complete_asset(line[3:end_index], start_index-3)
        candidate_list += remote.complete_namespace(line[3:end_index], start_index-3)
        return candidate_list

# ---
    def complete_file(self, text, line, start_index, end_index):
        remote = self.remotes_get(line)
        candidate_list = remote.complete_asset(line[5:end_index], start_index-5)
        candidate_list += remote.complete_namespace(line[5:end_index], start_index-5)
        return candidate_list

# ---
    def complete_publish(self, text, line, start_index, end_index):
        remote = self.remotes_get(line)
        candidate_list = remote.complete_asset(line[8:end_index], start_index-8)
        candidate_list += remote.complete_namespace(line[8:end_index], start_index-8)
        return candidate_list

# ---
    def complete_ls(self, text, line, start_index, end_index):
        remote = self.remotes_get(line)
        candidate_list = remote.complete_asset(line[3:end_index], start_index-3)
        candidate_list += remote.complete_namespace(line[3:end_index], start_index-3)
        return candidate_list

# ---
    def complete_cd(self, text, line, start_index, end_index):
        remote = self.remotes_get(line)
        ns_list = remote.complete_namespace(line[3:end_index], start_index-3)
        return ns_list

# ---
    def complete_rmdir(self, text, line, start_index, end_index):
        remote = self.remotes_get(line)
        ns_list = remote.complete_namespace(line[6:end_index], start_index-6)
        return ns_list

# ---
    def emptyline(self):
        return

# ---
    def default(self, line):
        remote = self.remotes_get(self.cwd)
        remote.command(line)

#------------------------------------------------------------
    def remotes_config_save(self):
        endpoints = json.loads(self.config.get(self.config_name, 'endpoints'))
# TODO - only if refresh=True
        for mount, endpoint in endpoints.items():
            client = self.remotes_get(mount)
            logging.debug("updating mount=[%s] using client=[%r]" % (mount, client))
# refresh the config endpoint via the client
            if client is not None:
                endpoints[mount] = client.endpoint()
        self.config[self.config_name]['endpoints'] = json.dumps(endpoints)
# commit
        logging.info("saving config to file = %s" % self.config_filepath)
        with open(self.config_filepath, 'w') as f:
            self.config.write(f)

#------------------------------------------------------------
    def remotes_add(self, mount='/remote', module=None):
        logging.info("mount = [%s], module = [%r]" % (mount, module))
        self.remotes[mount] = module

# init cwd for parser and remote
        if module.connect() is True:
            self.cwd = mount
            module.cd(mount)

# add or update endpoint in config 
        endpoints = json.loads(self.config.get(self.config_name, 'endpoints'))
        endpoints[mount] = module.endpoint()
        self.config.set(self.config_name, 'endpoints', json.dumps(endpoints))
# NB: don't save config here ... remote_add is called on startup ... so we don't want to save (and wipe) before we add ALL remotes

#------------------------------------------------------------
    def remotes_get(self, path):
        logging.debug("path=[%s]" % path)
        fullpath = self.absolute_remote_filepath(path)
        for mount in self.remotes:
# FIXME - should look for best match ... eg we have a remote on '/' and another on '/projects'
            if fullpath.startswith(mount) is True:
                logging.debug("matched [%s] with [%s] = %r" % (fullpath, mount, self.remotes[mount]))
                return self.remotes[mount]
        raise Exception("Could not find anything connected to [%s]" % path)

#------------------------------------------------------------
    def requires_auth(self, line):
        local_commands = ["login", "help", "lls", "lcd", "lpwd", "version", "processes", "exit", "quit"]
# only want first keyword (avoid getting "not logged in" on input like "help get")
        try:
            primary = line.strip().split()[0]
            if primary in local_commands:
                return False
        except:
            pass

        return True

#------------------------------------------------------------
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

#------------------------------------------------------------
    def ask(self, text):
# if script, assumes you know what you're doing
        if self.interactive is False:
            return True
        response = input(text)
        if response == 'y' or response == 'Y':
            return True
        return False

#------------------------------------------------------------
    def escape_single_quotes(self, namespace):
        return namespace.replace("'", "\\'")

#------------------------------------------------------------
    def absolute_remote_filepath(self, line):
        if line.startswith('"') and line.endswith('"'):
            line = line[1:-1]
        if not posixpath.isabs(line):
            line = posixpath.join(self.cwd, line)
        fullpath = posixpath.normpath(line)
        return fullpath

#------------------------------------------------------------
    def help_version(self):
        print("\nReturn the current version build identifier\n")
        print("Usage: build\n")

    def do_version(self, line):
        global build
        print(" VERSION: %s" % build)

#------------------------------------------------------------
    def help_file(self):
        print("\nReturn metadata information on a remote file\n")
        print("Usage: file <filename>\n")

    def do_file(self, line):
        remote = self.remotes_get(line)
        if remote is not None:
            fullpath = self.absolute_remote_filepath(line)
# TODO - this should be a dict that we can display to make it generic
# TODO - include checksums ... will make for a faster compare to confirm migration
            remote.info(fullpath)

#------------------------------------------------------------
    def help_remotes(self):
        print("\nInformation about remote clients\n")
        print("Usage: remotes <add>\n")

# --- 
    def do_remotes(self, line):
        if 'add' in line:
            print("TODO")
        else:
            for mount, client in self.remotes.items():
                print("%-20s [%s]" % (mount, client.status))

#------------------------------------------------------------
    def help_ec2(self):
        print("\nInformation about ec2 credentials from keystone \n")
        print("Usage: ec2 <login/list/create/delete>\n")

# ---
    def do_ec2(self, line):
        args = line.split()
        nargs = len(args)
# do stuff like:
# info https://nimbus.pawsey.org.au:5000 -> projects (eg magenta-storage)
# info https://nimbus.pawsey.org.au:5000/magenta-storage/credentials
# TODO map to ls https://nimbus etc
#https://stackoverflow.com/questions/44751574/uploading-to-amazon-s3-via-curl-route/44751929
# list ec2 credentials (per project or for all if no project specified)

        if self.keystone is None:
            raise Exception("Missing keystone url.")

        if 'login' in line:
            logging.info("Attempting discovery via: [%s]" % self.keystone)
# find a mediaflux client
# use as SSO for keystone auth
# do discovery
# TODO - if no SSO (or we remove) then do old fashioned login
            mfclient = None
            for mount, client in self.remotes.items():
                if client.type == 'mfclient':
                    logging.info("Attempting SSO via: [%r]" % client)
                    mfclient = client
            try:
                self.keystone.connect(mfclient, refresh=True)
                s3_client = s3client.s3_client()
                self.keystone.discover_s3(s3_client)
                self.remotes_add('/'+s3_client.prefix, s3_client)
                self.remotes_config_save()
            except Exception as e:
                logging.info(str(e))
                # probably no boto3 - may have still got credentials
                print("Discovery incomplete")
            return

        if 'list' in line:
            self.keystone.credentials_print()
            return

        if 'create' in line:
            if nargs > 1:
                access = self.keystone.credentials_create(args[1])
                print("Created access: %s" % access)
            else:
                raise Exception("Error: missing project reference")
            return

        if 'delete' in line:
            if nargs > 1:
                result = self.keystone.credentials_delete(args[1])
                print(result)
            else:
                raise Exception("Error: missing access reference")
            return

        print("keystone=%s" % self.keystone.url)

#------------------------------------------------------------
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

# --- helper
# display prompt and return pagination specific directives (next page, quit)
    def pagination_controller(self, prompt):
        result = None
        if prompt is not None:
            if self.interactive:
                sys.stdout.write(prompt)
                sys.stdout.flush()
                result = ""
                while True:
                    key = self.wait_key()
#                    print "got [%r]" % key
                    sys.stdout.write(key)
# end pagination immediately on q press
                    if key == 'q':
                        result += key
                        print()
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
                        print()
                        return result
# concat everything else onto the final result
                    else:
                        result += key
#            else:
                # TODO - sleep?
#                print(prompt)

        return result

#------------------------------------------------------------
    def help_ls(self):
        print("\nList files stored on the remote server.")
        print("Navigation in paginated output can be achieved by entering a page number, [enter] for next page or q to quit.\n")
        print("Usage: ls <file pattern or folder name>\n")

# --- 
    def do_ls(self, line):
        fullpath = self.absolute_remote_filepath(line)
        remote = self.remotes_get(fullpath)
        remote_list = remote.ls_iter(fullpath)
        count = 0
        size = max(1, min(self.terminal_height - 3, 100))
        for line in remote_list:
            print(line)
            count = count+1
            if count > size:
                response = self.pagination_controller("=== (enter = next, q = quit) === ")
                if response is not None:
                    if response == 'q' or response == 'quit':
                        return
                    else:
                        count = 0

# --- helper
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
        logging.debug("import_metadata() [%s] : [%s]" % (asset_id, filepath))
        try:
            config = configparser.ConfigParser()
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

# update the asset metadata
            self.mf_client.aterm_run(xml_command)
# re-analyze the content - stricly only needs to be done if type/ctype/lctype was changed
# NEW - don't do this by default - it will generate stacktrace in mediaflux for DMF (offline) files
#            self.mf_client.aterm_run("asset.reanalyze :id %r" % asset_id)

        except Exception as e:
            logging.warning("Metadata population failed: %s" % str(e))

#------------------------------------------------------------
#    def help_import(self):
#        print("\nUpload files or folders with associated metadata")
#        print("For every file <filename.ext> another file called <filename.ext.meta> should contain metadata in INI file format\n")
#        print("Usage: import <file or folder>\n")

# ---
    def do_import(self, line):
        raise Exception("Not implemented yet.")
#        self.do_put(line, meta=True)

#------------------------------------------------------------
    def help_get(self):
        print("\nDownload remote files to the current local folder\n")
        print("Usage: get <remote files or folders>\n")

# --
    def cb_get(self, future):
        bytes_recv = int(future.result())
        with threading.Lock():
            self.get_count += 1
            self.get_bytes += bytes_recv
# progress update report
        self.print_over("get progress: [%r/%r] files and [%r/%r] bytes" % (self.get_count, self.total_count, self.get_bytes, self.total_bytes))

# --
    def do_get(self, line):
# turn input line into a matching file iterator
        line = self.absolute_remote_filepath(line)
        remote = self.remotes_get(line)
        if remote is not None:
            results = remote.get_iter(line)
            self.total_count = int(next(results))
            self.total_bytes = int(next(results))
            self.get_count = 0
            self.get_bytes = 0
            start_time = time.time()
            self.print_over("Setting up for %d files..." % self.total_count)
            try:
                count = 0
                for remote_fullpath in results:
                    remote_relpath = posixpath.relpath(path=remote_fullpath, start=self.cwd)
                    local_filepath = os.path.join(os.getcwd(), remote_relpath)
                    future = self.thread_executor.submit(jump_get, remote, remote_fullpath, local_filepath)
                    future.add_done_callback(self.cb_get)
                    count += 1

            except Exception as e:
                logging.info("[%s] count = %d" % (str(e), count))

# FIXME - throws exception "completed" when done - but no problem occurred ... 
                pass

# TODO - control-C -> terminate threads ...
#self.thread_executor.shutdown(wait=True, cancel_futures=True)

# wait until completed (cb_get does progress updates)
            logging.info("Waiting for thread executor...")
            while self.get_count < self.total_count:
                time.sleep(3)

# print summary and return
            elapsed = time.time() - start_time
            rate = float(self.total_bytes) / float(elapsed)
            rate = rate / 1000000.0
            self.print_over("Completed get for %d files with total size %s at: %.1f MB/s   \n" % (self.get_count, self.human_size(self.get_bytes), rate))

#------------------------------------------------------------
    def help_put(self):
        print("\nUpload local files or folders to the current folder on the remote server\n")
        print("Usage: put <file or folder>\n")

# --
    def cb_put(self, future):

        bytes_sent = int(future.result())
        with threading.Lock():
            self.put_count += 1
            self.put_bytes += bytes_sent
# progress update report
        self.print_over("put progress: [%r] files and [%r] bytes" % (self.put_count, self.put_bytes))

# --
    def put_iter(self, line):
        if os.path.isdir(line):
            logging.info("Walking directory tree...")
            line = os.path.abspath(line)
            parent = os.path.normpath(os.path.join(line, ".."))
            for root, directory_list, name_list in os.walk(line):
                local_relpath = os.path.relpath(path=root, start=parent)
                relpath_list = local_relpath.split(os.sep)
                remote_relpath = "/".join(relpath_list)
                remote_fullpath = posixpath.join(self.cwd, remote_relpath)
                for name in name_list:
                    fullpath = os.path.normpath(os.path.join(os.getcwd(), root, name))
                    yield remote_fullpath, fullpath
        else:
            logging.info("Building file list... ")
            for name in glob.glob(line):
                local_fullpath = os.path.abspath(name)
#                if os.path.isfile(local_fullpath):
                yield self.cwd, local_fullpath

# --
    def do_put(self, line, meta=False):

        logging.info("[%s]" % line)
        remote = self.remotes_get(self.cwd)

        try:
            self.put_count = 0
            self.put_bytes = 0
            total_count = 0
            start_time = time.time()

            results = self.put_iter(line)

            for remote_fullpath, local_fullpath in results:
                logging.info("put remote=[%s] local=[%s]" % (remote_fullpath, local_fullpath))
                future = self.thread_executor.submit(jump_put, remote, remote_fullpath, local_fullpath)
                future.add_done_callback(self.cb_put)
                total_count += 1

        except Exception as e:
            logging.error(str(e))
            pass

# wait until completed (cb_put does progress updates)
        while self.put_count < total_count:
            time.sleep(3)

# print summary and return
        elapsed = time.time() - start_time
        rate = float(self.put_bytes) / float(elapsed)
        rate = rate / 1000000.0
        self.print_over("Completed put for %d files with total size %s at: %.1f MB/s   \n" % (self.put_count, self.human_size(self.put_bytes), rate))

#------------------------------------------------------------
    def do_copy(self, line):
        logging.info("in: %s" % line)

# TODO - options for metadata copy as well (IF src = mflux)
#        option_list, tail = getopt.getopt(line, "r")
#        logging.info("options: %r" % option_list)
#        logging.info("tail: %r" % tail)

        try:
            path_list = shlex.split(line, posix=True)
            logging.info("copy [%r]" % path_list)

        except Exception as e:
            logging.debug(str(e))

        if len(path_list) != 2:
            raise Exception("Expected exactly two path arguments: source and destination")

# source location is the remote client that controls the copy
        from_abspath = self.absolute_remote_filepath(path_list[0])
        to_abspath = self.absolute_remote_filepath(path_list[1])
        source = self.remotes_get(from_abspath)
        destination = self.remotes_get(to_abspath)
        source.copy(from_abspath, to_abspath, destination, prompt=self.ask)

#------------------------------------------------------------
    def help_cd(self):
        print("\nChange the current remote folder\n")
        print("Usage: cd <folder>\n")

    def do_cd(self, line):
        candidate = self.absolute_remote_filepath(line)
        if candidate in self.remotes:
            self.cwd = candidate
        else:
            try:
                remote = self.remotes_get(candidate)
                self.cwd = remote.cd(candidate)
            except Exception as e:
                logging.debug(str(e))
                print("Could not access remote folder")

#------------------------------------------------------------
    def help_pwd(self):
        print("\nDisplay the current remote folder\n")
        print("Usage: pwd\n")

# use repr to help figure out issues such as invisible characters in folder names
    def do_pwd(self, line):
        print("Remote: %s" % repr(self.cwd))

#------------------------------------------------------------
    def help_mkdir(self):
        print("\nCreate a remote folder\n")
        print("Usage: mkdir <folder>\n")

    def do_mkdir(self, line, silent=False):
        ns_target = self.absolute_remote_filepath(line)
        remote = self.remotes_get(ns_target)
        remote.mkdir(ns_target)

#------------------------------------------------------------
    def help_rm(self):
        print("\nDelete remote file(s)\n")
        print("Usage: rm <file or pattern>\n")

    def do_rm(self, line):
        fullpath = self.absolute_remote_filepath(line)
        remote = self.remotes_get(fullpath)
        if remote.rm(fullpath, prompt=self.ask) is False:
            print("Delete aborted")

#------------------------------------------------------------
    def help_rmdir(self):
        print("\nRemove a remote folder\n")
        print("Usage: rmdir <folder>\n")

    def do_rmdir(self, line):
        ns_target = self.absolute_remote_filepath(line)
        remote = self.remotes_get(ns_target)
        if self.ask("Remove folder: %s (y/n) " % ns_target):
            remote.rmdir(ns_target)
        else:
            print("Aborted")

#------------------------------------------------------------
# -- local commands
# --
    def help_lpwd(self):
        print("\nDisplay local folder\n")
        print("Usage: lpwd\n")

    def do_lpwd(self, line):
        print("Local: %s" % os.getcwd())

# --
    def help_lcd(self):
        print("\nChange local folder\n")
        print("Usage: lcd <folder>\n")

    def do_lcd(self, line):
        os.chdir(line)
        print("Local: %s" % os.getcwd())

# --
    def help_lls(self):
        print("\nList contents of local folder\n")
        print("Usage: lls <folder>\n")

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

        print("Local folder: %s" % display_path)

# glob these to allow wildcards
        for filename in glob.glob(path):
            if os.path.isdir(filename):
                head, tail = os.path.split(filename)
                print("[Folder] " + tail)

        for filename in glob.glob(path):
            if os.path.isfile(filename):
                head, tail = os.path.split(filename)
                print("%s | %-s" % (self.human_size(os.path.getsize(filename)), tail))

#------------------------------------------------------------
    def help_processes(self):
        print("\nSet the number of concurrent processes to use when transferring files.")
        print("If no number is supplied, reports the current value.")
        print("Usage: processes <number>\n")

# ---
    def do_processes(self, line):
        try:
            p = max(1, min(int(line), 16))
            self.thread_max = p
# shutdown executor and start up again ...
            print("Restarting background processes...")
            self.thread_executor.shutdown()
            self.thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_max)
        except Exception as e:
            logging.info(str(e))
            pass
        print("Current number of processes: %r" % self.thread_max)

#------------------------------------------------------------
    def help_logout(self):
        print("\nTerminate the current session to the server\n")
        print("Usage: logout\n")

# ---
    def do_logout(self, line):
        remote = self.remotes_get(self.cwd)
        if remote is not None:
            remote.logout()

#------------------------------------------------------------
    def help_login(self):
        print("\nInitiate login to the current remote server\n")
        print("Usage: login\n")

# ---
    def do_login(self, line):
        remote = self.remotes_get(self.cwd)
        if remote is None:
            raise Exception("Please specify remote target for login")
        remote.login()
        self.remotes_config_save()

#------------------------------------------------------------
    def help_delegate(self):
        print("\nCreate a credential, stored in your local home folder, for automatic authentication to the remote server.")
        print("An optional argument can be supplied to set the lifetime, or off to destroy all your delegated credentials.\n")
        print("Usage: delegate <days/off>\n")

# ---
    def do_delegate(self, line):
        remote = self.remotes_get(self.cwd)
        if remote.delegate(line) is True:
            self.remotes_config_save()

#------------------------------------------------------------
    def help_publish(self):
        print("\nCreate public URLs for specified file(s)\nRequires public sharing to be enabled by the project administrator\n")
        print("Usage: publish <file(s) or folder>\n")

# --
    def do_publish(self, line):
        fullpath = self.absolute_remote_filepath(line)
        remote = self.remotes_get(fullpath)
        count = remote.publish(fullpath)
        print("Published %d files" % count)

#------------------------------------------------------------
    def help_unpublish(self):
        print("\nRemove public access for a file or files\n")
        print("Usage: unpublish <file(s) or folder>\n")

# --
    def do_unpublish(self, line):
        fullpath = self.absolute_remote_filepath(line)
        remote = self.remotes_get(fullpath)
        count = remote.unpublish(fullpath)
        print("Unpublished %d files" % count)

#------------------------------------------------------------
    def help_quit(self):
        print("\nExit without terminating the session\n")
    def do_quit(self, line):
        exit(0)

# --
    def help_exit(self):
        print("\nExit without terminating the session\n")
    def do_exit(self, line):
        exit(0)

#------------------------------------------------------------
    def loop_interactively(self):
        while True:
            try:
                self.cmdloop()

            except KeyboardInterrupt:
                print(" Interrupted by user")

# NB: here's where all command failures are caught
            except SyntaxError:
                print(" Syntax error: for more information on commands type 'help'")

            except Exception as e:
# exit on the EOF case ie where stdin/file is force fed via command line redirect
# FIXME - this can sometimes occur in some mediaflux error messages
                if "EOF" in str(e):
                    print("Exit: encountered EOF")
                    return
                print(str(e))

#------------------------------------------------------------
def main():
    global build

# server config (section heading) to use
    p = argparse.ArgumentParser(description="pshell help")
    p.add_argument("-c", dest='current', default='data.pawsey.org.au', help="the config name in $HOME/.mf_config to connect to")
    p.add_argument("-i", dest='script', help="input script file containing pshell commands")
    p.add_argument("-o", dest='output', default=None, help="output any failed commands to a script")
    p.add_argument("-v", dest='verbose', default=None, help="set verbosity level (0,1,2)")
    p.add_argument("-u", dest='url', default=None, help="Remote endpoint")
    p.add_argument("-d", dest='domain', default=None, help="login authentication domain")
    p.add_argument("-s", dest='session', default=None, help="session")
    p.add_argument("-m", dest='mount', default='/', help="mount point for remote")
    p.add_argument("--keystone", dest='keystone', default=None, help="A URL to the REST interface for Keystone (Openstack)")
    p.add_argument("command", nargs="?", default="", help="a single command to execute")
    args = p.parse_args()

# NEW
    logging_level = logging.ERROR
    if args.verbose is not None:
        if args.verbose == "2":
            logging_level = logging.DEBUG
        elif args.verbose == "1":
            logging_level = logging.INFO

#    print("log level = %d" % logging_level)
    logging.basicConfig(format='%(levelname)9s %(asctime)-15s >>> %(module)s.%(funcName)s(): %(message)s', level=logging_level)
    logging.info("PSHELL=%s" % build)


# TODO - may have to completely rework the ini file for the endpoint/type/name structure
#import ConfigParser
#import json
#IniRead = ConfigParser.ConfigParser()
#IniRead.read('{0}\{1}'.format(config_path, 'config.ini'))
#value = json.loads(IniRead.get('Section', 'Value'))

# get local path for storing the config, fallback to CWD if system gives a dud path for ~
#    config_filepath = os.path.expanduser("~/.mf_config")

    config_filepath = os.path.expanduser("~/.pshell_config")
    try:
        open(config_filepath, 'a').close()
    except:
        config_filepath = os.path.join(os.getcwd(), ".pshell_config")

    config = configparser.ConfigParser()
    logging.debug("Reading config file: [%s]" % config_filepath)
    config.read(config_filepath)

# attempt to use the current section in the config for connection info
    try:
        add_endpoint = True
        endpoints = None 

        if args.url is None:
# existing config and no input URL
            if config.has_section(args.current) is True:
                logging.info("No input URL, reading endpoints from existing config [%s]" % args.current)
                endpoints = json.loads(config.get(args.current, 'endpoints'))
            else:
# 1st time default
                logging.info("Initialising default config")
                args.url = 'https://data.pawsey.org.au:443'
                args.domain = 'ivec'
                args.mount = '/projects'

        if endpoints is None:
# if URL supplied or 1st time setup
            logging.info("Creating endpoint from url: [%s]" % args.url)

# WTF - urlparse not extracting the port
            aaa = urllib.parse.urlparse(args.url)
# HACK - workaround for urlparse not extracting port, despite the doco indicating it should
            p = '(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*'
            m = re.search(p,args.url)
            port = m.group('port')
            args.current = aaa.hostname

# FIXME - historically, have to assume it's mflux but now could be s3 
# FIXME - could adopt a scheme where we use "mflux://data.pawsey.org.au:443" and "s3://etc" ... and assume the proto from the port
            endpoint = {'name':args.current, 'type':'mfclient', 'protocol':aaa.scheme, 'server':aaa.hostname, 'port':port, 'domain':args.domain }
# FIXME - session="" needs to be strongly enforced or can get some wierd bugs
            endpoint['session'] = ""
            endpoint['token'] = ""

# no such config section - add and save
            logging.info("Saving section: [%s] in config" % args.current)

            endpoints = { args.mount:endpoint }

            config[args.current] = {'endpoints':json.dumps(endpoints)}

            with open(config_filepath, 'w') as f:
                config.write(f)

    except Exception as e:
        logging.debug(str(e))
        logging.info("No remote endpoints configured.")
        add_endpoint = False


# extract terminal size for auto pagination
    try:
        import fcntl, termios, struct
        size = struct.unpack('hh', fcntl.ioctl(0, termios.TIOCGWINSZ, '1234'))
    except:
# FIXME - make this work with windows
        size = (80, 20)


# hand control of mediaflux client over to parsing loop
    my_parser = parser()

    my_parser.config = config
    my_parser.config_name = args.current
    my_parser.config_filepath = config_filepath


# NEW 
# TODO - generic + link to # processes
    my_parser.thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=my_parser.thread_max)


# add discovery url
    if args.keystone is not None:
        my_parser.config.set(args.current, 'keystone', args.keystone)

    if my_parser.config.has_option(args.current, 'keystone'):
        my_parser.keystone = keystone.keystone(my_parser.config.get(args.current, 'keystone'))

# add endpoints
    try:
        for mount in endpoints:
            endpoint = endpoints[mount]
            logging.info("Connecting [%s] endpoint on [%s]" % (endpoint['type'], mount))
            if endpoint['type'] == 'mfclient':
                myclient = mfclient.mf_client(protocol=endpoint['protocol'], server=endpoint['server'], port=endpoint['port'], domain=endpoint['domain'])
# CLI overrides
                if args.session is not None:
                    endpoint['session'] = args.session
                if args.domain is not None:
                    endpoint['domain'] = args.domain
# TODO - remotes to accept endpoint as initialiser
                if 'session' in endpoint:
                    myclient.session = endpoint['session']
                if 'token' in endpoint:
                    myclient.token = endpoint['token']
                if 'domain' in endpoint:
                    myclient.domain = endpoint['domain']
            elif endpoint['type'] == 's3':
                myclient = s3client.s3_client(host=endpoint['host'], access=endpoint['access'], secret=endpoint['secret'])
            else:
                myclient = client()

# associate client with mount
            my_parser.remotes_add(mount, myclient)

# added all remotes without error - save to config
        my_parser.remotes_config_save()

    except Exception as e:
        logging.error(str(e))

# just in case the terminal height calculation returns a very low value
    my_parser.terminal_height = max(size[0], my_parser.terminal_height)

# restart script
    if args.output is not None:
        my_parser.script_output = args.output

# TAB completion
# strange hackery required to get tab completion working under OS-X and also still be able to use the b key
# REF - http://stackoverflow.com/questions/7124035/in-python-shell-b-letter-does-not-work-what-the
    try:
        if 'libedit' in readline.__doc__:
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")
    except:
        logging.warning("No readline module; tab completion unavailable")

# build non interactive input iterator
    input_list = []
    my_parser.interactive = True
    if args.script:
        input_list = itertools.chain(input_list, open(args.script))
        my_parser.interactive = False
# FIXME - stricly, need regex to avoid split on quote protected &&
    if len(args.command) != 0:
        input_list = itertools.chain(input_list, args.command.split("&&"))
        my_parser.interactive = False

# interactive or input iterator (scripted)
    if my_parser.interactive:
        print(" === pshell: type 'help' for a list of commands ===")
        my_parser.loop_interactively()
    else:
        for item in input_list:
            line = item.strip()
            try:
                print("%s:%s> %s" % (args.current, my_parser.cwd, line))
                my_parser.onecmd(line)
            except KeyboardInterrupt:
                print(" Interrupted by user")
                exit(-1)
            except SyntaxError:
                print(" Syntax error: for more information on commands type 'help'")
                exit(-1)
            except Exception as e:
                print(str(e))
                exit(-1)


if __name__ == '__main__':
# On Windows calling this function is necessary.
#    multiprocessing.freeze_support()
    main()
