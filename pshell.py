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
import urllib
import getpass
import logging
import zipfile
import argparse
import datetime
import itertools
import posixpath
import configparser
import xml.etree.ElementTree as ET
import mfclient
# no readline on windows
try:
    import readline
except:
    pass

# NEW
import keystone
import s3client
import getopt
import shlex
import pathlib
import concurrent.futures
import threading


# standard lib python command line client for mediaflux
# Author: Sean Fleming
build= "Latest"
delegate_default = 7
delegate_min = 1
delegate_max = 365


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
    cwd = '/'
    interactive = True
    transfer_processes = 3
    terminal_height = 20
    script_output = None


# TODO - class? or just a dict?
    get_executor = None
    get_count = 0
    get_bytes = 0
    total_count = 0
    total_bytes = 0
    put_count = 0
    put_bytes = 0


# --- initial setup of prompt
    def preloop(self):
         self.prompt = "%s:%s>" % (self.config_name, self.cwd)

# --- not logged in -> don't even attempt to process remote commands
    def precmd(self, line):
        return cmd.Cmd.precmd(self, line)

# --- prompt refresh (eg after login/logout)
    def postcmd(self, stop, line):
        self.prompt = "%s:%s>" % (self.config_name, self.cwd)
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
#        raise Exception("Unknown command passthrough not implemented")
# unrecognized - assume it's an aterm command
        remote = self.remotes_get(self.cwd)
        logging.info("default passthru cwd=[%s] -> remote=[%r]" % (self.cwd, remote))
        if remote is not None:
            logging.debug("Fixme - assuming mflux")
            reply = remote.aterm_run(line)
            remote.xml_print(reply)
        return

#------------------------------------------------------------
    def config_save(self):
        logging.info("saving config to file = %s" % self.config_filepath)
        with open(self.config_filepath, 'w') as f:
            self.config.write(f)

# ---
# TODO = if no mount -> refresh all
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
        self.config_save()

# ---
    def remotes_add(self, mount='/remote', module=None):

        logging.info("mount = [%s], module = [%r]" % (mount, module))
        self.remotes[mount] = module

# init cwd
        if module.connect() is True:
            self.cwd = module.cd(mount)

# add to config and save
        endpoints = json.loads(self.config.get(self.config_name, 'endpoints'))
        endpoints[mount] = module.endpoint()
        self.config.set(self.config_name, 'endpoints', json.dumps(endpoints))
        self.remotes_config_save()

# ---
# convert a relative or absolute path reference into an appropriate remote client
    def remotes_get(self, path):
        fullpath = self.absolute_remote_filepath(path)
        for mount in self.remotes:
# FIXME - should look for best match ... eg we have a remote on '/' and another on '/projects'
            if fullpath.startswith(mount) is True:
                logging.debug("matched [%s] with [%s] = %r" % (fullpath, mount, self.remotes[mount]))
                return self.remotes[mount]
        return None

# ---
    def do_remotes(self, line):
#        if "list" in line:
        for mount, client in self.remotes.items():
            print("%-20s [%s]" % (mount, client.status))

# TODO - if "add" in line ...



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
        response = input(text)
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

# --- version tracking 
    def help_version(self):
        print("\nReturn the current version build identifier\n")
        print("Usage: build\n")

    def do_version(self, line):
        global build
        print(" VERSION: %s" % build)

# --- file info
    def help_file(self):
        print("\nReturn metadata information on a remote file\n")
        print("Usage: file <filename>\n")

    def do_file(self, line):
        remote = self.remotes_get(line)
        if remote is not None:
            fullpath = self.absolute_remote_filepath(line)
            remote.info(fullpath)

# ---
# TODO - EC2 create/delete/list
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

        if 'discover' in line:
            logging.info("Attempting discovery via: [%s]" % self.keystone)
# find a mediaflux client
# use as SSO for keystone auth
# do discovery

            mfclient = None
            for mount, client in self.remotes.items():
                if client.type == 'mfclient':
                    logging.info("Attempting SSO via: [%r]" % client)
                    mfclient = client
            try:
                self.keystone.connect(mfclient, refresh=True)
                s3_client = s3client.s3client()
                self.keystone.discover_s3(s3_client)
# CURRENT - update config and save
                self.remotes_add('/'+s3_client.prefix, s3_client)
            except Exception as e:
                logging.info(str(e))
                print("Discovery failed")
            return

        if 'list' in line:
            self.keystone.credentials_print(line)
            return

        if 'create' in line:
            if nargs > 1:
                self.keystone.credentials_create(args[1])
            else:
                raise Exception("Error: missing project reference")
            return

        if 'delete' in line:
            if nargs > 1:
                self.keystone.credentials_delete(args[1])
            else:
                raise Exception("Error: missing access reference")
            return


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
        for line in remote_list:
            print(line)
            count = count+1
            size = max(1, min(self.terminal_height - 3, 100))

            if count > size:
                pagination_footer = "=== (enter = next, q = quit) === " 
                response = self.pagination_controller(pagination_footer)
                if response is not None:
                    if response == 'q' or response == 'quit':
                        return
                    else:
                        count = 0

# -- old
#    def poll_total(self, base_query):
# run in background as this can timeout on larger DMF folders
#        result = self.mf_client.aterm_run('asset.content.status.statistics :where "%s" &' % base_query)

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
    def help_import(self):
        print("\nUpload files or folders with associated metadata")
        print("For every file <filename.ext> another file called <filename.ext.meta> should contain metadata in INI file format\n")
        print("Usage: import <file or folder>\n")

# ---
    def do_import(self, line):
        self.do_put(line, meta=True)
        return

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
                    future = self.get_executor.submit(jump_get, remote, remote_fullpath, local_filepath)
                    future.add_done_callback(self.cb_get)
                    count += 1

            except Exception as e:
                logging.info("[%s] count = %d" % (str(e), count))

# FIXME - throws exception "completed" when done - but no problem occurred ... 
                pass

# TODO - control-C -> terminate threads ...
#self.get_executor.shutdown(wait=True, cancel_futures=True)

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
                future = self.get_executor.submit(jump_put, remote, remote_fullpath, local_fullpath)
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
            raise Exception("Expected only two path arguments: source and destination")

# FIXME - do via remote module types
# expect source or destination to be S3 and the other to be mflux
#        if self.s3client.is_mine(path_list[0]) == self.s3client.is_mine(path_list[1]):
#            raise Exception("Require source and destination to be different storage systems")

# CURRENT - only supporting mflux -> s3
        if self.mf_client.namespace_exists(path_list[0]):
            logging.info("Confirmed mediaflux namespace: %s" % path_list[0])
        else:
            raise Exception("Unsupported source for copy (expected single file? wildcards? namespace?")

# away we go ...

# 3rd party transfer (queues?) from mflux to s3 endpoint
# NB: currently thinking path will be dropped ... ie /projects/a/b/etc/file.txt -> s3:bucket/file.txt
# might have to have some smarts though if there is a max limit on # objects per bucket 
# TODO - mfclient.s3copy_managed() method for this ???

#------------------------------------------------------------
    def help_cd(self):
        print("\nChange the current remote folder\n")
        print("Usage: cd <folder>\n")

    def do_cd(self, line):
        candidate = self.absolute_remote_filepath(line)
        remote = self.remotes_get(candidate)
        try:
            self.cwd = remote.cd(candidate)
            return
        except Exception as e:
            print("No such remote folder, to show valid folders run: remotes list")

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

# --- working example of PKI via mediaflux
#     def do_mls(self, line):
#         pkey = open('/Users/sean/.ssh/id_rsa', 'r').read()
#         reply = self.mf_client.aterm_run("secure.shell.execute :command ls :host magnus.pawsey.org.au :private-key < :name sean :key \"%s\" >" % pkey)
#         self.mf_client.xml_print(reply)

# ---
    def help_processes(self):
        print("\nSet the number of concurrent processes to use when transferring files.")
        print("If no number is supplied, reports the current value.")
        print("Usage: processes <number>\n")

    def do_processes(self, line):
        try:
            p = max(1, min(int(line), 16))
            self.transfer_processes = p
        except:
            pass
        print("Current number of processes: %r" % self.transfer_processes)

# -- connection commands
    def help_logout(self):
        print("\nTerminate the current session to the server\n")
        print("Usage: logout\n")

    def do_logout(self, line):
        remote = self.remotes_get(self.cwd)
        if remote is not None:
            remote.logout()

# ---
    def help_login(self):
        print("\nInitiate login to the current remote server\n")
        print("Usage: login\n")

    def do_login(self, line):

        remote = self.remotes_get(self.cwd)
        logging.info("remote type = [%s]" % remote.type)
        if remote.type == 'mfclient':
            logging.info("Authentication domain [%s]" % remote.domain)
            user = input("Username: ")
            password = getpass.getpass("Password: ")
            remote.login(user, password)

# NEW
#            mount = self.remotes_mount_get(self.cwd)
            mount = self.cwd

            self.remotes_config_save()


# NEW - add to secure wallet for identity management
#        xml_reply = self.mf_client.aterm_run("secure.wallet.can.be.used")
#            self.mf_client.aterm_run("secure.wallet.recreate :password %s" % password)
# TODO - encrypt so it's not plain text 
#        self.mf_client.aterm_run("secure.wallet.set :key ldap :value %s" % password)
# this can only be done with an authenticated mfclient
#            my_parser.keystone.connect(mf_client, refresh=False)
#            my_parser.keystone.discover_s3(my_parser.s3client)
#            my_parser.remotes_add('/'+my_parser.s3client.prefix, my_parser.s3client)


# --
    def help_delegate(self):
        print("\nCreate a credential, stored in your local home folder, for automatic authentication to the remote server.")
        print("An optional argument can be supplied to set the lifetime, or off to destroy all your delegated credentials.\n")
        print("Usage: delegate <days/off>\n")

    def do_delegate(self, line):

        remote = self.remotes_get(self.cwd)
        if remote.type != 'mfclient':
            return


# argument parse
        dt = delegate_default
        destroy_session = False
        if line:
            if line == "off":
                try:
                    remote.aterm_run("secure.identity.token.all.destroy")
                    logging.debug("Removed secure tokens from server")
# figure out the current session
                    reply = remote.aterm_run("actor.self.describe")
                    if 'identity' in reply.find(".//actor").attrib['type']:
                        destroy_session = True
                except:
# probably a bad session (eg generated from an expired token)
                    logging.debug("Failed to remove secure tokens from server")
                    destroy_session = True
# remove all auth info and update config
                remote.token = ""
                use_token = False

# if current session is delegate based - destroy it too
                if destroy_session:
                    remote.session = ""

                self.config_save()

                print("Delegate credentials removed.")
                return
            else:
                try:
                    dt = max(min(float(line), delegate_max), delegate_min)
                except:
                    print("Bad delegate lifetime.")
# lifetime setup
        d = datetime.datetime.now() + datetime.timedelta(days=dt)
        expiry = d.strftime("%d-%b-%Y %H:%M:%S")

# query current authenticated identity
        domain = None
        user = None
        name = None
        result = remote.aterm_run("actor.self.describe")
        elem = result.find(".//actor")
        if elem is not None:
            actor = elem.attrib['name']
            if ":" in actor:
                i = actor.find(":")
                domain = actor[0:i]
                user = actor[i+1:]
        if user is None or domain is None:
            raise Exception("Delegate identity %r is not allowed to delegate" % actor)

# create secure token (delegate) and assign current authenticated identity to the token
        logging.debug("Attempting to delegate for: domain=%s, user=%s, until=%r" % (domain, user, expiry))
        result = remote.aterm_run('secure.identity.token.create :to "%s" :role -type user "%s" :role -type domain "%s" :min-token-length 16 :wallet true' % (expiry, actor, domain))
        for elem in result.iter():
            if elem.tag == 'token':
                print("Delegate valid until: " + expiry)
                remote.token = elem.text
# NEW
                endpoint = json.loads(self.config.get(self.config_name, 'endpoint'))
                endpoint['token'] = remote.token
                self.config[self.config_name] = {'endpoint':json.dumps(endpoint) }
                self.config_save()
                return
        raise Exception("Delegate command successfull; but failed to find return token")


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

# --- compare
    def help_compare(self):
        print("\nCompares a local and a remote folder and reports any differences")
        print("The local and remote folders must have the same name and appear in the current local and remote working directories")
        print("Usage: compare <folder>\n")

# --- compare
# NB: checksum compare is prohibitively expensive in general, so default to file size based comparison
    def do_compare(self, line, checksum=False, filesize=True):
        remote_fullpath = self.absolute_remote_filepath(line)

# check remote
        if self.mf_client.namespace_exists(remote_fullpath) is False:
            print("Could not find remote folder: %s" % remote_fullpath)
            return

# no folder specified - compare local and remote working directories 
        if remote_fullpath == self.cwd:
            local_fullpath = os.getcwd()
        else:
            remote_basename = posixpath.basename(remote_fullpath)
            local_fullpath = os.path.join(os.getcwd(), remote_basename)
# check local
        if os.path.exists(local_fullpath) is False:
            print("Could not find local folder: %s" % local_fullpath)
            return

# build remote set
        remote_files = set()
        print("Building remote file set under [%s] ..." % remote_fullpath)
        remote_files = self.get_remote_set(remote_fullpath)

# build local set
        local_files = set()
        print("Building local file set under [%s] ..." % local_fullpath)
        try:
            for (dirpath, dirnames, filenames) in os.walk(local_fullpath):
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    relpath = os.path.relpath(full_path, local_fullpath)
                    local_files.add(relpath)
        except Exception as e:
            logging.error(str(e))

# starting summary
        print("Total remote files = %d" % len(remote_files))
        print("Total local files = %d" % len(local_files))

# remote only count
        count_pull = 0
        print("=== Remote server only ===")
        for item in remote_files - local_files:
            count_pull += 1
            print(("%s" % item))
# report 
        count_push = 0
        print("=== Local filesystem only ===")
        for item in local_files - remote_files:
            print(("%s" % item))
            count_push += 1

# for common files, report if there are differences
        print("=== Differing files ===")
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
                    logging.error("do_compare(): %s" % str(e))
# always report (warning) mismatched files
                if local_crc32 == remote_crc32:
                    count_common += 1
                else:
                    print(("s: local crc32=%r, remote crc32=%r" % (item, local_crc32, remote_crc32)))
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
                    logging.error("do_compare(): %s" % str(e))
# always report (warning) mismatched files
                if local_size == remote_size:
                    count_common += 1
                else:
                    print(("%s: local size=%d, remote size=%d" % (item, local_size, remote_size)))
                    count_mismatch += 1
# existence compare
            else:
                count_common += 1

# concluding summary
        print("=== Complete ===")
        print("Files found only on local filesystem = %d" % count_push)
        print("Files found only on remote server = %d" % count_pull)
        print("Identical files = %d" % count_common)
        if checksum is True or filesize is True:
            print("Differing files = %d" % count_mismatch)

# -- generic operation that returns an unknown number of results from the server, so chunking must be used
    def mf_iter(self, iter_command, iter_callback, iter_size):
# NB: we expect cmd to have ":as iterator" in it
        result = self.mf_client.aterm_run(iter_command)
        elem = result.find(".//iterator")
        iter_id = elem.text
        while True:
            logging.debug("Online iterator chunk")
            result = self.mf_client.aterm_run("asset.query.iterate :id %s :size %d" % (iter_id, iter_size))
#  action the callback for this iteration
            iter_callback(result)
# if current iteration is flagged as completed -> we're done
            elem = result.find(".//iterated")
            state = elem.get('complete')
            if "true" in state:
                logging.debug("iteration completed")
                break

# -- callback for do_publish url printing 
    def print_published_urls(self, result):
        for elem in result.iter("path"):
            public_url = '%s://%s/download/%s' % (self.mf_client.protocol, self.mf_client.server, urllib.parse.quote(elem.text[10:]))
            print(public_url)

# --
    def help_publish(self):
        print("\nReturn a public, downloadable URL for a file or files\nRequires public sharing to be enabled by the project administrator\n")
        print("Usage: publish <file(s)>\n")

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
        print("\nRemove public access for a file or files\n")
        print("Usage: unpublish <file(s)>\n")

# --
    def do_unpublish(self, line):
        fullpath = self.absolute_remote_filepath(line)
        pattern = posixpath.basename(fullpath)
        namespace = posixpath.dirname(fullpath)
# un-publish everything that matches
        self.mf_client.aterm_run('asset.query :where "namespace=\'%s\' and name=\'%s\'" :action pipe :service -name asset.label.remove < :label PUBLISHED >' % (namespace, pattern), background=True)

# --
    def help_quit(self):
        print("\nExit without terminating the session\n")
    def do_quit(self, line):
        exit(0)

# --
    def help_exit(self):
        print("\nExit without terminating the session\n")
    def do_exit(self, line):
        exit(0)

# --
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

#            if port == 80:
#                endpoint['encrypt'] = False
#            else:
#                endpoint['encrypt'] = True

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
    my_parser.get_executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
# TODO - put_executor ... or generic ... ?


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
                mf_client = mfclient.mf_client(protocol=endpoint['protocol'], server=endpoint['server'], port=endpoint['port'], domain=endpoint['domain'])

# CLI overrides
                if args.session is not None:
                    endpoint['session'] = args.session
                if args.domain is not None:
                    endpoint['domain'] = args.domain

# TODO - remotes to accept endpoint as initialiser
                if 'session' in endpoint:
                    mf_client.session = endpoint['session']
                if 'token' in endpoint:
                    mf_client.token = endpoint['token']
                if 'domain' in endpoint:
                    mf_client.domain = endpoint['domain']

# connect
                my_parser.remotes_add(mount, mf_client)

            elif endpoint['type'] == 's3':
                client = s3client.s3client(host=endpoint['host'], access=endpoint['access'], secret=endpoint['secret'])
                my_parser.remotes_add(mount, client)

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
