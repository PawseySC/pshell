#!/usr/bin/env python3

import os
import cmd
import sys
import glob
import math
import time
import json
import logging
import posixpath
import threading
import concurrent.futures
import mfclient
import s3client

#------------------------------------------------------------
class parser(cmd.Cmd):
    config = None
    config_name = None
    config_filepath = None
    remotes_current = None
    remotes = {}
    cwd = '/'
    interactive = True
    terminal_height = 20
    script_output = None
    thread_executor = None
    thread_max = 3

# documentation headers
    doc_header = "Standard commands"
    misc_header = "Specialised commands"

# NEW - unified (get/put/cp) progress reporter
    progress_start_time = 0
    progress_total_items = 0
    progress_total_bytes = 0
    progress_completed_items = 0
    progress_completed_bytes = 0
    progress_running = 0
    progress_skipped = 0
    progress_errors = 0

    logging = logging.getLogger('parser')

# --- command parsing hooks
    def preloop(self):
# set the initial prompt
        self.prompt = "%s:%s>" % (self.remotes_current, self.cwd)
# ---
    def precmd(self, line):
# if command requires authentication, and not yet authenticated, print remote status and don't execute
        if self.requires_auth(line):
            remote = self.remote_active()
            if remote.status != "authenticated":
                print(remote.status)
                return ""
# normal command execution
        return cmd.Cmd.precmd(self, line)
# ---
    def postcmd(self, stop, line):
# set prompt again - in case the remote/cwd changed
        self.prompt = "%s:%s>" % (self.remotes_current, self.cwd)
        return cmd.Cmd.postcmd(self, stop, line)
# ---
    def emptyline(self):
        return
# ---
    def default(self, line):
        remote = self.remote_active()
        try:
# unknown command - passthrough to remote client custom handler
            remote.command(line)
        except Exception as e:
            self.logging.debug(str(e))
# ensure pshell passes exit code on error test when bombing out on malformed commands
            raise SyntaxError
# FIXME - running this will cause pshell to bomb when processing the mflux error response
# eg: asset.query :namespace "/projects/Data Team" :where "name='*.NEF''" :action count


# TODO - implement completion for local commands?
# ---
    def complete_get(self, text, line, start_index, end_index):
        return self.remote_complete(line[4:end_index], start_index-4)

# ---
    def complete_rm(self, text, line, start_index, end_index):
        return self.remote_complete(line[3:end_index], start_index-3)

# ---
    def complete_file(self, text, line, start_index, end_index):
        return self.remote_complete(line[5:end_index], start_index-5)
# ---
    def complete_info(self, text, line, start_index, end_index):
        return self.remote_complete(line[5:end_index], start_index-5)

# ---
    def complete_publish(self, text, line, start_index, end_index):
        return self.remote_complete(line[8:end_index], start_index-8)

# ---
    def complete_ls(self, text, line, start_index, end_index):
        return self.remote_complete(line[3:end_index], start_index-3)

# ---
    def complete_cd(self, text, line, start_index, end_index):
        return self.remote_complete(line[3:end_index], start_index-3, file_search=False)

# ---
    def complete_rmdir(self, text, line, start_index, end_index):
        return self.remote_complete(line[6:end_index], start_index-6, file_search=False)

#------------------------------------------------------------
    def remote_active(self):
        if self.remotes_current in self.remotes:
            return self.remotes[self.remotes_current]
        raise Exception("No current active remote")

#------------------------------------------------------------
    def remote_complete(self, partial, start, file_search=True, folder_search=True):
        try:
            candidate_list = []
            remote = self.remote_active()
            if "not" in remote.status:
                return

            if file_search is True:
                candidate_list += remote.complete_file(self.cwd, partial, start)
            if folder_search is True:
                candidate_list += remote.complete_folder(self.cwd, partial, start)
        except Exception as e:
            self.logging.error(str(e))

        return candidate_list

#------------------------------------------------------------
    def remotes_config_save(self):
        endpoints = json.loads(self.config.get(self.config_name, 'endpoints'))
        for mount, endpoint in endpoints.items():
            client = self.remotes[mount]
            self.logging.debug("updating mount=[%s] using client=[%r]" % (mount, client))
            if client is not None:
                endpoints[mount] = client.endpoint()
        self.config[self.config_name]['endpoints'] = json.dumps(endpoints)
# commit
        self.logging.info("saving config to file = %s" % self.config_filepath)
        with open(self.config_filepath, 'w') as f:
            self.config.write(f)

#------------------------------------------------------------
    def remote_add(self, name, endpoint):
        try:
            client = None
            self.logging.debug("remote: [%s] endpoint: %r" % (name, endpoint))
# create client
            if endpoint['type'] == 'mflux':
                client = mfclient.mf_client.from_endpoint(endpoint)
            elif endpoint['type'] == 's3':
                client = s3client.s3_client.from_endpoint(endpoint)
            else:
                raise Exception("Unknown endpoint type=%s" % endpoint['type'])

# if configured successfully, register in hash table
            if client is not None:
                self.remotes[name] = client
            else:
                raise Exception("Failed to configure remote client.")
# save in config
            if self.config is not None:
                endpoints = json.loads(self.config.get(self.config_name, 'endpoints'))
                endpoints[name] = client.endpoint()
                self.config.set(self.config_name, 'endpoints', json.dumps(endpoints))

        except Exception as e:
            self.logging.error(str(e))

#------------------------------------------------------------
    def remote_del(self, name):
        try:
# remove remote entry
# TODO - logic is a bit convoluted ... merge self.remotes[] and endpoints in some way?
            del self.remotes[name]
            self.logging.info("Deleted remote [%s]" % name)
            endpoints = json.loads(self.config.get(self.config_name, 'endpoints'))
            del endpoints[name]
            self.config.set(self.config_name, 'endpoints', json.dumps(endpoints))
# update stored config
            self.remotes_config_save()
        except Exception as e:
            self.logging.debug(str(e))
            self.logging.error("No such remote [%s]" % name)

#------------------------------------------------------------
    def remote_set(self, name, home='/'):
        try:
            remote = self.remotes[name]
            remote.connect()
            self.remotes_current = name
            self.cwd = home 
            self.config.set(self.config_name, 'remotes_current', name)
            self.config.set(self.config_name, 'remotes_home', home)
            with open(self.config_filepath, 'w') as f:
                self.config.write(f)
        except Exception as e:
            self.logging.debug(str(e))
            self.logging.error("No such remote [%s]" % name)

#------------------------------------------------------------
    def remote_info(self, name):
        try:
            list_info = ['type', 'protocol', 'server', 'port', 'domain', 'url', 'access']
            client = self.remotes[name]
            hash_info = client.endpoint()
            print("name = %s" % name)
            for item in list_info:
                if item in hash_info:
                    print("%s = %s" % (item, hash_info[item]))
        except Exception as e:
            self.logging.debug(str(e))
            self.logging.error("No such remote [%s]" % name)

#------------------------------------------------------------
    def help_remote(self):
        print("\nSelect, add, or delete remote storage locations\n")
        print("Usage: remote <name><type><url> <--remove><--info>\n")
        print("Examples:")
        print("    remote portal --info")
        print("    remote myaws s3 ap-southeast-2")
        print("    remote mystuff mflux https://somewhere.org:8080")
        print("    remote mystuff")

# --- 
    def do_remote(self, line):

# check for flags
        mode = 0
        if "--remove" in line:
            line = line.replace("--remove", "")
            mode=1
        elif "--info" in line:
            line = line.replace("--info", "")
            mode=2
# get args
        args = line.split()
        nargs = len(args)

# list, set, add modes
        if mode == 0:
            if nargs == 0:
                for name, client in self.remotes.items():
                    text = "%-20s [ %-15s ]" % (name, client.status)
                    if name == self.remotes_current:
                        text += " *"
                    print(text)
                return
            elif nargs == 1:
                self.remote_set(args[0])
                return
            elif nargs == 3:
                self.remote_add(args[0], {'type':args[1], 'url':args[2]})
                return
# remove mode
        elif mode == 1:
            if nargs == 1:
                self.remote_del(args[0])
                return
# info mode
        elif mode == 2:
            if nargs == 0:
                self.remote_info(self.remotes_current)
                return
            elif nargs == 1:
                self.remote_info(args[0])
                return

        raise Exception("Bad command, help available by typing: help remote")

#------------------------------------------------------------
    def abspath(self, line):
        if line.startswith('"') and line.endswith('"'):
            line = line[1:-1]
# convert blank entry to cwd (which should have a trailing / for S3 reasons)
        if line == "":
            line = self.cwd
# convert relative path to absolute
        if posixpath.isabs(line) is False:
            path = posixpath.normpath(posixpath.join(self.cwd, line))
        else:
            path = posixpath.normpath(line)
# enforce trailing / removed by normpath - important for S3 prefix handling
        if line.endswith('/') is True:
            if path.endswith('/') is False:
                path = path+'/'
        return path

#------------------------------------------------------------
    def requires_auth(self, line):
        local_commands = ["login", "help", "lls", "lcd", "lpwd", "processes", "remote", "exit", "quit"]
        try:
            primary = line.strip()
            for item in local_commands:
                if primary.startswith(item):
                    return False
        except Exception as e:
            self.logging.error(str(e))
            return False
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
    def print_over(self, text):
        sys.stdout.write("\r"+text)
        sys.stdout.flush()

#------------------------------------------------------------
# TODO - can we cleanly separate thread executor (futures) stuff from the progress wrapper and then redo as class
# thread-safe background task progress helpers for file transfers
    def progress_start(self, total_items, total_bytes=0):

        self.progress_total_items = total_items
        self.progress_total_bytes = total_bytes
        self.progress_completed_items = 0
        self.progress_completed_bytes = 0
        self.progress_running = 0
        self.progress_skipped = 0
        self.progress_errors = 0

# long tail to cleanup any background task running/completed messages
        self.print_over("Preparing %d files...                          " % total_items)
# NEW - rename to something less silly...
        self.progress_start_time = time.time()

#---
    def progress_item_add(self, future):
        future.add_done_callback(self.progress_item_completed)
        with threading.Lock():
            self.progress_running += 1

#---
    def progress_item_completed(self, future):
        error = 0
        skip = 0
        try:
            code = int(future.result())
            if code < 0:
                skip = 1
        except Exception as e:
# NB: can get some really strange stack traces here
            self.logging.debug(str(e))
            error = 1
# update
        with threading.Lock():
            self.progress_completed_items += 1
            self.progress_running -= 1
            self.progress_skipped += skip
            self.progress_errors += error

#---
    def progress_byte_chunk(self, chunk):
        with threading.Lock():
            self.progress_completed_bytes += int(chunk)

#---
    def progress_display(self):

        elapsed = time.time() - self.progress_start_time

# avoid naughtiness
        if elapsed == 0:
            elapsed = 1
        if self.progress_total_bytes == 0:
            self.progress_total_bytes = 1

        rate = float(self.progress_completed_bytes) / float(elapsed)
        rate = rate / 1000000.0
        progress_pc = 100.0 * float(self.progress_completed_bytes) / float(self.progress_total_bytes)

        msg = "progress=%3.1f%%, " % progress_pc
        msg+= "%d/%d files, " % (self.progress_completed_items-self.progress_errors, self.progress_total_items)
        msg += "errors=%s, " % self.progress_errors
        msg += "skipped=%s, " % self.progress_skipped
        msg += "running=%s, " % self.progress_running
        msg += "overall rate=%.2f MB/s                " % rate

        self.print_over(msg)

#---
# size = 0 -> drain running to 0, else drain while running > size
    def progress_throttle(self, size=0, wait=5):
        self.progress_display()
        while self.progress_running > size:
            time.sleep(wait)
            self.progress_display()

#------------------------------------------------------------
    def help_info(self):
        print("\nReturn information for a remote file or folder\n")
        print("Usage: info <filename/folder>\n")
# --- 
    def do_info(self, line):
        remote = self.remote_active()
        fullpath = self.abspath(line)
        for item in remote.info_iter(fullpath):
            print(item)

#------------------------------------------------------------
# immediately return any key pressed as a character
    def wait_key(self):
        result = None
        if self.interactive is False:
            return result
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
#                    print("got [%r]" % key)
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

        return result

#------------------------------------------------------------
    def help_ls(self):
        print("\nList files stored on the remote server.")
        print("Navigation in paginated output can be achieved by entering a page number, [enter] for next page or q to quit.\n")
        print("Usage: ls <file pattern or folder name>\n")

# --- 
    def do_ls(self, line):

        fullpath = self.abspath(line)
        remote = self.remote_active()

        remote_list = remote.ls_iter(fullpath)
        count = 0
        size = max(1, min(self.terminal_height - 3, 100))
        for line in remote_list:
            print(line)
            count = count+1
            if count > size:
                response = self.pagination_controller(" ------- (enter = next page, q = quit) ------- ")
                if response is not None:
                    if response == 'q' or response == 'quit':
                        return
                    else:
                        count = 0

#------------------------------------------------------------
    def help_import(self):
        print("\nUpload files or folders with associated metadata")
        print("For every file <filename.ext> another file called <filename.ext.meta> should contain metadata in INI file format\n")
        print("Usage: import <file or folder>\n")

# ---
    def do_import(self, line):
        self.do_put(line, metadata=True)

#------------------------------------------------------------
    def help_get(self):
        print("\nDownload remote files to the current local folder\n")
        print("Usage: get <remote files or folders>\n")

# --
    def do_get(self, line):
        if len(line) == 0:
            raise Exception("Nothing specified to get")

# turn input line into a matching file iterator
        remote = self.remote_active()
        abspath = self.abspath(line)

        if remote is not None:
            results = remote.get_iter(abspath)
            total_count = int(next(results))
            total_bytes = int(next(results))
            self.progress_start(total_count, total_bytes)

            try:
# define batch limit
                batch_size = self.thread_max * 2 - 1
# TODO - redo as a queue instead of iter? 
# TODO - should allow better progress reporting as we're not stuck in mfclient waiting for a recall
                for remote_fullpath in results:
# TODO - this needs a tweak so we don't get the intermediate directories ...
                    remote_relpath = posixpath.relpath(path=remote_fullpath, start=self.cwd)
                    local_filepath = os.path.join(os.getcwd(), remote_relpath)
                    future = self.thread_executor.submit(remote.get, remote_fullpath, local_filepath, self.progress_byte_chunk)
                    self.progress_item_add(future)
# NEW - don't submit any more than the batch size - this allows for faster cleanup of threads
                    self.progress_throttle(batch_size)

            except Exception as e:
                self.logging.error(str(e))
                pass

# wait until completed 
            self.progress_throttle()
            print("")

# non-zero exit code on termination if there were any errors
            if self.progress_errors > 0:
                raise Exception("get: download failed for %d file(s)" % self.progress_errors)

#------------------------------------------------------------
    def help_put(self):
        print("\nUpload local files or folders to the current folder on the remote server\n")
        print("Usage: put <file or folder>\n")

# --
    def put_iter(self, line, metadata=False, setup=False):
        count = 0
        size = 0
        if os.path.isdir(line):
            self.logging.info("Analysing directories...")
            line = os.path.abspath(line)
            parent = os.path.normpath(os.path.join(line, ".."))
            for root, directory_list, name_list in os.walk(line):
                local_relpath = os.path.relpath(path=root, start=parent)
                relpath_list = local_relpath.split(os.sep)
                remote_relpath = "/".join(relpath_list)
                remote_fullpath = posixpath.join(self.cwd, remote_relpath)
                for name in name_list:
                    ignore = False
                    if metadata:
                        if name.endswith(".meta"):
                            ignore = True
                    if ignore is False:
                        fullpath = os.path.normpath(os.path.join(os.getcwd(), root, name))
                        if setup is False:
                            yield remote_fullpath, fullpath
                        else:
                            count += 1
                            size += int(os.path.getsize(fullpath))
        else:
            self.logging.info("Building file list... ")
            for name in glob.glob(line):
                local_fullpath = os.path.abspath(name)
                ignore = False
                if metadata:
                    if name.endswith(".meta"):
                        ignore = True
                if ignore is False:
                    if setup is False:
                        yield self.cwd, local_fullpath
                    else:
                        count += 1 
                        size += int(os.path.getsize(local_fullpath))

        if setup is True:
            yield count
            yield size

# --
    def do_put(self, line, metadata=False):
        if len(line) == 0:
            raise Exception("Nothing specified to put")

        self.logging.info("[%s]" % line)
        remote = self.remote_active()
        try:

# determine size of upload
            self.print_over("put: analysing...")
            results = self.put_iter(line, metadata=metadata, setup=True)
            total_count = next(results)
            total_bytes = next(results)

# iterate over upload items
            self.progress_start(total_count, total_bytes)
            results = self.put_iter(line, metadata=metadata)
            batch_size = self.thread_max * 2 - 1
            for remote_fullpath, local_fullpath in results:
                future = self.thread_executor.submit(remote.put, remote_fullpath, local_fullpath, cb_progress=self.progress_byte_chunk, metadata=metadata)
                self.progress_item_add(future)
                self.progress_throttle(batch_size)

        except Exception as e:
            self.logging.error(str(e))
            pass

# wait until completed (cb_put does progress updates)
        self.progress_throttle()
        print("")

        if self.progress_errors > 0:
            raise Exception("put: upload failed for %d file(s)" % self.progress_errors)

#------------------------------------------------------------
#    def help_cp(self):
#        print("\nCopy folders/files from the current remote to a folder in a different remote.")
#        print("Currently only supports copying data from Mediaflux to S3\n")
#        print("Usage: cp <folder/file> acacia:/folder\n")
#------------------------------------------------------------
# use:  copy file/folder remote2:/abs/path
# NB: the current remote must be MFLUX and the destination remote must be S3
#    def do_cp(self, line, skip=True):
# CURRENT - WIP
    def todo_cp(self, line, skip=True):

        remote = self.remote_active()
        try:
            ix1 = line.find(':')
            ix2 = line[:ix1].rindex(' ')
            src = self.abspath(line[:ix2])
            uri = line[ix2+1:]
            to = uri.partition(":")

# TODO - if remote doesn't exist -> may not be an issue -> really only need an s3.client.host
# HOWEVER - if we're doing some form of S3 skip-if-exists we will need S3 remote object methods
            if to[0] in self.remotes:
                to_remote = self.remotes[to[0]]
            else:
                to_remote = None
# TODO - if skip is true -> log that we can't do the existence check and set skip to false

            to_root = to[2]

# check and/or or configure the s3 client host
            remote.copy_host_setup(to[0], to_remote)

        except Exception as e:
            self.logging.error(str(e))
            raise Exception("Invalid source or remote destination")

# main call to get source, destination pairs for the copy
        results = remote.copy_iter(src, to_root)
        total_items = int(next(results))
        total_bytes = int(next(results))
# raise if nothing to do
        if total_items == 0 or total_bytes == 0:
            raise Exception("Nothing to copy")

# start
        self.progress_start(total_items, total_bytes)
        try:
            batch_size = self.thread_max * 2 - 1
            for src, dest in results:

# NEW - use info() to check if already exists
                if skip is True:
                    filename = posixpath.basename(src)
                    remote_fullpath = posixpath.join(dest, filename)
                    self.logging.info("check src=%s, dest=%s, remote_fullpath=%s" % (src, dest, remote_fullpath))
# TODO - change to -> if skip is true
                if to_remote is not None:
                    exist_check = to_remote.info_iter(remote_fullpath)
                    exist_count = int(next(exist_check))
                    exist_size = int(next(exist_check))
                    self.logging.info("count=%d, size=%d" % (exist_count, exist_size))
# TODO - if count is not 0 -> already exists, so can skip ... 
# NB: probably too expensive to get src size and compare ... should bake into original remote.copy_iter() if we want this


# submit and throttle against the batch_size - allows for faster cleanup of threads (if cancelled)
                future = self.thread_executor.submit(remote.copy, src, to[0], dest, cb_progress=self.progress_byte_chunk)
                self.progress_item_add(future)
                self.progress_throttle(size=batch_size)

        except Exception as e:
            self.logging.error(str(e))
            pass

# wait until completed (cb_get does progress updates)
        self.logging.info("Waiting for background downloads...")

        self.progress_throttle()

# newline
        print("")

# TODO - progress call to trigger this
# non-zero exit code on termination if there were any errors
        if self.progress_errors > 0:
            raise Exception("copy: failed for %d file(s)" % self.progress_errors)

#------------------------------------------------------------
    def help_cd(self):
        print("\nChange the current remote folder\n")
        print("Usage: cd <folder>\n")

    def do_cd(self, line):
        remote = self.remote_active()
        abspath = self.abspath(line)
        self.cwd = remote.cd(abspath)

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
        ns_target = self.abspath(line)
        remote = self.remote_active()
        remote.mkdir(ns_target)

#------------------------------------------------------------
    def help_rm(self):
        print("\nDelete remote file(s)\n")
        print("Usage: rm <file or pattern>\n")

# TODO - rework as _iter() implementation ... although that will be inefficient for MFLUX
    def do_rm(self, line):
        abspath = self.abspath(line)
        remote = self.remote_active()
        if remote.rm(abspath, prompt=self.ask) is False:
            print("rm aborted")

#------------------------------------------------------------
    def help_rmdir(self):
        print("\nRemove a remote folder\n")
        print("Usage: rmdir <folder>\n")

    def do_rmdir(self, line):
        ns_target = self.abspath(line)
        remote = self.remote_active()
        if remote.rmdir(ns_target, prompt=self.ask) is False:
            print("rmdir aborted")

#------------------------------------------------------------
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
            self.logging.debug(str(e))
            pass
        print("Current number of processes: %r" % self.thread_max)

#------------------------------------------------------------
    def help_logout(self):
        print("\nTerminate the current session to the server\n")
        print("Usage: logout\n")

# ---
    def do_logout(self, line):
        remote = self.remote_active()
        if remote is not None:
            remote.logout()
            self.remotes_config_save()

#------------------------------------------------------------
    def help_login(self):
        print("\nInitiate login to the current remote server\n")
        print("Usage: login\n")

# ---
    def do_login(self, line):
        remote = self.remote_active()
        remote.login()
        self.remotes_config_save()

#------------------------------------------------------------
    def help_publish(self):
        print("\nCreate public URLs for specified file(s)\nRequires public sharing to be enabled by the project administrator\n")
        print("Usage: publish <file(s) or folder>\n")

# --
    def do_publish(self, line):
        fullpath = self.abspath(line)
        remote = self.remote_active()
        count = remote.publish(fullpath)
        print("Published %d item(s)" % count)

#------------------------------------------------------------
    def help_unpublish(self):
        print("\nRemove public access for a file or files\n")
        print("Usage: unpublish <file(s) or folder>\n")

# --
    def do_unpublish(self, line):
        fullpath = self.abspath(line)
        remote = self.remote_active()
        count = remote.unpublish(fullpath)
        print("Unpublished %d item(s)" % count)

#------------------------------------------------------------
    def help_whoami(self):
        print("\nReport the current authenticated user or delegate and associated roles\n")
        print("Usage: whoami\n")

# --
    def do_whoami(self, line):
        remote = self.remote_active()
        for line in remote.whoami():
            print(line)

#------------------------------------------------------------
    def help_quit(self):
        print("\nExit without terminating the session\n")
    def do_quit(self, line):
        self.remotes_config_save()
        sys.exit(0)

# --
    def help_exit(self):
        print("\nExit without terminating the session\n")
    def do_exit(self, line):
        self.remotes_config_save()
        sys.exit(0)

#------------------------------------------------------------
# MISC section - mfclient specific commands
    def help_delegate(self):
        print("NOTE: This command is only available for Mediaflux remotes.\n")
        print("Create a credential, stored in your local home folder, for automatic authentication to the Mediaflux server.")
        print("An optional argument can be supplied to set the lifetime, or off to destroy all your delegated credentials.\n")
        print("Usage: delegate <days><off>\n")
# ---
#    def do_delegate(self, line):
#        remote = self.remote_active()
#        if remote.delegate(line) is True:
#            self.remotes_config_save()

#------------------------------------------------------------
# MISC section - s3client specific commands
    def help_lifecycle(self):
        print("NOTE: This command is only available for S3 remotes.\n")
        print("Usage: lifecycle bucket (+-)(mv) <days> (--review)(--restore)")
# ---
    def help_policy(self):
        print("NOTE: This command is only available for S3 remotes.\n")
        print("Usage: policy bucket (+-)(rw)(comma-separated user list)\n")

#------------------------------------------------------------
    def loop_interactively(self):
        while True:
            try:
                self.cmdloop()

            except KeyboardInterrupt:
                self.print_over(" Interrupting, please wait... ")
                remote = self.remote_active()
# signal running threads to terminate ...
# TODO: implemented interrupting for mfclient, but not s3client ...
                remote.polling(False)
# wait until threads have terminated
                self.thread_executor.shutdown()
# start thread machinery back up
                self.thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_max)
                remote.polling(True)
                print("done ")

# NB: here's where command failures are caught
            except FileNotFoundError:
                print("File not found.")

            except SyntaxError:
                print("Syntax error.")

            except Exception as e:
# exit on the EOF case ie where stdin/file is force fed via command line redirect
# FIXME - this can sometimes occur in some mediaflux error messages
                if "EOF" in str(e):
                    print("Exit: encountered EOF")
                    return
                print(str(e))

