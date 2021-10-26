#!/usr/bin/env python3

import sys
import os
import cmd
import glob
import math
import time
import json
import shlex
import logging
import pathlib
import posixpath
import threading
import configparser
import concurrent.futures
import xml.etree.ElementTree as ET
import mfclient
import s3client

#------------------------------------------------------------
# picklable get()
def jump_get(remote, remote_filepath, local_filepath):
    try:
        size = remote.get(remote_filepath, local_filepath)
        logging.info("Local file size (bytes) = %r" % size)
    except Exception as e:
        logging.error(str(e))
        size = 0
    return size

#------------------------------------------------------------
# picklable put()
def jump_put(remote, remote_fullpath, local_fullpath, metadata=False):
    try:
# FIXME - should be generic ... bit too MFLUX specific
        asset_id = remote.put(remote_fullpath, local_fullpath)
        size = os.path.getsize(local_fullpath)
# import metadata if required
        if metadata:
            metadata_filepath = local_fullpath+".meta"
            if os.path.isfile(metadata_filepath):
                remote.import_metadata(asset_id, metadata_filepath)
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
    remotes_current = None
    remotes = {}
    cwd = '/'
    interactive = True
    terminal_height = 20
    script_output = None
    thread_executor = None
    thread_max = 4
    get_count = 0
    get_bytes = 0
    total_count = 0
    total_bytes = 0
    put_count = 0
    put_bytes = 0
    logging = logging.getLogger('parser')

# --- initial setup 
    def preloop(self):
        self.prompt = "%s:%s>" % (self.remotes_current, self.cwd)

# --- not logged in -> don't even attempt to process remote commands
    def precmd(self, line):
        return cmd.Cmd.precmd(self, line)

# --- prompt refresh 
    def postcmd(self, stop, line):
        self.prompt = "%s:%s>" % (self.remotes_current, self.cwd)
        return cmd.Cmd.postcmd(self, stop, line)

# ---
    def emptyline(self):
        return

# ---
    def default(self, line):
        remote = self.remotes[self.remotes_current]
        remote.command(line)

# TODO - complete for local commands?
# ---
    def complete_get(self, text, line, start_index, end_index):
        return self.remotes_complete(line[4:end_index], start_index-4)

# ---
    def complete_rm(self, text, line, start_index, end_index):
        return self.remotes_complete(line[3:end_index], start_index-3)

# ---
    def complete_file(self, text, line, start_index, end_index):
        return self.remotes_complete(line[5:end_index], start_index-5)

# ---
    def complete_publish(self, text, line, start_index, end_index):
        return self.remotes_complete(line[8:end_index], start_index-8)

# ---
    def complete_ls(self, text, line, start_index, end_index):
        return self.remotes_complete(line[3:end_index], start_index-3)

# ---
    def complete_cd(self, text, line, start_index, end_index):
        return self.remotes_complete(line[3:end_index], start_index-3, file_search=False)

# ---
    def complete_rmdir(self, text, line, start_index, end_index):
        return self.remotes_complete(line[6:end_index], start_index-6, file_search=False)

# ---
    def complete_remote(self, text, line, start_index, end_index):
#        self.logging.info("text=[%s] line=[%s] start_index=[%d] end_index=[%d]" % (text, line, start_index, end_index))
        candidates = []
        for name in self.remotes:
            if name.startswith(text):
                candidates.append(name)
        return candidates

#------------------------------------------------------------
    def remotes_complete(self, partial, start, file_search=True, folder_search=True):
        try:
            candidate_list = []
            remote = self.remotes[self.remotes_current]
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
    def remotes_add(self, name, endpoint):
        try:
            self.logging.info("Creating remote name = [%s] type = [%s]" % (name, endpoint['type']))
# create client
            if endpoint['type'] == 'mflux':
                client = mfclient.mf_client.from_endpoint(endpoint)
            elif endpoint['type'] == 's3':
                client = s3client.s3_client.from_endpoint(endpoint)
# register in parser
            self.remotes[name] = client
# get connection status
            client.connect()
# register in config
            if self.config is not None:
                endpoints = json.loads(self.config.get(self.config_name, 'endpoints'))
                endpoints[name] = client.endpoint()
                self.config.set(self.config_name, 'endpoints', json.dumps(endpoints))

        except Exception as e:
            self.logging.error(str(e))

#------------------------------------------------------------
    def abspath(self, path):
        self.logging.info("input path=[%s]" % path)

        if path.startswith('"') and path.endswith('"'):
            path = line[1:-1]

        if posixpath.isabs(path) is False:
            path = posixpath.normpath(posixpath.join(self.cwd, path))

        self.logging.info("output path=[%s]" % path)
        return path

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
# replace any trailing / that may have been removed - important for S3 prefixes
        if line[-1] == '/':
            fullpath = fullpath+'/'
        return fullpath

#------------------------------------------------------------
    def help_file(self):
        print("\nReturn metadata information on a remote file\n")
        print("Usage: file <filename>\n")

    def do_file(self, line):
        remote = self.remotes[self.remotes_current]
        fullpath = self.abspath(line)
        remote.info(fullpath)
        for key, value in remote.info(fullpath).items():
            print("%10s : %s" % (key, value))

#------------------------------------------------------------
    def help_remote(self):
        print("\nInformation about remote clients\n")
        print("Usage: remote <name>\n")
        print("Usage: remote <add /mount type URL>\n")

# --- 
    def do_remote(self, line):

        # CURRENT
        if line in self.remotes:
            self.remotes_current = line
            remote = self.remotes[line]
            self.cwd = "/"
            return


        if 'add' in line:

            args = line.split()
            if len(args) != 4:
                raise Exception("Expected command of the form: remotes add /mount type URL")

            mount = args[1]
            remote_type = args[2]
            remote_url = args[3]

            print("remote [%s] server of type [%s] with URL [%s]" % (mount, remote_type, remote_url))

            self.remotes_add(mount, {'type':remote_type, 'url':remote_url})

        else:
            for name, client in self.remotes.items():
                print("%-20s %s" % (name, client.status))

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
            self.logging.info("Attempting discovery via: [%s]" % self.keystone)
# find a mediaflux client
# use as SSO for keystone auth
# do discovery
            remote = self.remotes[self.remotes_current]
            endpoint = remote.endpoint()
            if endpoint['type'] == 'mflux':
                self.logging.info("Attempting SSO via: [%r]" % remote)
            else:
# TODO - do an old fashioned login instead
                raise Exception("No valid SSO client found")

            try:
                self.keystone.connect(remote, refresh=True)
                endpoint = self.keystone.discover_s3_endpoint()
                self.remotes_add(endpoint['name'], endpoint)
                self.remotes_config_save()
            except Exception as e:
                self.logging.info(str(e))
# probably no boto3 - may have still got credentials though
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

        fullpath = self.abspath(line)
        remote = self.remotes[self.remotes_current]

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

# --- helper
# --
    def print_over(self, text):
        sys.stdout.write("\r"+text)
        sys.stdout.flush()

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

#        line = self.absolute_remote_filepath(line)
#        remote = self.remotes_get(line)

        remote = self.remotes[self.remotes_current]
        abspath = self.abspath(line)

        if remote is not None:
#            results = remote.get_iter(line)
            results = remote.get_iter(abspath)
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
                self.logging.info("[%s] count = %d" % (str(e), count))

# FIXME - throws exception "completed" when done - but no problem occurred ... 
                pass

# TODO - control-C -> terminate threads ...
#self.thread_executor.shutdown(wait=True, cancel_futures=True)

# wait until completed (cb_get does progress updates)
            self.logging.info("Waiting for thread executor...")
            while self.get_count < self.total_count:
                time.sleep(3)

# print summary and return
            elapsed = time.time() - start_time
            rate = float(self.total_bytes) / float(elapsed)
            rate = rate / 1000000.0
            self.print_over("Completed get: %d files, total size: %s, speed: %.1f MB/s   \n" % (self.get_count, self.human_size(self.get_bytes), rate))

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
    def put_iter(self, line, metadata=False):
        if os.path.isdir(line):
            self.logging.info("Walking directory tree...")
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
                        yield remote_fullpath, fullpath
        else:
            self.logging.info("Building file list... ")
            for name in glob.glob(line):
                local_fullpath = os.path.abspath(name)
                ignore = False
                if metadata:
                    if name.endswith(".meta"):
                        ignore = True
                if ignore is False:
                    yield self.cwd, local_fullpath

# --
    def do_put(self, line, metadata=False):

        self.logging.info("[%s]" % line)
#        remote = self.remotes_get(self.cwd)
        remote = self.remotes[self.remotes_current]

        try:
            self.put_count = 0
            self.put_bytes = 0
            total_count = 0
            start_time = time.time()

            results = self.put_iter(line, metadata=metadata)

            for remote_fullpath, local_fullpath in results:
                self.logging.info("put remote=[%s] local=[%s]" % (remote_fullpath, local_fullpath))
                future = self.thread_executor.submit(jump_put, remote, remote_fullpath, local_fullpath, metadata=metadata)
                future.add_done_callback(self.cb_put)
                total_count += 1

        except Exception as e:
            self.logging.error(str(e))
            pass

# wait until completed (cb_put does progress updates)
        while self.put_count < total_count:
            time.sleep(3)

# print summary and return
        elapsed = time.time() - start_time
        rate = float(self.put_bytes) / float(elapsed)
        rate = rate / 1000000.0
        self.print_over("Completed put: %d files, total size: %s, speed: %.1f MB/s   \n" % (self.put_count, self.human_size(self.put_bytes), rate))

#------------------------------------------------------------
    def do_copy(self, line):
        self.logging.info("in: %s" % line)

# TODO - options for metadata copy as well (IF src = mflux)
#        option_list, tail = getopt.getopt(line, "r")
#        self.logging.info("options: %r" % option_list)
#        self.logging.info("tail: %r" % tail)

        try:
            path_list = shlex.split(line, posix=True)
            self.logging.info("copy [%r]" % path_list)

        except Exception as e:
            self.logging.debug(str(e))

        if len(path_list) != 2:
            raise Exception("Expected exactly two path arguments: source and destination")

# source location is the remote client that controls the copy
#        from_abspath = self.absolute_remote_filepath(path_list[0])
#        to_abspath = self.absolute_remote_filepath(path_list[1])
#        source = self.remotes_get(from_abspath)
#        destination = self.remotes_get(to_abspath)
#        source.copy(from_abspath, to_abspath, destination, prompt=self.ask)


#------------------------------------------------------------
# adapted from s3client
    def unc_split(self, fullpath):
        self.logging.info("[%s]" % fullpath)

# convert fullpath to [bucket][object]
        mypath = pathlib.PurePosixPath(fullpath)
        host = None 
        path = "/"
        count = len(mypath.parts)

        if count > 1:
            host = "%s%s" % (mypath.parts[0], mypath.parts[1])

        i=2
        while i<count:
            path = posixpath.join(path, mypath.parts[i])
            i += 1

        self.logging.info("host=[%r] path=[%r]" % (host, path))

        return host, path

#------------------------------------------------------------
    def help_cd(self):
        print("\nChange the current remote folder\n")
        print("Usage: cd <folder>\n")

    def do_cd(self, line):
        remote = self.remotes[self.remotes_current]
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
        remote = self.remotes[self.remotes_current]
        remote.mkdir(ns_target)

#------------------------------------------------------------
    def help_rm(self):
        print("\nDelete remote file(s)\n")
        print("Usage: rm <file or pattern>\n")

    def do_rm(self, line):
        abspath = self.abspath(line)
        remote = self.remotes[self.remotes_current]
        if remote.rm(abspath, prompt=self.ask) is False:
            print("Delete aborted")

#------------------------------------------------------------
    def help_rmdir(self):
        print("\nRemove a remote folder\n")
        print("Usage: rmdir <folder>\n")

    def do_rmdir(self, line):
        ns_target = self.abspath(line)
        remote = self.remotes[self.remotes_current]

        if self.ask("Remove folder: %s (y/n) " % ns_target):
            remote.rmdir(ns_target)
        else:
            print("Aborted")

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
            self.logging.info(str(e))
            pass
        print("Current number of processes: %r" % self.thread_max)

#------------------------------------------------------------
    def help_logout(self):
        print("\nTerminate the current session to the server\n")
        print("Usage: logout\n")

# ---
    def do_logout(self, line):
        remote = self.remotes[self.remotes_current]
        if remote is not None:
            remote.logout()
            self.remotes_config_save()

#------------------------------------------------------------
    def help_login(self):
        print("\nInitiate login to the current remote server\n")
        print("Usage: login\n")

# ---
    def do_login(self, line):
        remote = self.remotes[self.remotes_current]
        if remote is not None:
            remote.login()
            self.remotes_config_save()

#------------------------------------------------------------
    def help_delegate(self):
        print("\nCreate a credential, stored in your local home folder, for automatic authentication to the remote server.")
        print("An optional argument can be supplied to set the lifetime, or off to destroy all your delegated credentials.\n")
        print("Usage: delegate <days/off>\n")

# ---
    def do_delegate(self, line):
        remote = self.remotes[self.remotes_current]
        if remote.delegate(line) is True:
            self.remotes_config_save()

#------------------------------------------------------------
    def help_publish(self):
        print("\nCreate public URLs for specified file(s)\nRequires public sharing to be enabled by the project administrator\n")
        print("Usage: publish <file(s) or folder>\n")

# --
    def do_publish(self, line):
        fullpath = self.abspath(line)
        remote = self.remotes[self.remotes_current]
        count = remote.publish(fullpath)
        print("Published %d files" % count)

#------------------------------------------------------------
    def help_unpublish(self):
        print("\nRemove public access for a file or files\n")
        print("Usage: unpublish <file(s) or folder>\n")

# --
    def do_unpublish(self, line):
        fullpath = self.abspath(line)
        remote = self.remotes[self.remotes_current]
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

