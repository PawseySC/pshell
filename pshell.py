#!/usr/bin/python

import os
import re
import cmd
import sys
import glob
import math
import time
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
	mf_fast = None
	cwd = '/projects'

# --- init global
	def preloop(self):
		if self.mf_client.authenticated():
			self.need_auth = False
			self.prompt = "%s:%s>" % (self.config_name, self.cwd)
		else:
			self.need_auth = True
			self.prompt = "%s:offline>" % self.config_name

# --- if not logged in -> don't even attempt to process remote commands
	def precmd(self, line):
		if self.need_auth:
			self.prompt = "%s:offline>" % self.config_name
			if self.requires_auth(line):
				print "Not logged in."
				return cmd.Cmd.precmd(self, "")
		else:
			self.prompt = "%s:%s>" % (self.config_name, self.cwd)

		return cmd.Cmd.precmd(self, line)

# --- init for each
	def postcmd(self, stop, line):
		if self.need_auth:
			self.prompt = "%s:offline>" % self.config_name
		else:
			self.prompt = "%s:%s>" % (self.config_name, self.cwd)

		return cmd.Cmd.postcmd(self, stop, line)

# --- helper: attempt to complete a partial namespace with replacement offset = start
	def complete_namespace(self, partial_ns, start):

# construct an absolute namespace (required for any remote lookups)
		if posixpath.isabs(partial_ns):
			candidate_ns = posixpath.normpath(partial_ns)
			isabs = True
		else:
			candidate_ns = posixpath.normpath(posixpath.join(self.cwd, partial_ns))
			isabs = False

		if self.mf_client.namespace_exists(candidate_ns):
# candidate is a namespace -> it's our target for listing
			target_ns = candidate_ns
# no pattern -> add all namespaces 
#			pattern = partial_ns[start:]
			pattern = None
# replacement prefix for any matches
			prefix = partial_ns[start:]
		else:
# candidate not a namespace -> set the parent as the namespace target
			match = re.match(r".*/", candidate_ns)
			if match:
				target_ns = match.group(0)
# extract pattern to search and prefix for any matches
				pattern = candidate_ns[match.end():]
				prefix = partial_ns[start:-len(pattern)]
			else:
				return None
#
# noisy DEBUG
#		print "cn: partial [%s] : target_ns: [%s] : isabs = %r : pattern = %r : prefix = %r" % (partial_ns, target_ns, isabs, pattern, prefix)

# generate listing in target namespace for completion matches
		result = self.mf_client.run("asset.namespace.list", [("namespace", target_ns)])

# noisy DEBUG
#		self.mf_client.xml_print(result)

		ns_list = []
		for elem in result.iter('namespace'):
			if elem.text is not None:
# namespace matches the pattern we're looking for?
				if pattern is not None:
					if elem.text.startswith(pattern):
# construct the full namespace that matches the pattern
						path = posixpath.join(target_ns, elem.text)
# extract the replacement text required to achieve the full namespace
						if isabs:
							ns_list.append(path[start:] + "/")
						else:
							ns_list.append(prefix + elem.text + "/")
				else:
					ns_list.append(posixpath.join(prefix,elem.text+"/"))

#		print "cn: ", ns_list

		return ns_list


# --- helper: attempt to complete an asset
	def complete_asset(self, partial_asset_path, start):

# construct an absolute namespace (required for any remote lookups)
		if posixpath.isabs(partial_asset_path):
			candidate_ns = posixpath.normpath(partial_asset_path)
			isabs = True
		else:
			candidate_ns = posixpath.normpath(posixpath.join(self.cwd, partial_asset_path))
			isabs = False

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

		target_ns = self.safe_namespace_query(target_ns)

#		print "ca: target_ns: [%s] : isabs = %r : pattern = %r : prefix = %r" % (target_ns, isabs, pattern, prefix)

		if pattern is not None:
			result = self.mf_client.run("asset.query", [("where", "namespace='%s' and name like '%s'" % (target_ns, pattern)), ("action", "get-values"), ("xpath ename=\"name\"", "name") ])
		else:
			result = self.mf_client.run("asset.query", [("where", "namespace='%s'" % target_ns), ("action", "get-values"), ("xpath ename=\"name\"", "name") ])

#		self.mf_client.xml_print(result)

		ns_list = []
		for elem in result.iter('name'):
			if elem.text is not None:
				ns_list.append(posixpath.join(prefix,elem.text))

#		print "B", ns_list

		return ns_list


# NB: if the return result is ambigious (>1 option) it'll require 2 presses to get the list
# TODO - can we query matching mediaflux commands for completion???
#	def completedefault(self, text, line, start_index, end_index):
#		list = ["stuff", "things"]
#		return list
# ---
	def complete_get(self, text, line, start_index, end_index):

		save_state = self.mf_client.debug
		self.mf_client.debug = False
# FIXME - only the 1st 50 results returned...
		result = self.mf_client.run("asset.query", [("where", "namespace='%s' and name='%s*'" % (self.safe_cwd(), text)), ("action", "get-values"), ("xpath", "name") ])
		self.mf_client.debug = save_state
		asset_list = []
		for elem in result.iter('value'):
			if elem.text is not None:
				asset_list.append(elem.text)
		return asset_list

# ---
	def complete_rm(self, text, line, start_index, end_index):

		save_state = self.mf_client.debug
		self.mf_client.debug = False
# FIXME - only the 1st 50 results returned...
		result = self.mf_client.run("asset.query", [("where", "namespace='%s' and name='%s*'" % (self.safe_cwd(), text)), ("action", "get-values"), ("xpath", "name") ])
		self.mf_client.debug = save_state
		asset_list = []
		for elem in result.iter('value'):
			if elem.text is not None:
				asset_list.append(elem.text)
		return asset_list

# ---
	def complete_file(self, text, line, start_index, end_index):
# turn off DEBUG -> gets in the way of commandline completion
		save_state = self.mf_client.debug
		self.mf_client.debug = False
		ns_list = self.complete_asset(line[5:end_index], start_index)
		self.mf_client.debug = save_state
		return ns_list

# ---
	def complete_ls(self, text, line, start_index, end_index):
# turn off DEBUG -> gets in the way of commandline completion
# directory
#		print "text=[%s] : line=[%s] : start = %d : end = %d" % (text, line, start_index, end_index)
		save_state = self.mf_client.debug
		self.mf_client.debug = False
		ns_list = self.complete_namespace(line[3:end_index], start_index-3)
# if nothing - see if we can complete an asset instead
		if len(ns_list) == 0:
			try:
				ns_list = self.complete_asset(line[3:end_index], start_index-3)
			except Exception as e:
				print str(e)
		self.mf_client.debug = save_state
		return ns_list

# ---
	def complete_cd(self, text, line, start_index, end_index):
# turn off DEBUG -> gets in the way of commandline completion
		save_state = self.mf_client.debug
		self.mf_client.debug = False
		ns_list = self.complete_namespace(line[3:end_index], start_index-3)
		self.mf_client.debug = save_state
		return ns_list

# ---
	def complete_rmdir(self, text, line, start_index, end_index):
# FIXME - this is currently not working ... strange as complete_cd is working ... and it's the same code
# turn off DEBUG -> gets in the way of commandline completion
		save_state = self.mf_client.debug
		self.mf_client.debug = False
		ns_list = self.complete_namespace(line[6:end_index], start_index-6)
		self.mf_client.debug = save_state
		return ns_list

# ---
	def emptyline(self):
		return

# ---
	def default (self, line):
# pull service call
		match = re.match('^\S+', line)
		if match:
			service_call = match.group(0)
		else:
			print "Nothing to process"
			return

# pull element name list
# TODO - do attributes as well ... when I feel strong enough
		pattern = re.compile(r'(\s:\w+)\s(\S+)')

# TODO - cope with quoted strings/spaces in argument data eg asset.store.describe :name "Data Team" -> use -i script
#		pattern = re.compile(r'(\s:\w+)\s("?)(\S+)(\1)')
#		pattern = re.compile(r'(\s:\w+)\s(["])(?:\\?+.)*?\1')

		list_args = []
		for elem,value in re.findall(pattern, line):
			list_args.append((elem[2:],value))

# DEBUG
#		print list_args
#		return

# generic passthru
		reply = self.mf_client.run(service_call, list_args)
		self.mf_client.xml_print(reply)


# --- helper
	def requires_auth(self, line):
		local_commands = ["login", "help", "lls", "lcd", "lpwd", "debug", "exit", "quit"]

		if not line:
			return False

# only want first keyword (avoid false positives on things like "help get")
		primary = line.strip().split()[0]
		if primary in local_commands:
			return False

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
	def ask(self, text):
		response = raw_input(text)
		if response == 'y' or response == 'Y':
			return True
		return False

# --- helper - I think this is only required if passing self.cwd through an asset.query
	def safe_cwd(self):
		return(self.cwd.replace("'", "\\'"))

# CURRENT - asset.query with namespaces enclosed by ' - must have ' double escaped ... asset.namespace.exists namespaces - must be just single escaped 
# CURRENT - but asset.namespace.list should have no escaping ... thanks Arcitecta
	def safe_namespace_query(self, namespace):
		return(namespace.replace("'", "\\'"))


# -- remote commands

# CURRENT - return asset.get info
	def help_file(self):
		print "Return metadata information on a remote file\n"
		print "Usage: file <filename>\n"

	def do_file(self, line):
		if not posixpath.isabs(line):
			line = posixpath.join(self.cwd, line)
# this works - but encasing the whole thing or line in single or double quotes generates a server error
# TODO - test how this handles spaces
		result = self.mf_client.run("asset.get", [("id", "path=%s" % line)]) 
		self.mf_client.xml_print(result)

# ---
	def help_ls(self):
		print "List files stored on the remote server\n"
		print "Pagination (if required) is controlled by the optional page and size arguments.\n"
		print "Usage: ls <folder> <-p page> <-s size>\n"
		print "Examples: ls /projects/my project/some directory"
		print "          ls -p 2"
		print "          ls\n"

# TODO - default page size -> .mf_config
	def do_ls(self, line):

# process flags (if any)
		page = 1
		size = 20
		list_args = re.findall(r'-\S+\s+\S+', line)
		for arg in list_args:
			if arg.startswith("-p "):
				page = int(arg[2:])
			if arg.startswith("-s "):
				size = int(arg[2:])

# strip out flags - look for directory/filename patterns
		line = re.sub(r'-\S+\s+\S+', '', line)
		line = line.strip()

		asset_query = False
		if len(line) == 0:
			cwd = self.cwd
		else:
# if absolute path exists as a namespace -> query this, else query via an asset pattern match
# FIXME - this will fail if line is already an absolute path
			if posixpath.isabs(line):
				cwd = line
			else:
				cwd = posixpath.normpath(posixpath.join(self.cwd, line))

			if not self.mf_client.namespace_exists(cwd):
				basename = posixpath.basename(cwd)
				cwd = self.safe_namespace_query(posixpath.dirname(cwd))
				asset_query = True

#		print "Remote: %s" % cwd
# query attempt
		pagination_footer = None
		try:
			if asset_query:
				reply = self.mf_client.run("asset.query", [("where", "namespace='%s' and name='%s'" % (cwd, basename)), ("action", "get-values"), ("xpath ename=\"name\"", "name"), ("xpath ename=\"size\"", "content/size") ])
			else:
				reply = self.mf_client.run("www.list", [("namespace", cwd), ("page", page), ("size", size)])

# process pagination information
				for elem in reply.iter('parent'):
					for child in elem:
						if child.tag == "page":
							canonical_page = int(child.text)
						if child.tag == "last":
							canonical_last = int(child.text)
						if child.tag == "size":
							canonical_size = int(child.text)
				if canonical_last > 1:
					pagination_footer = "Displaying %r files per page; page %r of %r" % (canonical_size, canonical_page, canonical_last)

# display results
			for elem in reply.iter('namespace'):
				for child in elem:
					if child.tag == "name":
							print "[directory] %s" % child.text
# TODO - when production updated (new www.list) -> report the online/offline status
# TODO - staging ...
			for elem in reply.iter('asset'):
				line = ""
				for child in elem:
					if child.tag == "name":
						name = child.text
					if child.tag == "size":
						size = child.text
# CURRENT - hmmm, asset with no content ... a problem elsewhere?
						if size is None:
							size = 0

				print "%s | %-s" % (self.human_size(int(size)), name)

# display pagination info
			if pagination_footer is not None:
				print pagination_footer

# fallback if www.list (custom service call) isn't installed on the mediaflux server
		except Exception as e:
			print "ERROR: %s" % str(e)
			print "WARNING: failed to execute custom service call www.list, falling back ..."
			reply = self.mf_client.run("asset.namespace.list", [("namespace", cwd), ("assets", "true")])
# FIXME - do this a bit better
			self.mf_client.xml_print(reply)


# --
	def help_get(self):
		print "Download remote files to the current local working directory\n"
# TODO
#		print "If a folder is selected then an archive (zip) will be downloaded."
		print "Usage: get <remote files or folders>\n"
		print "Examples: get /projects/My Project/images"
		print "          get *.txt\n"


	def do_get(self, line):
		list_asset_filepath = []
		list_local_path = {}
		total_bytes = 0

# FIXME - will fail for things like get Data Team/sean or get Data Team/sean/*.zip -> need to do some unix style path analysis 1st ...
# prefix with CWD -> then unix extract path and basename
# NB: use posixpath for mediaflux namespace manipulation
		if not posixpath.isabs(line):
			line = posixpath.join(self.cwd, line)

# sanitise as asset.query is special
		double_escaped = self.safe_namespace_query(line)
# collapsed namespace
		namespace = posixpath.normpath(posixpath.dirname(double_escaped))
# possible download on asset/pattern
		basename = posixpath.basename(double_escaped)
# possible download on namespace
		candidate = posixpath.join(namespace, basename)

		self.mf_client.log("DEBUG", "do_get(): namespace=[%s] , asset_query=[%s] , candidate_namespace=[%s]" % (namespace, basename, candidate))

# CURRENT - redo to cope with 100 size limit ...

# this requires different escaping to an asset.query
		if self.mf_client.namespace_exists(line):
			args_init = [("where", "namespace >='%s'" % candidate), ("count", "true"), ("action", "sum") , ("xpath", "content/size") ]
			args_main = [("where", "namespace >='%s'" % candidate), ("as", "iterator"), ("action", "get-values"), ("xpath ename=\"id\"", "id"), ("xpath ename=\"namespace\"", "namespace"), ("xpath ename=\"filename\"", "name") ]
		else:
# FIXME - this will fail on picking up namespaces (ie it only returns assets found)
			args_init = [("where", "namespace='%s' and name='%s'" % (namespace, basename)), ("count", "true"), ("action", "sum") , ("xpath", "content/size") ]
			args_main = [("where", "namespace='%s' and name='%s'" % (namespace, basename)), ("as", "iterator"), ("action", "get-values"), ("xpath ename=\"id\"", "id"), ("xpath ename=\"filename\"", "name"), ("xpath ename=\"size\"", "content/size") ]

		result = self.mf_client.run("asset.query", args_init)

#		self.mf_client.xml_print(result)

		elem = self.mf_client.xml_find(result, "value")

# sometimes None gets returned instead of 0 ... gg
		try:
			total_bytes = int(elem.text)
			total_assets = int(elem.attrib['nbe'])
		except:
			print "No matching files"
			return

		self.mf_client.log("DEBUG", "Total assets to get: %d" % total_assets)
		self.mf_client.log("DEBUG", "Total bytes to get: %d" % total_bytes)

		result = self.mf_client.run("asset.query", args_main)

#		self.mf_client.xml_print(result)

		elem = self.mf_client.xml_find(result, "iterator")
		iterator = elem.text

# TODO - adaptable size choice based on asset_count
		iterate_size = 100

# CURRENT - iterator based download
		total_bytes_sent = 0

# FIXME - turn noise on/off
		self.mf_client.debug = False

		while True:
			try:
# clean
				self.mf_client.log("DEBUG", "Iterator chunk start")
				list_asset_filepath = []

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
							relpath = posixpath.relpath(namespace, self.cwd)
							path = os.path.join(os.getcwd(), relpath)
# add valid download entry
					if asset_id is not None and filename is not None:
						if path is None:
							filepath = os.path.join(os.getcwd(), filename)
						else:
							filepath = os.path.join(path, filename)
							list_local_path[path] = 1
						list_asset_filepath.extend([(asset_id, filepath)])

# create any required local dirs (NB: may get exception if they exist)
# FIXME - permission denied exception left to actual download ... better way to handle?
				for local_path in list_local_path:
					try:
						self.mf_client.log("DEBUG", "Creating local directory: %s" % local_path)
						os.makedirs(local_path)
					except Exception as e:
						self.mf_client.log("DEBUG", "%s" % str(e))
						pass

# TODO - upload iterate sub-set of files
#				for asset_id, filepath in list_asset_filepath:
#					print "get [id=%r] => %r" % (asset_id, filepath)

				if self.mf_fast:
					self.mf_fast.session = self.mf_client.session
					manager = self.mf_fast.get_managed(list_asset_filepath, total_bytes=total_bytes)
				else:
					manager = self.mf_client.get_managed(list_asset_filepath, total_bytes=total_bytes)

				try:
					while True:
						progress = 100.0 * manager.bytes_recv() / float(total_bytes)
						sys.stdout.write("Progress: %3.0f%% at %.1f MB/s    \r" % (progress, manager.byte_recv_rate()))
						sys.stdout.flush()
						if manager.is_done():
							break
						time.sleep(1)

				except KeyboardInterrupt:
					manager.cleanup()
					break

			except:
				print "\n"
				self.mf_client.log("DEBUG", "Last iterator completed")
				break

		return

# --
	def help_put(self):
		print "Upload local files or directories to the current folder on the remote server\n"
		print "Usage: put <file or folder>\n"
		print "Examples: put /home/sean/*.jpg"
		print "          put /home/sean/mydirectory/\n"


	def do_put(self, line):
# TODO - args for overwrite/crc checks?
# build upload list pairs
		upload_list = []
		total_bytes = 0
		if os.path.isdir(line):
			print "Walking directory tree..."
# FIXME - handle input of '/'
			line = os.path.abspath(line)
			parent = os.path.normpath(os.path.join(line, ".."))

			for root, directory_list, name_list in os.walk(line):
				remote = self.cwd + "/" + os.path.relpath(path=root, start=parent)
				upload_list.extend( [(remote , os.path.normpath(os.path.join(os.getcwd(), root, name))) for name in name_list] )
		else:
			print "Building file list..."
			upload_list = [(self.cwd, os.path.join(os.getcwd(), filename)) for filename in glob.glob(line)]

# DEBUG
#		for dest,src in upload_list:
#			print "put: %s -> %s" % (src, dest)
#		return
#		manager = self.mf_client.put_managed(upload_list)

		if self.mf_fast:
			print "Switching to HTTP"
			self.mf_fast.session = self.mf_client.session
			manager = self.mf_fast.put_managed(upload_list)
		else:
			manager = self.mf_client.put_managed(upload_list)


		try:
			while True:
				progress = 100.0 * manager.bytes_sent() / float(manager.bytes_total)
				sys.stdout.write("Progress: %3.0f%% at %.1f MB/s    \r" % (progress, manager.byte_sent_rate()))
				sys.stdout.flush()
				if manager.is_done():
					break
				time.sleep(1)
		except KeyboardInterrupt:
			manager.cleanup()

		print "\n"

# CURRENT
		print "Bytes sent: %f" % manager.bytes_sent()

# TODO - transfer summary of some kind? (dump log file if too many failed transfers?)
#		for pair in manager.summary:
#			print "uploaded asset ID = {0}".format(pair[0])

# --
	def help_cd(self):
		print "Change the current remote working directory.\n"
		print "Usage: cd <directory>\n"

	def do_cd(self, line):
		if os.path.isabs(line):
			candidate = line
		else:
			candidate = posixpath.normpath(self.cwd + "/" + line)
# set if exists on remote server
		if self.mf_client.namespace_exists(candidate):
			self.cwd = candidate
			print "Remote: %s" % self.cwd
		else:
			print "Invalid remote directory: %s" % candidate

# --
	def help_pwd(self):
		print "Display the current remote working directory\n"
		print "Usage: pwd\n"

	def do_pwd(self, line):
		print "Remote: %s" % self.cwd

# --
	def help_mkdir(self):
		print "Create a remote directory\n"
		print "Usage: mkdir <directory>\n"

	def do_mkdir(self, line):
		if posixpath.isabs(line):
			ns_target = line
		else:
			ns_target = posixpath.normpath(self.cwd + "/" + line)

		self.mf_client.run("asset.namespace.create", [("namespace", ns_target)])

# --
	def help_rm(self):
		print "Delete remote file(s)\n"
		print "Usage: rm <file/pattern>\n"

	def do_rm(self, line):
# TODO - cope with absolute path
		try:
			result = self.mf_client.run("asset.query", [("where", "namespace='{0}' and name='{1}'".format(self.safe_cwd(), line)), (":action", "count")])
		except:
			print "Server responded with an error"
			return

# not sure why this find doesn't work
#		elem = result.find("value")
		for elem in result.iter():
			if elem.tag == "value":
				count = int(elem.text)
				if count == 0:
					print "No match"
					return

				if self.ask("Remove %d files: (y/n) " % count):
					self.mf_client.run("asset.query", [("where", "namespace='{0}' and name='{1}'".format(self.safe_cwd(), line)), (":action", "pipe"), (":service name=\"asset.destroy\"", "")])
				else:
					print "Aborted"
				return

# --
	def help_rmdir(self):
		print "Remove a remote directory\n"
		print "Usage: rmdir <directory>\n"

	def do_rmdir(self, line):
		if posixpath.isabs(line):
			ns_target = line
		else:
			ns_target = posixpath.normpath(self.cwd + "/" + line)

		if self.mf_client.namespace_exists(ns_target):
			if self.ask("Remove directory: %s (y/n) " % ns_target):
				self.mf_client.run("asset.namespace.destroy", [("namespace", ns_target)])
			else:
				print "Aborted"
		else:
			print "No such directory: %s" % ns_target

# -- local commands
	def help_debug(self):
		print "Turn debugging output on/off\n"
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
		print "Display local working directory\n"
		print "Usage: lpwd\n"

	def do_lpwd(self, line):
		print "Local: %s" % os.getcwd()

# --
	def help_lcd(self):
		print "Change local working directory\n"
		print "Usage: lcd <directory>\n"

	def do_lcd(self, line):
		os.chdir(line)
		print "Local: %s" % os.getcwd()

# --
	def help_lls(self):
		print "List contents of local working directory\n"
		print "Usage: lls <directory>\n"

	def do_lls(self, line):

# TODO - process flags, but for now strip out so (eg) "ls -al" doesn't trigger an exception
# TODO - sort out other cases eg "ls *.pdf"
		line = re.sub(r'-\S+', '', line)

		if not len(line):
			cwd = os.getcwd()
		else:
			cwd = line

		print "Local: %s" % cwd
		for filename in os.listdir(cwd):
			if os.path.isdir(filename):
				print "[directory] " + filename
		for filename in os.listdir(cwd):
			if os.path.isfile(filename):
#				print "bytes=%-15d | %-s" % (os.path.getsize(filename), filename)
				print "%s | %-s" % (self.human_size(os.path.getsize(filename)), filename)

# --- 
	def help_whoami(self):
		print "Report the current authenticated user or delegate and associated roles\n"
		print "Usage: whoami\n"

	def do_whoami(self, line):
		result = self.mf_client.run("actor.self.describe")
		try:
			for elem in result.iter('actor'):
				name = elem.attrib['name']

				if ":" in name:
					print "actor = %s" % name
				else:
					print "actor = delegate"
			for elem in result.iter('role'):
				print "  role = %s" % elem.text
		except:
			print "I'm not sure who you are"


# -- connection commands
	def help_logout(self):
		print "Terminate the current session to the server\n"
		print "Usage: logout\n"

	def do_logout(self, line):
		self.mf_client.logout()

# --- 
	def help_login(self):
		print "Initiate login to the current remote server\n"
		print "Usage: login\n"

	def do_login(self, line):

		user = raw_input("Username: ")

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
			print "Writing session to config file: %s" % self.config_filepath

			self.config.set(self.config_name, 'session', self.mf_client.session)

			f = open(self.config_filepath, "w")
			self.config.write(f)
			f.close()

		except Exception as e:
			print "Not logged in: %s" % str(e)


# --
	def help_delegate(self):
		print "Create a delegated credential, stored in your local home directory, that will be automatically reused to authenticate to the remote server.\n"
		print "An optional argument can be supplied to set the credential lifetime, or set to off to destroy all delegated credentials for your account.\n"
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
#					self.mf_client.run("secure.identity.token.destroy.all")
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

# --
	def help_quit(self):
		print "Exit without terminating the session\n"
	def do_quit(self, line):
		exit(0)

# --
	def help_exit(self):
		print "Exit without terminating the session\n"
	def do_exit(self, line):
		exit(0)

# --
	def loop_interactively(self):
		while True:
			try:
				self.cmdloop()
			except KeyboardInterrupt:
				print "Interrupted, cleaning up     "
				continue
			except Exception as e:
				print str(e)

def main():

# server config (section heading) to use
	p = argparse.ArgumentParser(description='pshell help')
	p.add_argument('-c', dest='config', default='pawsey', help='the configuration name describing the remote server to connect to (eg pawsey)')
	p.add_argument('-i', dest='script', help='input script file containing commands to run)')
	args = p.parse_args()
	current = args.config
	script = args.script

# use config if exists, else create a dummy one
	config = ConfigParser.ConfigParser()

# hydrographic NAS box gives a dud path for ~
# NEW - test readwrite and if fail -> use CWD
	config_filepath = os.path.expanduser("~/.mf_config")

#	if not os.access(config_filepath, os.W_OK):
	try:
		open(config_filepath, 'a').close()
	except:
		print "Bad home directory [%s] ... falling back to current directory" % os.path.expanduser("~")
		config_filepath = os.path.join(os.getcwd(), ".mf_config")

	config.read(config_filepath)

	encrypt = True
	debug = False
	session = ""
	token = None
	config_changed = False

	if config.has_section(current):
		print "Reading config [%s]" % config_filepath
		try:
			server = config.get(current, 'server')
			protocol = config.get(current, 'protocol')
			port = config.get(current, 'port')
		except:
			print "Config file has insufficiently specified server"
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

# mediaflux client
	try:
		mf_client = mfclient.mf_client(protocol=protocol, port=port, server=server, session=session, enforce_encrypted_login=encrypt, debug=debug)
	except:
		print "Failed to establish network connection to: %s" % current
		exit(-1)

# test http connection (data only) and don't send any session info
	http_available = False
	if protocol == "https":
		try:	
			mf_fast = mfclient.mf_client(protocol="http", port=80, server=server, session="", enforce_encrypted_login=encrypt, debug=debug)
# FIXME - do a public login to check instead?
			result = mf_fast.run("actor.self.describe")
# TODO - strictly, should look at result
			http_available = True
		except Exception as e:
			if "session is not valid" in str(e):
				http_available = True


# FIXME - need to deal with config parse inconsistency here
# ie session = None gets spat out and read back in as a pure text string 
# CURRENT - simplest soln might be to use empty string instead ie session="" instead of None (token as well ....)

# check session
	if not len(session) == 0:
		if not mf_client.authenticated():
			session = ""
			config.set(current, 'session', session)
			config_changed = True

# no valid session - can we get one via token
	if len(session) == 0:
		if token:
			try:
				mf_client.login(token=token)
				config.set(current, 'session', mf_client.session)
				config_changed = True
				if debug:
					print "Token ok"
			except:
				config.set(current, 'token', None)
				config_changed = True
				if debug:
					print "Invalid/expired token"

# update config to match current state
	if config_changed:
		if debug:
			print "Writing config..."

		f = open(config_filepath, "w")
		config.write(f)
		f.close()

# hand control of mediaflux client over to parsing loop
	my_parser = parser()
	my_parser.mf_client = mf_client
	my_parser.config_name = current
	my_parser.config_filepath = config_filepath
	my_parser.config = config

# CURRENT - TAB completion
# this works for OS-X
# NOTE - no readline in Windows ...
	try:
		readline.parse_and_bind("bind ^I rl_complete")
		readline.parse_and_bind("tab: complete")
	except:
		mf_client.log("WARNING", "No readline module available")

# CURRENT
	if http_available:
		my_parser.mf_fast = mf_fast
		if debug:
			print "HTTP: available"

# process script or go interactive
	if script:
		with open(script) as f:
			for line in f:
				try:
					print "input> %s" % line
					my_parser.onecmd(line)
				except Exception as e:
					print str(e)
					exit(-1)
	else:
		print("Welcome to pshell, type 'help' for a list of commands")
		my_parser.loop_interactively()


if __name__ == '__main__':
	main()
