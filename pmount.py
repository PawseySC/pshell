#!/usr/bin/env python

import os
import sys
import math
import stat
import time
import errno
import random
import shutil
import string
import getpass
import logging
import httplib
import urllib2
import argparse
import tempfile
import posixpath
import ConfigParser
from datetime import datetime
import mfclient

try:
    from fuse import FUSE, FuseOSError, Operations
except:
    print "Error: this system does not seem to have FUSE installed."

# ===
class mfread():
    """
    Handle downloads from mediaflux
    """
    def __init__(self, response):
        self.response = response
        self.offset = 0
        self.total = 0
        self.start = time.time()

# ===
class mfwrite():
    """
    Handle buffered uploads to Mediaflux
    """
    def __init__(self, store=None, quota=None, tmpfile=None, fullpath=None):
        self.buffer = bytearray()
        self.length = 0
        self.offset = 0
        self.total = 0
        self.tmpfile = tmpfile
        self.fullpath = fullpath
        self.store = store
        self.quota = quota
        self.start = time.time()

    def inject(self, buff, offset):
        size = len(buff)
        if offset == self.total:
# sequential buffer extend
            self.buffer.extend(buff)
            self.length += size
            self.total += size
        else:
# inject at non-sequential offset (NB: Linux uses this to skip 0's)
            print "inject() non-seq: buffer => offset=%d,length=%d,total=%d : input => offset=%d,size=%d" % (self.offset,self.length,self.total,offset,size)
            if offset == self.offset:
# case 1 - restart at same offset as the current buffer -> truncate buffer to the current input 
                self.buffer[0:] = buff
                self.length = size
                self.total = self.offset + size
                print "inject() case1: buffer => offset=%d,length=%d,total=%d" % (self.offset,self.length,self.total)
            else:
# case 2 a) offset is ahead (assume we can just fill in with 0's)
                if offset > self.total:
# pad with 0's to get to offset 
                    pad_size = offset - self.total
                    for i in range(0, pad_size):
                        self.buffer.append(0)
# add the injected data
                    self.buffer.extend(buff)
                    self.length = len(self.buffer)
                    self.total += pad_size + size
                    print "inject() case2a: buffer length=%d, total=%d" % (self.length, self.total)
                else:
                    raise FuseOSError(errno.EILSEQ)

# fail if we exceed the destination store quota
        if self.quota is not None:
            if self.total > self.quota:
                raise FuseOSError(errno.EDQUOT)

        return size

    def truncate(self):
        self.buffer = self.buffer[:0]
        self.offset = self.total
        self.length = 0

# ===
class pmount(Operations):
    """
    FUSE implementation for mounting a Mediaflux namespace as a local folder
    """

# --- performance decorators 
    class iostats():
        t_count = dict()
        t_bytes = dict()
        t_time = dict()

        @classmethod
        def human(mystats, value):
            units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
            if value:
                rank = int((math.log10(value)) / 3)
                rank = min(rank, len(units) - 1)
                human = value / (1000.0 ** rank)
                f = ("%.2f" % human).rstrip('0').rstrip('.')
            else:
                f = "0"
                rank = 0
            return "%s %-s" % (f, units[rank])

        @classmethod
        def display(mystats, logger):
            logger.info(" === iostats times ===")
            for fname in mystats.t_count.keys():
                size = mystats.t_bytes[fname]
                time = mystats.t_time[fname]
                if size == 0:
                    logger.info("%-20s : %d calls : total time = %f s" % (fname, mystats.t_count[fname], time))

            logger.info(" === iostats rates ===")
            for fname in mystats.t_count.keys():
                time = max(0.1, mystats.t_time[fname])
                size = mystats.t_bytes[fname]
                if size != 0:
                    rate = float(size) / time
                    h_rate = mystats.human(rate)
                    logger.info("%-20s : %d bytes @ %s/s" % (fname, mystats.t_bytes[fname], h_rate))

        @classmethod
        def insert(mystats, fname, size, elapsed):
            if fname in mystats.t_count.keys():
                mystats.t_count[fname] += 1
                mystats.t_bytes[fname] += size
                mystats.t_time[fname] += elapsed
            else:
                mystats.t_count[fname] = 1
                mystats.t_bytes[fname] = size
                mystats.t_time[fname] = elapsed

        @classmethod
        def record(mystats, func):
            def wrapper(*args):
# setup for stats recording
                if func.__name__ == 'read':
                    fname = "network_r"
                    size = int(args[2])
                elif func.__name__ == 'mf_write':
                    fname = "network_w"
                    size = args[3]
                else:
                    fname = func.__name__
                    size = 0
# time the wrapped function
                start = time.time()
                res = func(*args)
                elapsed = time.time() - start

# fill out dictionaries
                mystats.insert(fname, size, elapsed)

                return res
            return wrapper


# --- MAIN setup
    def __init__(self, args):

# debugging
        self.log = logging.getLogger('pmount')
        if args.verbose:
            self.log.setLevel(logging.DEBUG)
        else:
            self.log.setLevel(logging.INFO)
        if args.logfile:
            logfile = datetime.now().strftime('pmount-%Y-%m-%d-%H:%M:%S.log')
            print "Writing log to: %s" % logfile
            logging.basicConfig(filename=logfile, format='%(asctime)s - %(levelname)s - %(message)s')
        else:
            logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')

# NB: always use config file as we want a secure location to store the authentication token
        token = "xyz123"
        try:
            config_filepath = os.path.expanduser("~/.mf_config")
            self.log.debug("init() : config=%s" % config_filepath)
            config = ConfigParser.ConfigParser()
            config.read(config_filepath)

            if config.has_section(args.config) is False:
                config.add_section(args.config)

            if config.has_option(args.config, 'server'):
                args.server = config.get(args.config, 'server')

            if config.has_option(args.config, 'protocol'):
                args.protocol = config.get(args.config, 'protocol')

            if config.has_option(args.config, 'port'):
                args.port = config.get(args.config, 'port')

            if config.has_option(args.config, 'domain'):
                args.domain = config.get(args.config, 'domain')

            if config.has_option(args.config, 'token'):
                token = config.get(args.config, 'token')

            if config.has_option(args.config, 'namespace'):
                args.namespace = config.get(args.config, 'namespace')

        except Exception as e:
            self.log.error("init(): %s" % str(e))

# remote filesystem 
        self.remote_root = args.namespace
        self.readonly = args.readonly
# dictionary cache of assets and namespaces
# NB: have to separate as they are quite distinct objects in mediaflux
# key = parent namespace, value = another dictionary of children in the namespace
# second dictionary key is the child namespace or asset name, value = inode attributes
        self.namespace_cache = dict()
        self.asset_cache = dict()
# temporary local cache for new files
        self.inode_cache = dict()
# mediaflux readonly and writeonly communication objects (keys are the fake filehandles to pass around)
        self.mf_ronly = dict()
        self.mf_wonly = dict()
# ownership cheat
#        self.uid = os.getuid()
#        self.gid = os.getgid()
# NEW - match ownership to mount path
        stat_info = os.stat(args.path)
        self.uid = stat_info.st_uid
        self.gid = stat_info.st_gid

# mtime cheat
        delta = datetime.utcnow() - datetime(1970, 1, 1)
        self.st_time = delta.total_seconds()

# communication setup
        self.timeout = 1800
# around 100 MB is the best "sweet spot" so far ... but performance can be quite variable 
        self.buffer_max = 100000000

# attempt to connect and authenticate
        self.log.info("init(): protocol=%s, server=%s, port=%s, domain=%s, namespace=%s" % (args.protocol, args.server, args.port, args.domain, args.namespace))
        try:
            self.mf_client = mfclient.mf_client(protocol=args.protocol, server=args.server, port=args.port, domain=args.domain, debug=0)
        except Exception as e:
            self.log.error("init(): failed to connect: %s" % str(e))
            exit(-1)
        try:
            self.mf_client.login(token=token)
        except Exception as e:
            self.log.debug("init(): %s" % str(e))
            response = raw_input("No valid token found. Do you want to create one? ")
            if response.startswith('y') or response.startswith('Y'):
                print "Login to server [%s] and domain [%s] required." % (args.server, args.domain)
                user = raw_input("Username: ")
                password = getpass.getpass("Password: ")
                self.mf_client.login(user, password)
# create token
                actor = "%s:%s" % (args.domain, user)
                result = self.mf_client.aterm_run('secure.identity.token.create :role -type user "%s" :role -type domain "%s" :min-token-length 16' % (actor, args.domain))
                for elem in result.iter():
                    if elem.tag == 'token':
                        token = elem.text
                        self.mf_client.token = token
                        self.log.info("init(): Token created successfully")
# save configuration
                config.set(args.config, 'server',  args.server)
                config.set(args.config, 'protocol', args.protocol)
                config.set(args.config, 'port', args.port)
                config.set(args.config, 'domain', args.domain)
                config.set(args.config, 'namespace', args.namespace)
                config.set(args.config, 'session', self.mf_client.session)
                config.set(args.config, 'token', token)
                f = open(config_filepath, "w")
                config.write(f)
                f.close()
            else:
                print "Authentication failed"
                exit(-1)
# success
        self.verbose = args.verbose
        self.log.info("init(): connection established")

# ---
    def _debug_namespace_cache(self):
        for namespace in self.namespace_cache:
            print("[%s]" % namespace)
            for child in self.namespace_cache[namespace]:
                print("  - %s : %r" % (child, self.namespace_cache[namespace][child]))

# --- triggered on ctrl-C or unmount
    def destroy(self, path):
        self.iostats.display(self.log)
        return 0

# --- fake filehandle for download (read only)
#    def mf_ronly_open(self, namespace, filename):
    def mf_ronly_open(self, fullpath):

        namespace = posixpath.dirname(fullpath)
        filename = posixpath.basename(fullpath)

 # get the asset ID (NB: asset.query with :get-content-status True doesn't work)
        reply = self.mf_client.aterm_run('asset.query :where "namespace=\'%s\' and name=\'%s\'"' % (namespace, filename))
        elem = reply.find(".//id")
        if elem is not None:
            asset_id = int(elem.text)
        else:
            self.log.debug("mf_ronly_open(): couldn't find asset [%s] in namespace [%s]" % (filename, namespace))
            raise FuseOSError(errno.ENOENT)
# get the content status
        reply = self.mf_client.aterm_run('asset.content.status :id %d' % asset_id)
        elem = reply.find(".//asset/state")
        if elem is not None:
# recall if offline and raise a "Try Again" ref: https://github.com/sahlberg/libnfs/issues/164
            if "offline" in elem.text:
                self.log.info("mf_ronly_open(): asset [%s] in namespace [%s] is OFFLINE -> migrating [%d] ONLINE" % (filename, namespace, asset_id))
                self.mf_client.aterm_run("asset.content.migrate :id %d :destination 'online'" % asset_id)
                raise FuseOSError(errno.EAGAIN)
        else:
            self.log.warning("mf_ronly_open(): failed to retrieve content status for asset [%s] in namespace [%s]" % (filename, namespace))

# construct URL to the file and open 
        url = self.mf_client.data_get + "?_skey=%s&id=%d" % (self.mf_client.session, asset_id)
        response = urllib2.urlopen(url, timeout=self.timeout)
# FIXME - end of range = max open files ...
        for fh in range(1,100):
            if self.mf_ronly.get(fh) is None:
                self.mf_ronly[fh] = mfread(response)
                return fh
# couldn't get a free filehandle - give up
        raise FuseOSError(errno.EMFILE)

# --- fake filehandle for upload (write only)
#    def mf_wonly_open(self, folder, filename):
    def mf_wonly_open(self, fullpath):
# NEW
        folder = posixpath.dirname(fullpath)
        filename = posixpath.basename(fullpath)

# get info on destination
        try:
            reply = self.mf_client.aterm_run("asset.namespace.describe :namespace %s" % folder)
            elem = reply.find(".//store")
            store = elem.text
# NB: doesn't fit the namespace quota scheme
            reply = self.mf_client.aterm_run("asset.store.describe :name %s" % store)
            elem = reply.find(".//mount/free")
            quota = int(elem.text)
        except Exception as e:
            self.log.error("mf_wonly_open() : %s" % str(e))
            raise FuseOSError(errno.ENODATA)
# create server upload job
        try:
            reply = self.mf_client.aterm_run("server.io.job.create :store 'asset:%s'" % store)
            elem = reply.find(".//ticket")
            ticket = int(elem.text)
            reply = self.mf_client.aterm_run("server.io.job.describe :ticket %d" % ticket)
            elem = reply.find(".//path")
            tmpfile = elem.text
#            self.mf_wonly[ticket] = mfwrite(store=store, quota=quota, tmpfile=tmpfile)
            self.mf_wonly[ticket] = mfwrite(store=store, quota=quota, tmpfile=tmpfile, fullpath=fullpath)
        except Exception as e:
            self.log.error("mf_wonly_open() : %s" % str(e))
            raise FuseOSError(errno.EPERM)

        self.log.debug("mf_wonly_open() : backing store=%s, quota=%d, tmpfile=%s, ticket=%s" % (store, quota, tmpfile, ticket))
        return ticket

# --- convert virtual path to the server path
    def _remote_fullpath(self, partial):
        if self.remote_root == '/':
            return partial
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.remote_root, partial)
        if path.endswith('/'):
            path = path[:-1]
        return path

# --- TODO - window's metadata files too?
    def _should_ignore(self, filename):
        if filename.startswith(".local"):
            return True
        if filename.startswith("._"):
            return True
        return False

# --- create a new attribute dictionary
    def inode_new(self, mode, links, size=0, mtime=-1):
        if self.readonly is False:
            mode = mode | 0200
        if mtime == -1:
            mtime = self.st_time
        attr = { 'st_uid':self.uid, 'st_gid':self.gid, 'st_size':size, 'st_mode':mode, 'st_nlink':links, 'st_mtime':mtime }
        return attr

# --- namespace only population of inode and directory listing caches
# NB: assumes this inode and content listing are not cached & creates new entries
    @iostats.record
    def get_namespaces(self, fullpath):
# add inodes for all child namespaces and populate directory listing 
        reply = self.mf_client.aterm_run('asset.namespace.list :namespace %s' % fullpath)
        this_folder = dict()
        for elem in reply.findall(".//namespace/namespace"):
            folder = posixpath.join(fullpath, elem.text)
            this_folder[elem.text] = self.inode_new(stat.S_IFDIR | 0500, 2)
# cache the directory listing (namespaces only)
        self.namespace_cache[fullpath] = this_folder

# --- get asset query as an iterator object 
    @iostats.record
    def get_asset_iter(self, namespace):
        reply = self.mf_client.aterm_run('asset.query :where "namespace=\'%s\'" :action get-meta :as iterator' % namespace)
        elem = reply.find(".//iterator")
        return elem.text

# --- get asset metadata for single item 
    @iostats.record
    def get_asset(self, namespace, filename):
        try:
            reply = self.mf_client.aterm_run('asset.query :where "namespace=\'%s\' and name=\'%s\'" :action get-meta' % (namespace, filename))
            # no match? => doesn't exist
            xml_path = reply.find(".//path")
            if xml_path is None:
                return None
            # NB: some assets have no content => missing size element
            xml_size = reply.find(".//size")
            if xml_size is not None:
                size = int(xml_size.text)
            else:
                size = 0
            mtime = self.st_time
            xml_mtime = reply.find(".//mtime")
            if xml_mtime is not None:
                for k,v in xml_mtime.attrib.iteritems():
                    if k == 'millisec':
                        mtime = int(v) / 1000
            # it's an asset -> return info
            return { 'st_uid':self.uid, 'st_gid':self.gid, 'st_size':size, 'st_mode':stat.S_IFREG | 0400, 'st_nlink':1, 'st_mtime':mtime }

        except Exception as e:
            self.log.error("get_asset() : %s" % str(e))

        return None

# --- testing mechanism
    def fail_session(self):
        print "Injecting session failure"
        print "old session = %s" % self.mf_client.session
        self.mf_client.session = None

# --- grant visibility of virtual fs
    def access(self, path, mode):
        return 0

# --- get file/folder attributes 
    @iostats.record
    def getattr(self, path, fh=None):
#        self.log.debug("getattr() : path=%s" % path)
# check the temporary cache
        inode = self.inode_cache.get(path)
        if inode is not None:
            return inode

        fullpath = self._remote_fullpath(path)
        name = posixpath.basename(fullpath)
        parent = posixpath.dirname(fullpath)

# ignore silly metadata files which shouldn't exist (or be silently created by the OS)
        if self._should_ignore(name):
            raise FuseOSError(errno.ENOENT)

# generate parent namespace's namespace cache if it doesn't exist
        if parent not in self.namespace_cache:
            self.get_namespaces(parent)
# return the inode (value) if the name (key) exists in the parent's namespace cache
        if name in self.namespace_cache[parent]:
            return self.namespace_cache[parent][name]

# NB: asset caching is handled differently to the namespace caching above - see readir() implementation for why
# if the parent namespace has an asset cache and the filename appears - return the inode
        if parent in self.asset_cache:
            if name in self.asset_cache[parent]:
                return self.asset_cache[parent][name]

# triggered on non-navigational file query (eg file /some/random/path/file)
        inode = self.get_asset(parent, name)
        if inode is not None:
            self.log.debug("getattr() : returning standalone inode for [%s/%s]" % (parent, name))
            return inode

# I got nothing
        raise FuseOSError(errno.ENOENT)

# --- display cached entries, otherwise lookup (NB: assets are only cached if we've FULLY iterated them all)
    @iostats.record
    def readdir(self, path, fh):
        namespace = self._remote_fullpath(path)

# namespace entries
        if namespace not in self.namespace_cache:
            self.log.debug("readdir() : namespace listing server call needed [%s]" % namespace)
            self.get_namespaces(namespace)
        for item in self.namespace_cache[namespace].keys():
            yield item
# asset entries
        if namespace in self.asset_cache:
            for item in self.asset_cache[namespace].keys():
                yield item
        else:
            self.log.debug("readdir() : asset listing server call needed [%s]" % namespace)
# NB: we do the asset query inline (with yield) for performance reasons on very large directories
            iterator = self.get_asset_iter(namespace)
            done = False
            this_folder = dict()
            while not done:
                result = self.mf_client.aterm_run("asset.query.iterate :id %s :size %d" % (iterator, 100))
                for elem in result.iter("asset"):
                    xml_path = elem.find(".//path")
                    if xml_path is not None:
                        xml_size = elem.find(".//size")
                        if xml_size is not None:
                            size = int(xml_size.text)
                        else:
                            size = 0
                        fullpath = xml_path.text
                        filename = posixpath.basename(fullpath)
                        mtime = self.st_time
                        xml_mtime = elem.find(".//mtime")
                        if xml_mtime is not None:
                            for k,v in xml_mtime.attrib.iteritems():
                                if k == 'millisec':
                                    mtime = int(v) / 1000
# add the inode
                        this_folder[filename] = self.inode_new(stat.S_IFREG | 0400, 1, size=size, mtime=mtime)
                        yield filename
                    else:
                        self.log.debug("readdir() : bad asset metadata for element [%r]" % elem)
# check for completion
                for elem in result.iter("iterated"):
                    state = elem.get('complete')
                    if "true" in state:
                        done = True
# completed ... now we can cache the full asset listing
            self.asset_cache[namespace] = this_folder

# ---
    def statfs(self, path):
# FUSE seems to ignore the recommened block size values, as far as I can tell ...
        keys = ['f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax']
        fake = dict()
        for key in keys:
            fake[key] = 0
        return fake

# --- 
    def utimens(self, path, times=None):
        self.log.debug("utimens() : path=%s " % path)
        return 0

# --- TODO - metadata via extended attributes?
#    def getxattr(self, path, name, *args):
#        self.log.debug("getxattr() : path=%s : name=%s" % (path, name))
#        return "value[%s]" % name
#    def listxattr(self, path):
#        self.log.debug("listxattr() : path=%s" % path)
#        return ["xpath1", "xpath2"]
#   def setxattr(self, path, name, value, size, options, *args):
#   def removexattr(self, path, name):

# --- 
    def mkdir(self, path, mode):
        if self.readonly:
            raise FuseOSError(errno.EACCES)
# build paths
        fullpath = self._remote_fullpath(path)
        parent = posixpath.dirname(fullpath)
        child = posixpath.basename(fullpath)
# run mkdir command and update cache on success
        try:
            self.mf_client.aterm_run('asset.namespace.create :namespace "%s"' % fullpath)
            namespace_cache = self.namespace_cache.get(parent)
            if namespace_cache is not None:
                namespace_cache[child] = self.inode_new(stat.S_IFDIR | 0500, 2)
        except Exception as e:
            self.log.debug("mkdir(): %s" % str(e))
            raise FuseOSError(errno.EACCES)

# ---
    def rmdir(self, path):
        if self.readonly:
            raise FuseOSError(errno.EACCES)
# check if there is any content under this folder
        fullpath = self._remote_fullpath(path)
        parent = posixpath.dirname(fullpath)
        child = posixpath.basename(fullpath)
        reply = self.mf_client.aterm_run('asset.query :where "namespace>=\'%s\'" :action count :size 1' % fullpath)
        elem = reply.find(".//value")
        count = int(elem.text)
# delete only if there is absolutely no content (helps me sleep better at nights)
        if count == 0:
            self.log.info("rmdir(): removing empty folder: %s" % fullpath)
            try:
                self.mf_client.aterm_run('asset.namespace.destroy :namespace "%s"' % fullpath)
                namespace_cache = self.namespace_cache.get(parent)
                if namespace_cache is not None:
                    del self.namespace_cache[parent][child]
                return
            except Exception as e:
                self.log.debug("rmdir(): %s" % str(e))
                raise FuseOSError(errno.EACCES)
# non empty folder error
        raise FuseOSError(errno.ENOTEMPTY)

# ---
# ref: http://pubs.opengroup.org/onlinepubs/9699919799/utilities/mv.html
# NB: mv file folder ... gets mapped to rename file folder/file
    def rename(self, old, new):
        self.log.debug("rename(): %s -> %s" % (old, new))
        if self.readonly:
            raise FuseOSError(errno.EACCES)

# get origin inode (if exists)
        try:
            inode = self.getattr(old)
        except:
            raise FuseOSError(errno.ENOENT)

# build source and destination paths
        old_fullpath = self._remote_fullpath(old)
        oldname = posixpath.basename(old_fullpath)
        oldspace = posixpath.dirname(old_fullpath)
        new_fullpath = self._remote_fullpath(new)
        newspace = posixpath.dirname(new_fullpath)
        newname = posixpath.basename(new_fullpath)

# mediaflux call block
        try:
            if inode['st_mode'] & stat.S_IFREG:
# move the file and update the cache (if any) 
                self.mf_client.aterm_run('asset.move :id "path=%s" :namespace "%s" :name "%s"' % (old_fullpath, newspace, newname))
                if oldspace in self.asset_cache:
                    del self.asset_cache[oldspace][oldname]
                if newspace in self.asset_cache:
                    self.asset_cache[newspace][newname] = inode
            elif inode['st_mode'] & stat.S_IFDIR:
                if oldspace == newspace:
# rename folder (parent stays the same) and update cache
                    self.mf_client.aterm_run('asset.namespace.rename :namespace "%s" :name "%s"' % (old_fullpath, newname))
                    if oldspace in self.namespace_cache:
                        self.namespace_cache[oldspace][newname] = self.namespace_cache[oldspace].pop(oldname)
                else:
# CURRENT - FUSE cache gets really screwed up and ends up bugging the shell os.cwd
                    raise Exception("Operation not implemented")
# this works (on the mediaflux side) but at a low level, FUSE is caching something that causes a problem when statefully navigating the filesystem
#                    self.mf_client.aterm_run('asset.namespace.move :namespace "%s" :to "%s"' % (old_fullpath, newspace))
#                    if oldspace in self.namespace_cache:
#                        del self.namespace_cache[oldspace][oldname]
#                    if newspace in self.namespace_cache:
#                        self.namespace_cache[newspace][newname] = inode
        except Exception as e:
            self.log.error("rename(): %s" % str(e))
            # could be either:
            # no permission
            # destination already exists (file -> file)
            raise FuseOSError(errno.EACCES)

# ---
    def unlink(self, path):
        if self.readonly:
            raise FuseOSError(errno.EACCES)
        fullpath = self._remote_fullpath(path)
        namespace = posixpath.dirname(fullpath)
        filename = posixpath.basename(fullpath)
# run delete command and remove from cache on success
        try:
            self.mf_client.aterm_run('asset.destroy :id "path=%s"' % fullpath)
            asset_cache = self.asset_cache.get(namespace)
            if asset_cache is not None:
                del asset_cache[filename]
        except Exception as e:
# FIXME - raise the correct error for other cases (eg doesn't exist, deleted from the server by 3rd party)
            self.log.debug("unlink(): %s" % str(e))
            raise FuseOSError(errno.EACCESS)

# --- not supported
    def mknod(self, path, mode, dev):
        self.log.debug("mknod() : path=%s" % path)
        raise FuseOSError(errno.EACCES)

    def readlink(self, path):
        self.log.debug("readlink() : path=%s" % path)
        raise FuseOSError(errno.EPERM)

    def symlink(self, name, target):
        self.log.debug("symlink() : name=%s, target=%s" % (name, target))
        raise FuseOSError(errno.EPERM)

    def link(self, target, name):
        self.log.debug("link() : target=%s, name=%s" % (target, name))
        raise FuseOSError(errno.EPERM)

# ---
# FIXME - flags can be 0 sometimes which doesn't match any of the 3 modes it must be in [ O_RDWR, O_WRONLY, or O_RDONLY] 
# CURRENT - just assume it's rdonly ... 
    def open(self, path, flags):
        self.log.debug("open() : path=%s, flags=%r" % (path, flags))
# init paths
        fullpath = self._remote_fullpath(path)
        namespace = posixpath.dirname(fullpath)
        filename = posixpath.basename(fullpath)

# it will be difficult to implement this mode
        if flags & os.O_RDWR:
            raise FuseOSError(errno.EPERM)

# this path gets hit on overwrite else it does a create()
        if flags & os.O_WRONLY:
            if self.readonly:
                raise FuseOSError(errno.EPERM)
# FIXME - size = 0? bit of a hack as the truncate() is technically what should be doing this ...
# ie can we just update the cache (if exists?)
            self.inode_cache[path] = self.inode_new(mode=stat.S_IFREG | 0400, links=1, size=0)
            # fake filehandle for writing
#            return self.mf_wonly_open(namespace, filename)
            return self.mf_wonly_open(fullpath)

# fake filehandle for reading
#        return self.mf_ronly_open(namespace, filename)
        return self.mf_ronly_open(fullpath)

# ---
    @iostats.record
    def read(self, path, size, offset, fh):
        mfobj = self.mf_ronly[fh]
# if offset matches our mediaflux stream object - continue the sequential read
        if offset == mfobj.offset and mfobj.response is not None:
            try:
                data = mfobj.response.read(size)
# TODO - if len(data) != size ... we have a problem, raise?
                mfobj.offset += size
                mfobj.total += size
                return data

            except Exception as e:
                self.log.error("read() sequential: %s" % str(e))
                pass
        else:
# ensure we stick to random access mode
            mfobj.reponse = None
# random access method (unzip -v will hit this code path)
            fullpath = self._remote_fullpath(path)
            reply = self.mf_client.aterm_run('asset.content.get :id "path=%s" :length %d :offset %d :out dummy' % (fullpath, size, offset))
            try:
                elem = reply.find(".//outputs/url")
                url = elem.text
                response = urllib2.urlopen(url, timeout=self.timeout)
                mfobj.total += size
                return response.read(size)

            except Exception as e:
                self.log.error("read() random: %s" % str(e))
                pass

        raise FuseOSError(errno.EREMOTEIO)

# ---
    def create(self, path, mode, fi=None):
        self.log.debug("create() : path=%s, mode=%d" % (path, mode))
        if self.readonly:
            raise FuseOSError(errno.EACCES)

        fullpath = self._remote_fullpath(path)
        namespace = posixpath.dirname(fullpath)
        filename = posixpath.basename(fullpath)

# don't allow the OS to silently pollute
        if self._should_ignore(filename):
            raise FuseOSError(errno.EPERM)

# get ref to use as filehandle for write()
#        fakehandle = self.mf_wonly_open(namespace, filename)
        fakehandle = self.mf_wonly_open(fullpath)

# create temporary inode - required as the kernel calls getattr() to enforce there is an inode after create()
        self.inode_cache[path] = self.inode_new(mode=mode, links=1, size=0)

        return fakehandle

# --- write the current buffer to the io job ticket
    @iostats.record
    def mf_write(self, tmpfile, data, length, offset, ticket):
        if length == 0:
            self.log.debug("mf_write(): ticket=%d, 0 bytes -> %s" % (ticket, tmpfile))
            return

# custom multipart post to mediaflux
        xml_string = '<request><service name="service.execute" session="%s"><args><service name="server.io.write">' % self.mf_client.session
        xml_string += '<ticket>%d</ticket><offset>%d</offset></service></args></service></request>' % (ticket, offset)
        boundary = ''.join(random.choice(string.digits + string.ascii_letters) for i in range(30))
        mimetype = 'application/octet-stream'
        lines = []
        lines.extend(('--%s' % boundary, 'Content-Disposition: form-data; name="request"', '', str(xml_string),))
        lines.extend(('--%s' % boundary, 'Content-Disposition: form-data; name="nb-data-attachments"', '', "1",))
# NB: the tmp file name is required (otherwise the bytes go into a black hole)
        lines.extend(('--%s' % boundary, 'Content-Disposition: form-data; name="filename"; filename="%s"' % tmpfile, 'Content-Type: %s' % mimetype, '', ''))

        body = '\r\n'.join(lines)
        total_size = len(body) + length + len(boundary) + 8

        if self.mf_client.encrypted_data is True:
            conn = httplib.HTTPSConnection(self.mf_client.data_put, timeout=self.timeout)
        else:
            conn = httplib.HTTPConnection(self.mf_client.data_put, timeout=self.timeout)

        conn.putrequest('POST', "/__mflux_svc__")
        conn.putheader('Connection', 'keep-alive')
        conn.putheader('Cache-Control', 'no-cache')
        conn.putheader('Content-Length', str(total_size))
        conn.putheader('Content-Type', 'multipart/form-data; boundary=%s' % boundary)
        conn.putheader('Content-Transfer-Encoding', 'binary')
        conn.endheaders()

# main send
        conn.send(body)
        conn.send(data)

# terminating line (len(boundary) + 8)
        tail = "\r\n--%s--\r\n" % boundary
        conn.send(tail)
# get ACK from server (asset ID) else error (raise exception)
        resp = conn.getresponse()
        reply = resp.read()
        conn.close()

# --- buffer the data and send when limit has been exceeded
    def write(self, path, buf, offset, fh):

        self.log.debug("write() : path=%s, offset=%d, size=%d" % (path, offset, len(buf)))

        mfbuffer = self.mf_wonly[fh]
        size = mfbuffer.inject(buf, offset)
        if mfbuffer.length > self.buffer_max:
            self.mf_write(mfbuffer.tmpfile, mfbuffer.buffer, mfbuffer.length, mfbuffer.offset, fh)
            mfbuffer.truncate()
# NB: update inode immediately to stop the system retrying write() chunks
        inode = self.inode_cache[path]
        inode['st_size'] = mfbuffer.total
        return size

# Not implemented - but pretend everything's fine
# ---
    def chmod(self, path, mode):
        return 0
# ---
    def chown(self, path, uid, gid):
        return 0
# ---
    def lock(self, path, fip, cmd, lock):
        return 0
# ---
    def flush(self, path, fh):
        return 0
# ---
    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)

# --- 
# NB: never get the fh on Linux - see kernel truncate() signature
    def truncate(self, path, length, fh=None):

        self.log.debug("truncate() : path=%s, length=%d" % (path, length))

        fullpath = self._remote_fullpath(path)

# CURRENT - this (sort of works) ... but checksums don't match ... could be due to new out of order buffer writes tho ...
# hot truncate
# look for a current io job that matches 
        for ticket in self.mf_wonly:
            if self.mf_wonly[ticket].fullpath == fullpath:
                self.log.debug("truncate() : applying against active ticket=%r" % ticket)
                mfbuffer = self.mf_wonly[ticket]
                if mfbuffer.total < length:
                    size = length - mfbuffer.total
# is there a better way?
                    buff = bytearray()
                    for i in range(0,size):
                        buff.append(0)
# CURRENT - almost there ... except checksums now don't match
                    self.write(path, buff, mfbuffer.total, ticket)
                    return 0
                else:
                    print "why are you truncating bytes you've already sent?"

# TODO - cold truncate

# TODO - do this for cases that aren't part of a current write
#        try:
#            self.mf_client.aterm_run('asset.content.write.begin :id "path=%s"' % fullpath)
#
#            self.mf_client.aterm_run('asset.content.write.truncate :id "path=%s" :length %d' % (fullpath, length))
#
#            self.mf_client.aterm_run('asset.content.write.end :id "path=%s"' % fullpath)
#
#        except Exception as e:
#            self.log.warning("truncate() : %s" % str(e))


#        try:
#            if length == 0:
#                self.mf_client.aterm_run('asset.content.remove :id "path=%s" :action truncate' % fullpath)
#            else:
#                raise Exception("Non-zero length truncation is not implemented")
#        except Exception as e:
#            self.log.warning("truncate() : %s" % str(e))



        return 0

# NB - release() return code/exceptions are ignored by FUSE - so there's no way to fail the operation from this method
    def release(self, path, fh):
        fullpath = self._remote_fullpath(path)
        namespace = posixpath.dirname(fullpath)
        filename = posixpath.basename(fullpath)

        mfobj = self.mf_wonly.get(fh)
        if mfobj is not None:
# path1 - WRONLY
            try:
#                self.mf_write(mfobj, fh)
                self.mf_write(mfobj.tmpfile, mfobj.buffer, mfobj.length, mfobj.offset, fh)
            except Exception as e:
                self.log.error("release(1) : %s" % str(e))
            try:
                self.mf_client.aterm_run("server.io.write.finish :ticket %d" % fh)
            except Exception as e:
                self.log.error("release(2) : %s" % str(e))
            try:
# allow both create new or overwrite existing
                reply = self.mf_client.aterm_run('service.execute :service -name "asset.set" < :id "path=%s/%s" :store "%s" :create True > :input-ticket %d' % (namespace, filename, mfobj.store, fh))
# update directory cache if it exists (if it doesn't it'll be generated by a server call when needed anyway)
                if namespace in self.asset_cache:
                    self.asset_cache[namespace][filename] = self.inode_new(mode=stat.S_IFREG | 0400, links=1, size=mfobj.total) 

            except Exception as e:
                self.log.error("release(3) : %s" % str(e))
# cleanup 
            del self.inode_cache[path]
# stat for total (end to end) time
            self.iostats.insert('overall_w', mfobj.total, time.time() - mfobj.start)
            self.mf_wonly[fh] = None
        else:
# path2 - RDONLY
# stat for total (end to end) time
            self.iostats.insert('overall_r', self.mf_ronly[fh].total, time.time() - self.mf_ronly[fh].start)
            del self.mf_ronly[fh]


# --- main: process arguments and pass to FUSE
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Local path to present the virtual filesystem")
    parser.add_argument("-c", dest="config", default="pawsey", help="Use the section named [CONFIG] in $HOME/.mf_config")
    parser.add_argument("-s", dest="server", default="data.pawsey.org.au", help="The mediaflux server")
    parser.add_argument("-p", dest="port", default=443, help="The mediaflux port")
    parser.add_argument("-d", dest="domain", default="ivec", help="The mediaflux authentication domain")
    parser.add_argument("-n", dest="namespace", default="/projects", help="Top level mediaflux namespace")
    parser.add_argument("-b", "--background", help="Run in the background", action="store_true")
    parser.add_argument("-r", "--readonly", help="Mount as readonly", action="store_true")
    parser.add_argument("-l", "--logfile", help="Create timestamped logfile", action="store_true")
    parser.add_argument("-v", "--verbose", help="Activate verbose logging", action="store_true")
    args = parser.parse_args()

# default protocol for mediaflux
    if int(args.port) == 80:
        args.protocol = "http"
    else:
        args.protocol = "https"

# main call
    try:
        FUSE(pmount(args), args.path, nothreads=True, foreground=not args.background)
    except Exception as e:
        print("Mount failed: %s" % str(e))
        exit(-1)

