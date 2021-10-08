#!/usr/bin/python

"""
This module is a Python 3.x implementation of a simple boto3 client
Author: Sean Fleming
"""

import os
import sys
import logging
import pathlib
# deprec in favour of pathlib?
import posixpath

try:
    import boto3
    ok=True
except:
    ok=False

class s3client:
    def __init__(self, host=None, access=None, secret=None):
        self.ok = ok
# TODO - more reworking to replace these things with a generic "endpoint" dict
        self.type = "s3"
        self.host = host
        self.access = access
        self.secret = secret
# TODO - rename to mount or something - prefix is confusing with the aws definition of prefix
        self.prefix = None
        self.cwd = None
        self.s3 = None
        self.status = "not connected"
        self.logger = logging.getLogger('s3client')
# CURRENT
#        self.logger.setLevel(logging.DEBUG)

#------------------------------------------------------------
# prefix - the keystone project assoc with access/secret ... and trigger for s3client pathways
# VFS style ... TODO - allow for multiple mounts ... eg AWS
#    def connect(self, host, access, secret, prefix):
    def connect(self):
        self.logger.info('endpoint=%s using acess=%s and visible on path=%s' % (self.host, self.access, self.prefix))
        try:
            self.s3 = boto3.client('s3', endpoint_url=self.host, aws_access_key_id=self.access, aws_secret_access_key=self.secret)
            self.status = "connected: as access=%s" % self.access
        except Exception as e:
            self.status = "not connected: %s" % str(e)

#------------------------------------------------------------
    def endpoint(self):
        return { 'type':self.type, 'host':self.host, 'access':self.access, 'secret':self.secret, 'prefix':self.prefix }

#------------------------------------------------------------
# deprec -> status
    def whoami(self):
        print("=== %s ===" % self.host)
        if self.prefix is not None:
            print("    %s : access = %r" % (self.prefix, self.access))

#------------------------------------------------------------
# deprec -> pshell controlled via mount
    def is_mine(self, path):

        mypath = pathlib.PurePosixPath(path)
        try:
            if mypath.parts[1] == self.prefix:
                self.logger.info('[%s] = True' % path)
                return True
        except:
            pass
        return False

#------------------------------------------------------------
# split a key into prefix + filter
    def key_split(self, key):
        self.logger.info("key=[%s]" % key)

        mykey = str(key)

        c = mykey.rfind('/') 
        if c == -1:
            prefix = None
            pattern = mykey
        else:
            prefix = mykey[:c]
            pattern = mykey[c+1:]

        self.logger.info("prefix=[%r] pattern=[%r]" % (prefix, pattern))

        return prefix, pattern

#------------------------------------------------------------
# convert fullpath to bucket, key pair
    def path_split(self, fullpath):
        self.logger.info("[%s]" % fullpath)

# convert fullpath to [bucket][object]
        mypath = pathlib.PurePosixPath(fullpath)
        bucket = None 
        key = ""
        count = len(mypath.parts)
        if count > 2:
            bucket = mypath.parts[2]
            head = "%s%s/%s" % (mypath.parts[0], mypath.parts[1], bucket)

# remainder is the object, which will need to have "/" at the end for prefix matching
# FIXME - this may have consequences if it's a reference to an object and not a prefix
# currently though, since any trailing / characters will be stripped, it's difficult to fix
            key = fullpath[1+len(head):]
            if len(key) > 2:
                key = key + "/"

        self.logger.info("bucket=[%r] key=[%r]" % (bucket, key))

        return bucket, key

#------------------------------------------------------------
# TODO - deprecate this in clients??? (ie all done in pshell and we expect fullpath's always)
#    def absolute_remote_filepath(self, path):
#
#        self.logger.debug('in: %s' % path)
#
#        mypath = path.strip()
#        if mypath[0] != '/':
#            mypath = posixpath.join(self.cwd, mypath)
#        mypath = posixpath.normpath(mypath)
#
#        self.logger.debug('out: %s' % mypath)
#
#        return mypath

#------------------------------------------------------------
    def cd(self, fullpath):

        self.logger.debug("[%s]" % fullpath)

        mypath = pathlib.PurePosixPath(fullpath)
        count = len(mypath.parts)
        stop = min(3, len(mypath.parts)) 

        self.cwd = "/"
        for i in range(0,stop):
            self.cwd = posixpath.join(self.cwd, mypath.parts[i])

        self.logger.debug("cwd = [%s]" % self.cwd)

        return self.cwd

#------------------------------------------------------------
    def ls_iter(self, path):

#        if ok is False:
#            raise Exception("Could not find the boto3 library.")

        bucket,key = self.path_split(path)
        self.logger.info("bucket=[%r] key=[%r]" % (bucket, key))

        if bucket is not None:
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=key)
            for page in pages:
                if 'CommonPrefixes' in page:
                    for item in page['CommonPrefixes']:
                        yield "[prefix] %s" % item['Prefix']

                if 'Contents' in page:
                    for item in page['Contents']:
                        if item['Key'].endswith('/') is False:
                            yield "%d B | %s" % (item['Size'], item['Key'])

        else:
            response = self.s3.list_buckets()
            for item in response['Buckets']:
                yield "[Bucket] %s" % item['Name']

#------------------------------------------------------------
    def info(self, path):
        raise Exception("s3client.info() not implemented yet")

#------------------------------------------------------------
    def get(self, path):

        bucket,key = self.path_split(path)
        self.logger.info('remote bucket=[%r] key=[%r]' % (bucket, key))

        local_filepath = os.path.join(os.getcwd(), posixpath.basename(key))
        self.logger.info('downloading to [%s]' % local_filepath)

        self.s3.download_file(str(bucket), str(key), local_filepath)

#------------------------------------------------------------
    def put(self, remote_path, local_filepath):

        self.logger.info('uploading [%s]' % local_filepath)
        bucket,key = self.path_split(remote_path)
        self.logger.info('remote bucket=[%r] key=[%r]' % (bucket, key))

        self.s3.upload_file(local_filepath, bucket, os.path.basename(local_filepath))

#------------------------------------------------------------
    def managed_put(self, upload_list):
        for item in upload_list:
            self.put(item[0], item[1])

#------------------------------------------------------------
    def rm(self, filepath):
        bucket,key = self.path_split(filepath)

        if bucket is not None and key is not None:
# TODO - are you sure (y/n)
            self.s3.delete_object(Bucket=str(bucket), Key=str(key))
        else:
            raise Exception("No valid remote bucket, object in path [%s]" % filepath)

#------------------------------------------------------------
# TODO - this might have to become create bucket/folder -> split the components and then implement separately
    def mkdir(self, path):

        bucket,key = self.path_split(path)

        if bucket is not None and key is None:
            self.s3.create_bucket(Bucket=bucket)
        else:
            raise Exception("No valid remote bucket in path [%s]" % path)

#------------------------------------------------------------
    def rmdir(self, path):
        bucket,key = self.path_split(path)

        if bucket is not None and key is None:
# TODO - are you sure (y/n)
            self.s3.delete_bucket(Bucket=bucket)
        else:
            raise Exception("No valid remote bucket in path [%s]" % path)
