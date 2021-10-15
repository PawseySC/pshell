#!/usr/bin/python

"""
This module is a Python 3.x implementation of a simple s3 client
Author: Sean Fleming
"""

import os
import logging
import pathlib
# deprec in favour of pathlib?
import posixpath

# NEW
from remote import client

try:
    import boto3
    ok=True
except:
    ok=False

# auto 
build= "20211015131216"

#------------------------------------------------------------

class s3_client(client):
    def __init__(self, host=None, access=None, secret=None):
        self.ok = ok
# TODO - more reworking to replace these things with a generic "endpoint" dict
        self.type = "s3"
        self.host = host
        self.access = access
        self.secret = secret
# TODO - rename to mount or something - prefix is confusing with the aws definition of prefix
#        self.prefix = None
        self.cwd = None
        self.s3 = None
        self.status = "not connected"
        self.logging = logging.getLogger('s3client')
        global build

# CURRENT
#        self.logging.setLevel(logging.DEBUG)
        self.logging.info("S3CLIENT=%s" % build)
        self.logging.info("BOTO3=%r" % ok)

#------------------------------------------------------------
# prefix - the keystone project assoc with access/secret ... and trigger for s3client pathways
# VFS style ... TODO - allow for multiple mounts ... eg AWS
#    def connect(self, host, access, secret, prefix):
    def connect(self):
        self.logging.info('endpoint=%s using acess=%s' % (self.host, self.access))
        try:
            self.s3 = boto3.client('s3', endpoint_url=self.host, aws_access_key_id=self.access, aws_secret_access_key=self.secret)
            self.status = "connected: as access=%s" % self.access
        except Exception as e:
            self.status = "not connected: %s" % str(e)

#------------------------------------------------------------
    def endpoint(self):
        return { 'type':self.type, 'host':self.host, 'access':self.access, 'secret':self.secret }

#------------------------------------------------------------
# split a key into prefix + filter
    def key_split(self, key):
        self.logging.info("key=[%s]" % key)

        mykey = str(key)

        c = mykey.rfind('/') 
        if c == -1:
            prefix = None
            pattern = mykey
        else:
            prefix = mykey[:c]
            pattern = mykey[c+1:]

        self.logging.info("prefix=[%r] pattern=[%r]" % (prefix, pattern))

        return prefix, pattern

#------------------------------------------------------------
    def complete_folder(self, partial, start):
        self.logging.info("partial=[%s] start=[%d]" % (partial, start))
# TODO




#------------------------------------------------------------
    def complete_file(self, partial, start):
        self.logging.info("partial=[%s] start=[%d]" % (partial, start))
# TODO



#------------------------------------------------------------
# convert fullpath to bucket, key pair
    def path_split(self, fullpath):
        self.logging.info("[%s]" % fullpath)

# convert fullpath to [bucket][object]
        mypath = pathlib.PurePosixPath(fullpath)
        bucket = None 
        key = ""
        count = len(mypath.parts)
        if count > 2:
            bucket = mypath.parts[2]
            head = "%s%s/%s" % (mypath.parts[0], mypath.parts[1], bucket)
            key = fullpath[1+len(head):]

# remainder is the object, which will need to have "/" at the end for prefix matching
# FIXME - this may have consequences if it's a reference to an object and not a prefix
# currently though, since any trailing / characters will be stripped, it's difficult to fix

# FIXME - doing this will break downloads ... as it will add a / onto the end of the key -> and it won't be found
#            if len(key) > 2:
#                key = key + "/"

        self.logging.info("bucket=[%r] key=[%r]" % (bucket, key))

        return bucket, key

#------------------------------------------------------------
    def cd(self, fullpath):
        mypath = pathlib.PurePosixPath(fullpath)
        count = len(mypath.parts)
        stop = min(3, count) 
        self.cwd = "/"
        for i in range(0,stop):
            self.cwd = posixpath.join(self.cwd, mypath.parts[i])
        return self.cwd

#------------------------------------------------------------
    def ls_iter(self, path):

        bucket,key = self.path_split(path)
        self.logging.info("bucket=[%r] key=[%r]" % (bucket, key))

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
# TODO - wildcards?
    def get_iter(self, pattern):
        yield 1
# FIXME - fake it till you make it
        yield 666
        yield pattern

#------------------------------------------------------------
    def get(self, remote_filepath, local_filepath=None):

        bucket,key = self.path_split(remote_filepath)
        self.logging.info('remote bucket=[%r] key=[%r]' % (bucket, key))

        if local_filepath is None:
            local_filepath = os.path.join(os.getcwd(), posixpath.basename(key))
        self.logging.info('downloading to [%s]' % local_filepath)

        self.s3.download_file(str(bucket), str(key), local_filepath)

        return os.path.getsize(local_filepath)

#------------------------------------------------------------
    def put(self, remote_path, local_filepath):

        bucket,key = self.path_split(remote_path)
        self.logging.info('remote bucket=[%r] key=[%r]' % (bucket, key))

        self.s3.upload_file(local_filepath, bucket, os.path.basename(local_filepath))

#------------------------------------------------------------
    def rm(self, filepath, prompt=None):
        bucket,key = self.path_split(filepath)
        if bucket is not None and key is not None:
            if prompt is not None:
                if prompt("Delete object (y/n)") is False:
                    return
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
    def rmdir(self, path, prompt=None):
        bucket,key = self.path_split(path)

        if bucket is not None and key is None:
            if prompt("Delete bucket (y/n)") is False:
                return
            self.s3.delete_bucket(Bucket=bucket)
        else:
            raise Exception("No valid remote bucket in path [%s]" % path)
