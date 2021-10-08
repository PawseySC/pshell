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
    def __init__(self):
        self.ok = ok
        self.host = None
        self.access = None
        self.secret = None
# TODO - rename to mount or something - prefix is confusing with the aws definition of prefix
        self.prefix = None
        self.cwd = None
        self.s3 = None
        self.type = "s3"
        self.status = "not connected"
        self.logger = logging.getLogger('s3client')
# CURRENT
#        self.logger.setLevel(logging.DEBUG)

#------------------------------------------------------------
# prefix - the keystone project assoc with access/secret ... and trigger for s3client pathways
# VFS style ... TODO - allow for multiple mounts ... eg AWS
    def connect(self, host, access, secret, prefix):
        self.logger.info('endpoint=%s using acess=%s and visible on path=%s' % (host, access, prefix))
        self.host = host
        self.access = access
        self.secret = secret
        self.prefix = prefix
        if ok:
            self.s3 = boto3.client('s3', endpoint_url=self.host, aws_access_key_id=self.access, aws_secret_access_key=self.secret)

#------------------------------------------------------------
    def whoami(self):
        print("=== %s ===" % self.host)
        if self.prefix is not None:
            print("    %s : access = %r" % (self.prefix, self.access))

#------------------------------------------------------------
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
    def path_split(self, path):

        self.logger.info("path=[%s]" % path)
        fullpath = self.absolute_remote_filepath(path)
        self.logger.info("fullpath=[%s]" % fullpath)

# convert fullpath to [bucket][object]
        mypath = pathlib.PurePosixPath(fullpath)
        bucket = None
        key = None
        try:
            if mypath.parts[1] != self.prefix:
                raise Exception("Bad remote path: [%s]" % fullpath)
            bucket = mypath.parts[2]

# HACK - trigger an exception if object reference doesn't exist, otherwise below will generate . instead of None
            key = mypath.parts[3]
            key = mypath.relative_to("/%s/%s" % (self.prefix, bucket))

        except Exception as e:
            self.logger.debug(str(e))

        self.logger.info("bucket=[%r] key=[%r]" % (bucket, key))

        return bucket, key

#------------------------------------------------------------
    def absolute_remote_filepath(self, path):

        self.logger.debug('in: %s' % path)

        mypath = path.strip()
        if mypath[0] != '/':
            mypath = posixpath.join(self.cwd, mypath)
        mypath = posixpath.normpath(mypath)

        self.logger.debug('out: %s' % mypath)

        return mypath

#------------------------------------------------------------
    def cd(self, path):

        self.logger.debug("path = [%s]" % path)

        fullpath = self.absolute_remote_filepath(path)

        bucket, key = self.path_split(fullpath)

        self.logger.debug("bucket = [%r]" % bucket)

# discard key ie enforce flat structure
        if bucket is not None:
            self.cwd = posixpath.join("/" + self.prefix, bucket)
        else:
            self.cwd = fullpath

        self.logger.debug("cwd = [%s]" % self.cwd)

        return self.cwd

#------------------------------------------------------------
# TODO - maybe have *kwargs -> prefix etc -> which could permit more advanced aws style listings

#------------------------------------------------------------
# deprec - difficult and complicated to turn the flat object structure into a traditional filesystem tree
# but if you want to do it, this is a partial implementation
    def ls(self, path):

        if ok is False:
            raise Exception("Could not find the boto3 library.")

        bucket,key = self.path_split(path)

        self.logger.info("bucket=[%r] key=[%r]" % (bucket, key))

        if bucket is not None:
# FIXME - apparently list_objects() is deprec, should use V2 ...
# TODO - apparently there is a paginator too ...
# https://stackoverflow.com/questions/54314563/how-to-get-more-than-1000-objects-from-s3-by-using-list-objects-v2

            if key is not None:
                self.logger.info("listing objects in bucket [%s] with prefix [%s/]" % (str(bucket), str(key)))
                reply = self.s3.list_objects_v2(Bucket=str(bucket), Delimiter='/', Prefix=str(key)+'/', MaxKeys=10)
            else:
                self.logger.info("listing objects in bucket [%s]" % str(bucket))
                reply = self.s3.list_objects_v2(Bucket=str(bucket), Delimiter='/', MaxKeys=10)

# DEBUG
#            print(reply)

# TODO - yield these ... and in pshell - hook to pagination

            if 'CommonPrefixes' in reply:
                for item in reply['CommonPrefixes']:
                    print("[prefix] %s" % item['Prefix'])

            if 'Contents' in reply:
                for item in reply['Contents']:
# NB: if the key of stuff in Contents ends in '/' then it's a prefix placeholder of some kind???
                    if item['Key'].endswith('/') is False:
                        print("%d B | %s" % (item['Size'], item['Key']))

# TODO - pagination
# testing directory = /magenta-storage/mybucket/mediaflux_store_598_4/data

            try:
                is_truncated = reply['IsTruncated']
                if is_truncated:
                    print(" Truncated = %r - TODO - pagination" % is_truncated)

# TODO - parse for continuation token ...
#            self.logger.info("Continue = %r" % reply['ContinuationToken'])

            except Exception as e:
                self.logger.info(str(e))


#            paginator = self.s3.get_paginator('list_objects_v2')
#            pages = paginator.paginate(Bucket=bucket)
#            for page in pages:
#                for item in page['Contents']:
#                    print("%d B | %s" % (item['Size'], item['Key']))

        else:
            response = self.s3.list_buckets()
            for item in response['Buckets']:
                print("[Bucket] %s" % item['Name'])


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
