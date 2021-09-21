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
        self.prefix = None
        self.cwd = None
        self.s3 = None
        self.logger = logging.getLogger('s3client')
# CURRENT
        self.logger.setLevel(logging.DEBUG)

#------------------------------------------------------------
# prefix - the keystone project assoc with access/secret ... and trigger for s3client pathways
# VFS style ... TODO - allow for multiple mounts ... eg AWS
    def connect(self, host, access, secret, prefix):
        self.host = host
        self.access = access
        self.secret = secret
        self.prefix = prefix
        if ok:
            self.s3 = boto3.client('s3', endpoint_url=self.host, aws_access_key_id=self.access, aws_secret_access_key=self.secret)
            self.logger.info('%s using %s' % (self.host, self.access))

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
    def path_split(self, fullpath):

# convert fullpath to [bucket][object]
        mypath = pathlib.PurePosixPath(fullpath)
        bucket = None
        object = None
        try:
            if mypath.parts[1] != self.prefix:
                raise Exception("Bad remote path: [%s]" % fullpath)
            bucket = mypath.parts[2]

# HACK - trigger an exception if object reference doesn't exist, otherwise below will generate . instead of None
            object = mypath.parts[3]
            object = mypath.relative_to("/%s/%s" % (self.prefix, bucket))

        except Exception as e:
            self.logger.debug(str(e))

        self.logger.info("[%s] -> [%s][%s]" % (fullpath, bucket, object))

        return bucket, object

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
        self.cwd = self.absolute_remote_filepath(path)
        self.logger.debug('%s' % self.cwd)
        return self.cwd

#------------------------------------------------------------
    def ls(self, path):

        if ok is False:
            raise Exception("Could not find the boto3 library.")

        bucket,object = self.path_split(path)

        if bucket is not None:
# FIXME - apparently list_objects() is deprec, should use V2 ...
# TODO - apparently there is a paginator too ...
# https://stackoverflow.com/questions/54314563/how-to-get-more-than-1000-objects-from-s3-by-using-list-objects-v2
            reply = self.s3.list_objects(Bucket=bucket, MaxKeys=100)
            for item in reply['Contents']:
                print("%d B | %s" % (item['Size'], item['Key']))
        else:
            response = self.s3.list_buckets()
            for item in response['Buckets']:
                print("[Bucket] %s" % item['Name'])

#------------------------------------------------------------
    def get(self, path):

        self.logger.info('[%s]' % path)
        mypath = self.absolute_remote_filepath(path)
        bucket,object = self.path_split(mypath)

        if ok is False:
            raise Exception("Could not find the boto3 library.")

        if object is not None:
            file_path = os.path.join(os.getcwd(), object)
            self.s3.download_file(bucket, object, file_path)
            self.logger.debug('downloaded to [%s]' % file_path)
        else:
            raise Exception("No object to download in path [%s]" % mypath)

#------------------------------------------------------------
    def put(self, remote_path, local_filepath):

        self.logger.info('[%s] -> [%s]' % (local_filepath, remote_path))
        bucket,object = self.path_split(remote_path)

        if ok is False:
            raise Exception("Could not find the boto3 library.")

        if bucket is not None:
            response = self.s3.upload_file(local_filepath, bucket, os.path.basename(local_filepath))
        else:
            raise Exception("No remote bucket in path [%s]" % remote_path)

#------------------------------------------------------------
    def managed_put(self, upload_list):
        for item in upload_list:
            self.put(item[0], item[1])

#------------------------------------------------------------
    def delete_object(self, filepath):
        bucket,object = self.path_split(filepath)

        if bucket is not None and object is not None:
# TODO - are you sure (y/n)
            self.s3.delete_object(Bucket=str(bucket), Key=str(object))
        else:
            raise Exception("No valid remote bucket, object in path [%s]" % filepath)

#------------------------------------------------------------
    def create_bucket(self, path):

        bucket,object = self.path_split(path)

        if bucket is not None and object is None:
            self.s3.create_bucket(Bucket=bucket)
        else:
            raise Exception("No valid remote bucket in path [%s]" % path)

#------------------------------------------------------------
    def delete_bucket(self, path):
        bucket,object = self.path_split(path)

        if bucket is not None and object is None:
# TODO - are you sure (y/n)
            self.s3.delete_bucket(Bucket=bucket)
        else:
            raise Exception("No valid remote bucket in path [%s]" % path)
