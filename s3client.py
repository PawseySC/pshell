#!/usr/bin/python

"""
This module is a Python 3.x implementation of a simple boto3 client
Author: Sean Fleming
"""

import os
import sys
import logging
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
    def whoami(self):
        print("=== %s ===" % self.host)
        if self.prefix is not None:
            print("    %s : access = %r" % (self.prefix, self.access))

#------------------------------------------------------------
    def is_mine(self, path):
        if self.prefix is not None:
            if path.strip().startswith(self.prefix):
                self.logger.debug('[%s] = True' % path)
                return True
        return False

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

        args = []
        head,tail = posixpath.split(path)
        while tail:        
            args.append(tail)
            head,tail = posixpath.split(head)

        if len(args) > 1:
            bucket_name = args[-2]

# FIXME - apparently list_objects() is deprec, should use V2 ...
# TODO - apparently there is a paginator too ...
# https://stackoverflow.com/questions/54314563/how-to-get-more-than-1000-objects-from-s3-by-using-list-objects-v2

            reply = self.s3.list_objects(Bucket=bucket_name, MaxKeys=100)
            for item in reply['Contents']:
                print("%d B | %s" % (item['Size'], item['Key']))
        else:
            response = self.s3.list_buckets()
            for item in response['Buckets']:
                print("[Bucket] %s" % item['Name'])

#------------------------------------------------------------
    def get(self, path):

        mypath = self.absolute_remote_filepath(self, path)
        if self.is_mine(mypath) is False:
            raise Exception("Error: bad remote path [%s]" % mypath)

# two options: get on a bucket, or get on an object in a bucket
        args = []
        head,tail = posixpath.split(mypath)
        while tail:        
            args.append(tail)
            head,tail = posixpath.split(head)

        if ok is False:
            raise Exception("Could not find the boto3 library.")

        if len(args) > 2:
#            project_name = args[-1]
            bucket_name = args[-2]
            args.reverse()
            bucket_obj = "/".join(args[2:])

            self.logger.debug('bucket=[%s] object=[%s]' % (bucket_name, bucket_obj))

            file_path = os.path.join(os.getcwd(), bucket_obj)
            self.s3.download_file(bucket_name, bucket_obj, file_path)

            self.logger.debug('downloaded to [%s]' % file_path)

        else:
            raise Exception("Error: bad remote path [%s]" % mypath)

#------------------------------------------------------------
    def put(self, remote_path, local_filepath):

        self.logger.info('[%s] -> [%s]' % (local_filepath, remote_path))

        raise Exception("Sorry, not implemented yet")


#------------------------------------------------------------
    def managed_put(self, upload_list):
        for item in upload_list:
            self.put(item[0], item[1])

#------------------------------------------------------------
    def delete_object(self, filepath):
        self.logger.info("TODO - [%s]" % filepath)
        raise Exception("Sorry, not implemented yet")

#------------------------------------------------------------
    def create_bucket(self, path):
        self.logger.info("TODO - [%s]" % path)
        raise Exception("Sorry, not implemented yet")

#------------------------------------------------------------
    def delete_bucket(self, path):
        self.logger.info("TODO - [%s]" % path)
        raise Exception("Sorry, not implemented yet")
