#!/usr/bin/python

"""
This module is a Python 3.x implementation of a simple boto3 client
Author: Sean Fleming
"""

import os
import sys
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

#------------------------------------------------------------
    def whoami(self):
        print("=== %s ===" % self.host)
        if self.prefix is not None:
            print("    %s : access = %r" % (self.prefix, self.access))

#------------------------------------------------------------
    def is_mine(self, path):
        sys.stdout.write("s3client.is_mine() : " + path)
        if self.prefix is not None:
            if path.strip().startswith(self.prefix):
                print(" ... T")
                return True
        print(" ... F")
        return False

#------------------------------------------------------------
# prefix - the keystone project assoc with access/secret ... and trigger for s3client pathways
# VFS style ... TODO - allow for multiple mounts ... eg AWS
    def mount(self, host, access, secret, prefix):
        self.host = host
        self.access = access
        self.secret = secret
        self.prefix = prefix

#------------------------------------------------------------
# helper
    def absolute_remote_filepath(self, path):

        print("abs remote path : [%s]" % path)

        mypath = path.strip()

        if mypath[0] != '/':
            mypath = posixpath.join(self.cwd, mypath)

        return posixpath.normpath(mypath)

#------------------------------------------------------------
    def cd(self, path):
        self.cwd = self.absolute_remote_filepath(path)
        print("s3client.cwd: [%s]" % self.cwd)
        return self.cwd

#------------------------------------------------------------
    def ls(self, path):

        print("s3client.ls [%s]" % path)

        args = []
        head,tail = posixpath.split(path)
        while tail:        
            args.append(tail)
            head,tail = posixpath.split(head)

        s3 = boto3.client('s3', endpoint_url=self.host, aws_access_key_id=self.access, aws_secret_access_key=self.secret)
        print("s3client.test() connect to: %s" % self.host)

        if len(args) > 1:
            bucket_name = args[-2]
            print(" === bucket contents ===")
            for key in s3.list_objects(Bucket=bucket_name)['Contents']:
                print(key['Key'])
        else:
            print("=== project buckets ===")
            response = s3.list_buckets()
            for item in response['Buckets']:
                print(item['CreationDate'], item['Name'])

#------------------------------------------------------------
    def get(self, path):

# TODO - make_abolute() helper
        mypath = path.strip()
        if mypath.startswith('/') is False:
            mypath = posixpath.join(self.cwd, mypath)

        if self.is_mine(mypath) is False:
            raise Exception("Error: bad remote path [%s]" % mypath)

        print("s3client().get [%s]" % mypath)

# two options: get on a bucket, or get on an object in a bucket
        args = []
        head,tail = posixpath.split(mypath)
        while tail:        
            args.append(tail)
            head,tail = posixpath.split(head)

        if len(args) > 2:
#            project_name = args[-1]
            bucket_name = args[-2]
            args.reverse()
            bucket_obj = "/".join(args[2:])
            print("bucket = [%s]" % bucket_name)
            print("object = [%s]" % bucket_obj)

            s3 = boto3.client('s3', endpoint_url=self.host, aws_access_key_id=self.access, aws_secret_access_key=self.secret)
            print("s3client.test() connect to: %s" % self.host)

            file_path = os.path.join(os.getcwd(), bucket_obj)

            print("local file = [%s]" % file_path)

            s3.download_file(bucket_name, bucket_obj, file_path)

        else:
            raise Exception("Error: bad remote path [%s]" % mypath)


#------------------------------------------------------------
    def put(self, line):

        print("s3client().put [%s]" % line)

        raise Exception("Sorry, not implemented yet")

        if self.is_mine(self.cwd) is False:
            raise Exception("Error: bad remote destination [%s]" % self.cwd)





