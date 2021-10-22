#!/usr/bin/python

"""
This module is a Python 3.x implementation of a simple s3 client
Author: Sean Fleming
"""

import os
import math
import getpass
import logging
import pathlib
# deprec in favour of pathlib?
import posixpath
import remote

try:
    import boto3
    ok=True
except:
    ok=False

# auto 
build= "20211015131216"

#------------------------------------------------------------

class s3_client(remote.client):
    def __init__(self, url=None, access=None, secret=None):
        self.ok = ok
        self.type = "s3"
        self.url = url
        self.access = access
        self.secret = secret
        self.cwd = None
        self.s3 = None
        self.status = "not connected"
        self.logging = logging.getLogger('s3client')
        global build

# DEBUG
#        self.logging.setLevel(logging.DEBUG)
        self.logging.info("S3CLIENT=%s" % build)
        self.logging.info("BOTO3=%r" % ok)

# --- NEW
    @classmethod
    def from_endpoint(cls, endpoint):
        """
        Create s3client using an endpoint description
        """
        client = cls()
        if 'url' in endpoint:
            client.url = endpoint['url']
        if 'access' in endpoint:
            client.access = endpoint['access']
        if 'secret' in endpoint:
            client.secret = endpoint['secret']
        return client

#------------------------------------------------------------
    def connect(self):
        self.logging.info('endpoint=%s using acess=%s' % (self.url, self.access))
        try:
            self.s3 = boto3.client('s3', endpoint_url=self.url, aws_access_key_id=self.access, aws_secret_access_key=self.secret)
            self.status = "connected: as access=%s" % self.access
        except Exception as e:
            self.status = "not connected: %s" % str(e)

#------------------------------------------------------------
    def login(self, access=None, secret=None):
        if access is None:
            self.access = input("Access: ")
            self.secret = getpass.getpass("Secret: ")
        self.connect()

#------------------------------------------------------------
    def endpoint(self):
        return { 'type':self.type, 'url':self.url, 'access':self.access, 'secret':self.secret }

#------------------------------------------------------------
    def human_size(self, nbytes):
        suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']

        try:
            nbytes = int(nbytes)
        except Exception as e:
            self.logging.debug("Bad input [%r]: %s" % (nbytes, str(e)))
            nbytes = 0

        if nbytes:
            rank = int((math.log10(nbytes)) / 3)
            rank = min(rank, len(suffixes) - 1)
            human = nbytes / (1000.0 ** rank)
            f = ("%.2f" % human).rstrip('0').rstrip('.')
        else:
            f = "0"
            rank = 0

        return "%6s %-2s" % (f, suffixes[rank])

#------------------------------------------------------------
    def complete_path(self, partial, start, match_prefix=True, match_object=True):

        self.logging.info("partial=[%s] start=[%d]" % (partial, start))

        try:
            fullpath = posixpath.join(self.cwd, partial)
            self.logging.info("fullpath=[%s]" % fullpath)
            bucket, key = self.path_split(fullpath)

            candidate_list = []

            if key == "":
                self.logging.info("bucket search")

                response = self.s3.list_buckets()
                for item in response['Buckets']:
                    if item['Name'].startswith(partial):
                        candidate_list.append(item['Name'])
            else:
                self.logging.info("prefix search")

                prefix_ix = key.rfind('/')
                if prefix_ix > 0:
                    self.logging.info(prefix_ix)
                    prefix = key[:prefix_ix+1]
                else:
                    self.logging.info(prefix_ix)
                    prefix = ""

                self.logging.info("bucket=[%s] prefix=[%s] pattern=[%s]" % (bucket, prefix, key))

                response = self.s3.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix) 
#                print(response)

                if match_prefix is True:
                    if 'CommonPrefixes' in response:
                        for item in response['CommonPrefixes']:
                            if item['Prefix'].startswith(key):
                                candidate_ix = item['Prefix'].rfind(partial)
                                candidate_list.append(item['Prefix'][candidate_ix+start:])

                if match_object is True:
                    if 'Contents' in response:
                        for item in response['Contents']:
                            if item['Key'].startswith(key):
                                # TODO - do we need to do something like the prefix match?
                                candidate_list.append(item['Key'][start:])


        except Exception as e:
            self.logging.error(str(e))

        self.logging.info(candidate_list)

        return candidate_list

#------------------------------------------------------------
    def complete_folder(self, partial, start):
        return self.complete_path(partial, start, match_prefix=True, match_object=False)

#------------------------------------------------------------
    def complete_file(self, partial, start):
        return self.complete_path(partial, start, match_prefix=True, match_object=True)

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

        self.logging.info("bucket=[%r] key=[%r]" % (bucket, key))

        return bucket, key

#------------------------------------------------------------
    def cd(self, fullpath):

        self.logging.info("input fullpath=[%s]" % fullpath)
# make it easier to extract the last path item (/ may or may not be terminating fullpath)
        if fullpath[-1] == '/':
            fullpath = fullpath[:-1]
        bucket,key = self.path_split(fullpath)
        self.logging.info("bucket=[%r] key=[%r]" % (bucket, key))
        exists = False
        try:
            if bucket is None:
                # root level
                exists = True
            else:
                if key == "":
                    response = self.s3.list_buckets()
                    for item in response['Buckets']:
                        if item['Name'] == bucket:
                            exists = True
                else:
                    prefix_ix = key.rfind('/')
                    prefix = key[:prefix_ix+1]
                    pattern = key+'/'
                    self.logging.info("bucket=[%s] prefix=[%s] pattern=[%s]" % (bucket, prefix, pattern))
                    response = self.s3.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix) 
#                    print(response)
                    if 'CommonPrefixes' in response:
                        for item in response['CommonPrefixes']:
                            if item['Prefix'] == pattern:
                                exists = True

        except Exception as e:
            self.logging.error(str(e))

        if exists is True:
            self.cwd = fullpath
        else:
            self.logging.info("Could not find remote path: [%s]" % fullpath)

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
                            yield "%s | %s" % (self.human_size(item['Size']), item['Key'])

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
