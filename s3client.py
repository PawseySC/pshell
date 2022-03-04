#!/usr/bin/python

"""
This module is a Python 3.x implementation of a simple s3 client
Author: Sean Fleming
"""

import os
import math
import urllib
import fnmatch
import getpass
import logging
import pathlib
# deprec in favour of pathlib?
import posixpath

try:
    import boto3
    import botocore
    ok=True
except:
    ok=False

# auto 
build= "20211015131216"

#------------------------------------------------------------
class s3_client():
    def __init__(self, url=None, access=None, secret=None):
        self.ok = ok
        self.type = "s3"
        self.url = url
        self.access = access
        self.secret = secret
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

# connection check
        try:
            # pshell threads x boto3 threads cap
            s3config=botocore.client.Config(max_pool_connections=50)
            if 'http' in self.url:
                self.logging.info("Assuming url is endpoint")
                self.logging.info("%r : %r : %r" % (self.url, self.access, self.secret))
                self.s3 = boto3.client('s3', endpoint_url=self.url, aws_access_key_id=self.access, aws_secret_access_key=self.secret, config=s3config)
                self.logging.info("boto3 client ok")
            else:
                self.logging.info("Assuming url is region")
                self.s3 = boto3.client('s3', region_name=self.url, aws_access_key_id=self.access, aws_secret_access_key=self.secret, config=s3config)

# reachability check ... more for info, probably not really required
# CURRENT - this works for acacia - URL ... but breaks AWS 
#            code = urllib.request.urlopen(self.url, timeout=10).getcode()
#            self.logging.info("connection code: %r" % code)

        except Exception as e:
            self.logging.error(str(e))
            self.status = "not connected to %s: %s" % (self.url, str(e))
            return

# authenticated user check
        try:
# TODO - check if we're actually authenticated ... resource discovery ... etc
#            self.s3.get_available_resources()
#            self.s3.get_caller_identity()
            self.status = "authenticated to %s as access=%s" % (self.url, self.access)
        except Exception as e:
            self.status = "not authenticated to %s: %s" % (self.url, str(e))

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
    def bucket_exists(self, bucket):
        try:
            response = self.s3.head_bucket(Bucket=bucket)
            return True
        except:
            pass
        return False

#------------------------------------------------------------
    def complete_path(self, cwd, partial, start, match_prefix=True, match_object=True):

        self.logging.info("cwd=[%s] partial=[%s] start=[%d]" % (cwd, partial, start))

        try:
            fullpath = posixpath.join(cwd, partial)
            self.logging.info("fullpath=[%s]" % fullpath)
            bucket, key = self.path_split(fullpath)
            candidate_list = []
            if self.bucket_exists(bucket) is False:
# setup for bucket search
                self.logging.info("bucket search: partial bucket=[%s]" % bucket)
                response = self.s3.list_buckets()
                for item in response['Buckets']:
                    if item['Name'].startswith(partial):
#                        candidate_list.append(item['Name'])
                        candidate_list.append(item['Name']+'/')
            else:
# setup for prefix/object search
                prefix_ix = key.rfind('/')
                if prefix_ix > 0:
                    prefix = key[:prefix_ix+1]
                else:
                    prefix = ""
                self.logging.info("prefix search: bucket=[%s] prefix=[%s] pattern=[%s] start=[%s]" % (bucket, prefix, key, start))
                response = self.s3.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix) 
# process folder (prefix) matches
                if match_prefix is True:
                    if 'CommonPrefixes' in response:
                        for item in response['CommonPrefixes']:
                            if item['Prefix'].startswith(key):
                                tail = item['Prefix'][len(key):]
                                candidate = partial + tail
                                self.logging.info("tail=[%s] : candidate=[%s]" % (tail, candidate))
                                candidate_list.append(candidate)
# process file (object) matches
                if match_object is True:
                    if 'Contents' in response:
                        for item in response['Contents']:
                            if item['Key'].startswith(key):
                                # TODO - do we need to do something like the prefix match?
                                self.logging.info("raw object candidate: [%s]" % item['Key'])
                                candidate_list.append(item['Key'][start:])

        except Exception as e:
            self.logging.error(str(e))

        self.logging.info(candidate_list)

        return candidate_list

#------------------------------------------------------------
    def complete_folder(self, cwd, partial, start):
        return self.complete_path(cwd, partial, start, match_prefix=True, match_object=False)

#------------------------------------------------------------
    def complete_file(self, cwd, partial, start):
        return self.complete_path(cwd, partial, start, match_prefix=True, match_object=True)

#------------------------------------------------------------
# NEW - convert fullpath to bucket, commonprefix, and key (which might contain wildcards)
    def path_convert(self, path):
        self.logging.info("path=[%s]" % path)

        if posixpath.isabs(path) is False:
            self.logging.debug("Warning: converting relative path to absolute")
            path = '/' + path

        fullpath = posixpath.normpath(path)
        mypath = pathlib.PurePosixPath(fullpath)
        count = len(mypath.parts)
        prefix = ""
        key = ""
        if count > 1:
            bucket = mypath.parts[1]
            if path.endswith('/') or count==2:
                # path contains no key
                prefix_last = count
            else:
                # otherwise key is last item
                prefix_last = count-1
                key = mypath.parts[count-1]
            head = "%s%s" % (mypath.parts[0], mypath.parts[1])
            for i in range(2, prefix_last):
                prefix = posixpath.join(prefix, mypath.parts[i])
# prefixes (if not empty) MUST always end in '/'
            if prefix_last > 2:
                prefix = prefix + '/'
        else:
            bucket = None

        self.logging.info("bucket=[%r] prefix=[%r] key=[%s]" % (bucket, prefix, key))

        return bucket, prefix, key

#------------------------------------------------------------
# TODO - rework complete_path() and get rid of this (superceeded by path_convert)
# convert fullpath to bucket, and full object key
    def path_split(self, path):
        self.logging.info("path=[%s]" % path)
        fullpath = posixpath.normpath(path)
# we only accept absolute paths for s3
        if posixpath.isabs(fullpath) is False:
            self.logging.info("Warning: converting relative path to absolute")
            fullpath = '/'+fullpath
        mypath = pathlib.PurePosixPath(fullpath)
        bucket = None 
        key = ""
        count = len(mypath.parts)
        if count > 1:
            bucket = mypath.parts[1]
            head = "%s%s" % (mypath.parts[0], mypath.parts[1])
            key = fullpath[1+len(head):]

# normpath will remove trailing slash, but this is meaningful for navigation if we have non-empty key
        if bucket is not None:
            if path.endswith('/') and len(key) > 0:
                key = key + '/'

        self.logging.info("bucket=[%r] key=[%s]" % (bucket, key))

        return bucket, key

#------------------------------------------------------------
    def cd(self, path):
# all paths must end in /
        fullpath = posixpath.normpath(path)
        if fullpath.endswith('/') is False:
            fullpath += '/'
        self.logging.info("input fullpath=[%s]" % fullpath)
        bucket,prefix,key = self.path_convert(fullpath)
# check for existence
        exists = False
        try:
            if bucket is None:
# root level
                exists = True
            else:
# bucket level
                if prefix == "":
                    response = self.s3.list_buckets()
                    for item in response['Buckets']:
                        if item['Name'] == bucket:
                            exists = True
# prefix (subdir) levels
                else:
                    self.logging.info("bucket=[%s] prefix=[%s]" % (bucket, prefix))
                    response = self.s3.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix) 
# if the path contains objects or prefixes then it is valid
                    if 'Contents' in response:
                        exists = True
                    if 'CommonPrefixes' in response:
                        exists = True

        except Exception as e:
            self.logging.error(str(e))

# return path (for setting cwd) if exists
        if exists is True:
            self.logging.info("output fullpath=[%s]" % fullpath)
            return fullpath

        raise Exception("Could not find remote path: [%s]" % fullpath)

#------------------------------------------------------------
    def ls_iter(self, path):

        bucket,prefix,key = self.path_convert(path)
# NEW - trim the input prefix from all returned results (will look more like a normal filesystem)
        prefix_len = len(prefix)

        if bucket is not None:
            paginator = self.s3.get_paginator('list_objects_v2')
            do_match = True
            if len(key) == 0:
                do_match = False
            page_list = paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=prefix)
            for page in page_list:
                if 'CommonPrefixes' in page:
                    for item in page.get('CommonPrefixes'):
                        if do_match: 
                            if fnmatch.fnmatch(item['Prefix'], key) is False:
                                continue
                        yield "[Folder] %s" % item['Prefix'][prefix_len:]

                if 'Contents' in page:
                    for item in page.get('Contents'):
                        if do_match:
                            if fnmatch.fnmatch(item['Key'], key) is False:
                                continue
                        yield "%s | %s" % (self.human_size(item['Size']), item['Key'][prefix_len:])
        else:
            response = self.s3.list_buckets()
            for item in response['Buckets']:
                yield "[Bucket] %s" % item['Name']

#------------------------------------------------------------
# return number, size of objects that match the pattern, followed by the URL to the objects
    def get_iter(self, pattern, delimiter='/'):
        bucket,prefix,key = self.path_convert(pattern)
        self.logging.info("bucket=[%s], prefix=[%s], key=[%s]" % (bucket, prefix, key))

# match everything and recurse if no key supplied (ie get on a folder)
        if len(key) == 0:
            # match all keys
            key = '*'
            # recursively
            delimiter = ""
# build full filename (prefix+key) matching string
        key_pattern = posixpath.join(prefix, key)
        self.logging.info("key_pattern=[%s], delimiter=[%s]" % (key_pattern, delimiter))

# attempt to compute size of the match
        count = 0
        size = 0
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Delimiter=delimiter, Prefix=prefix):
                for item in page.get('Contents'):
                    if fnmatch.fnmatch(item['Key'], key_pattern):
                        count += 1
                        size += item['Size']
        except Exception as e:
# failed ... usually means no 'Contents' in page
            self.logging.debug(str(e))

# nothing found - terminate iterator
        if count == 0:
            raise Exception("Could not find a match for [%s]" % pattern)

# return the number and size of match
        yield count
        yield size

# iterate to yield the actual objects
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Delimiter=delimiter, Prefix=prefix):
            for item in page.get('Contents'):
                if fnmatch.fnmatch(item['Key'], key_pattern):
                    yield "/%s/%s" % (bucket, item['Key'])

#------------------------------------------------------------
    def get(self, remote_filepath, local_filepath=None):

        bucket,prefix,key = self.path_convert(remote_filepath)
        fullkey = posixpath.join(prefix, key)

        self.logging.info('remote bucket=[%r] fullkey=[%r] : local_filepath=[%r]' % (bucket, fullkey, local_filepath))

        if local_filepath is None:
            local_filepath = os.path.normpath(os.path.join(os.getcwd(), posixpath.basename(fullkey)))

        self.logging.info('Downloading to [%s]' % local_filepath)

# make any intermediate folders required ...
        local_parent = os.path.dirname(local_filepath)
        if os.path.exists(local_parent) is False:
            self.logging.debug("Creating required local folder(s): [%s]" % local_parent)
            os.makedirs(local_parent)

# can tweak this, default concurrency is 10
#        from boto3.s3.transfer import TransferConfig
#        config = TransferConfig(max_concurrency=5)
#        self.s3.download_file(str(bucket), str(fullkey), local_filepath, Config=config)

        self.s3.download_file(str(bucket), str(fullkey), local_filepath)

        return os.path.getsize(local_filepath)

#------------------------------------------------------------
    def put(self, remote_path, local_filepath):
        bucket,prefix,key = self.path_convert(remote_path+'/')
        filename = os.path.basename(local_filepath)
        fullkey = posixpath.join(prefix, filename)

# attempt to get remote size (if exists) and then local size for comparison
        try:
            response = self.s3.head_object(Bucket=bucket, Key=fullkey)
            rsize = int(response['ResponseMetadata']['HTTPHeaders']['content-length'])
            lsize = os.path.getsize(local_filepath)
            if lsize == rsize:
                self.logging.info("File of same size already exists, skipping [%s]" % local_filepath)
                return
        except Exception as e:
            # file doesn't exist (or couldn't get size)
            self.logging.debug(str(e))

        self.s3.upload_file(local_filepath, bucket, fullkey)

#------------------------------------------------------------
    def rm(self, pattern, prompt=None):

        results = self.get_iter(pattern)
        count = int(next(results))
        size = int(next(results))

        if prompt is not None:
            if prompt("Delete %d objects, size: %s (y/n)" % (count,self.human_size(size))) is False:
                return False

        for filepath in results:
            bucket,prefix,key = self.path_convert(filepath)
            fullkey = posixpath.join(prefix, key)

# TODO - delete_objects() more efficient if lots of matches
            if bucket is not None:
                self.s3.delete_object(Bucket=str(bucket), Key=str(fullkey))
            else:
                raise Exception("No valid remote bucket, object in path [%s]" % filepath)

        return True

#------------------------------------------------------------
    def mkdir(self, path):
        bucket,prefix,key = self.path_convert(path)

        if bucket is not None:
            if prefix == "":
# create a bucket if top level
                self.logging.debug("Creating bucket [%s]" % bucket)
                self.s3.create_bucket(Bucket=bucket)
                return
            else:
# create an empty object with / appended to the key to simulate a folder
                self.logging.debug("Creating prefix [%s] in bucket [%s]" % (prefix, bucket))
                self.s3.put_object(Bucket=bucket, Key=prefix, Body='')
                return

        raise Exception("Bad input bucket [%s] or prefix [%s] in folder [%s]" % (bucket, prefix, path))

#------------------------------------------------------------
    def rmdir(self, path, prompt=None):
        bucket,prefix,key = self.path_convert(path)

#        if key != "":
#            raise Exception("Bad input path [%s], missing / terminating character" % path)

        if bucket is not None and key == "":
            if prefix == "":
# remove bucket if top level
                if prompt is not None:
                    if prompt("Delete bucket %s (y/n)" % bucket) is False:
                        return False
                self.logging.debug("Removing bucket [%s]" % bucket)
                self.s3.delete_bucket(Bucket=bucket)
                return True
            else:
# recursive get on objects if we have a prefix (folder)
                results = self.get_iter(path, '')
                count = int(next(results))
                size = int(next(results))
                if prompt is not None:
                    if prompt("Delete %d objects, size: %s (y/n)" % (count, self.human_size(size))) is False:
                        return False

                for item in results:
                    bucket,prefix,key = self.path_convert(item)
                    fullkey = posixpath.join(prefix, key)
                    self.s3.delete_object(Bucket=bucket, Key=fullkey)

                return True

        raise Exception("Invalid bucket, prefix, or key specified in folder [%s]" % path)

#------------------------------------------------------------
    def publish(self, pattern):
        results = self.get_iter(pattern)
        count = int(next(results))
        size = int(next(results))
        print("Publishing %d files..." % count)
        for filepath in results:
            self.logging.info("s3 publish: %s" % filepath)
            bucket,prefix,key = self.path_convert(filepath)
            fullkey = posixpath.join(prefix, key)

# try different expiry times ... no limit?
#            url = self.s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=3600)
            url = self.s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': bucket, 'Key': fullkey}, ExpiresIn=3600000)
            print("public url = %s" % url)

        return(count)

#------------------------------------------------------------
    def info(self, path):
        self.logging.info("HEAD on [%s]" % path)
        bucket,prefix,key = self.path_convert(path)
        fullkey = posixpath.join(prefix, key)
        response = self.s3.head_object(Bucket=bucket, Key=fullkey)

        print(response)

        return response['ResponseMetadata']['HTTPHeaders']

#------------------------------------------------------------
    def bucket_usage(self, bucket):
        count = 0
        size = 0
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket):
            if 'Contents' in page:
                for item in page.get('Contents'):
                    count += 1
                    size += item['Size']
        return count, size

#------------------------------------------------------------
# NEW - experimental ...
    def usage(self, path, recursive=True):

        bucket,prefix,key = self.path_convert(path)

        if bucket is not None:
            count, size = self.bucket_usage(bucket)
            print("[%s] has %d objects, total size: %s" % (bucket, count, self.human_size(size)))
        else:
            response = self.s3.list_buckets()
            for item in response['Buckets']:
                bucket = item['Name']
                count, size = self.bucket_usage(bucket)
                print("[%s] has %d objects, total size: %s" % (bucket, count, self.human_size(size)))

#------------------------------------------------------------
    def info_iter(self, pattern):
        bucket,prefix,key = self.path_convert(pattern)
# bucket and/or prefix request
        if key == "":
            if bucket is not None:
                count, size = self.bucket_usage(bucket)
                yield "[%s] has %d objects, total size: %s" % (bucket, count, self.human_size(size))
            else:
                response = self.s3.list_buckets()
                for item in response['Buckets']:
                    bucket = item['Name']
                    count, size = self.bucket_usage(bucket)
                    yield "[%s] has %d objects, total size: %s" % (bucket, count, self.human_size(size))
# exact key request
        else:
            fullkey = posixpath.join(prefix, key)
            response = self.s3.head_object(Bucket=bucket, Key=fullkey)

            for item in response['ResponseMetadata']['HTTPHeaders']:
                yield "%20s : %s" % (item, response['ResponseMetadata']['HTTPHeaders'][item])

