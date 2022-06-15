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
        self.enable_polling = True
        self.logging = logging.getLogger('s3client')
        global build

# DEBUG
#        self.logging.setLevel(logging.DEBUG)
        self.logging.debug("S3CLIENT=%s" % build)
        self.logging.debug("BOTO3=%r" % ok)

# --- NEW
    @classmethod
    def from_endpoint(cls, endpoint):
        """
        Create s3client using an endpoint description
        """
        client = cls()
        if 'url' in endpoint:
            client.url = endpoint['url']
            client.status = "not connected to: %s" % client.url
        if 'access' in endpoint:
            client.access = endpoint['access']
        if 'secret' in endpoint:
            client.secret = endpoint['secret']

        return client

#------------------------------------------------------------
    def connect(self):
        self.logging.info('endpoint=%s using acess=%s' % (self.url, self.access))

# connection check
        emsg = "unknown error"
        try:
            # pshell threads x boto3 threads cap
            s3config=botocore.client.Config(max_pool_connections=50)
            if 'http' in self.url:
                self.logging.debug("Assuming url is endpoint")
                self.s3 = boto3.client('s3', endpoint_url=self.url, aws_access_key_id=self.access, aws_secret_access_key=self.secret, config=s3config)
            else:
                self.logging.debug("Assuming url is region")
                self.s3 = boto3.client('s3', region_name=self.url, aws_access_key_id=self.access, aws_secret_access_key=self.secret, config=s3config)
# authenticated user check - test the client
            self.s3.list_buckets()
            self.status = "authenticated to: %s as access=%s" % (self.url, self.access)

        except Exception as e:
            emsg = str(e)
            self.logging.error(emsg)
            if "InvalidAccessKeyId" in emsg:
                emsg = "access=%s and secret were invalid" % self.access
            if "Unable to locate credentials" in emsg:
                emsg = "no access/secret" 
            self.status = "not connected to %s: %s" % (self.url, emsg)

# CURRENT - don't raise exception - this will prevent the remote from being set ... and so can't even login

# we didn't get a verified connection
#        raise Exception("Failed to connect: %s" % emsg)

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
    def polling(self, polling_state=True):
        """
        Set the current polling state, intended for terminating threads
        """
        self.enable_polling = polling_state

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
            bucket, prefix, pattern = self.path_convert(fullpath)
            self.logging.info("fullpath=%s, bucket=%s, prefix=%s, pattern=%s" % (fullpath, bucket, prefix, pattern))
            candidate_list = []
            if self.bucket_exists(bucket) is False:
# get results for bucket search
                self.logging.info("bucket search: partial bucket=[%s]" % bucket)
                response = self.s3.list_buckets()
                for item in response['Buckets']:
                    if item['Name'].startswith(partial):
                        candidate = item['Name']+'/'
                        candidate_list.append(candidate[start:])
            else:
# get results for non-bucket searches
                response = self.s3.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix) 
                prefix_len = len(prefix)
# process folder (prefix) matches
                if match_prefix is True:
                    if 'CommonPrefixes' in response:
                        for item in response['CommonPrefixes']:
                            # strip the base search prefix (if any) off the results so we can pattern match
                            candidate = item['Prefix'][prefix_len:]
                            self.logging.debug("prefix=%s, candidate=%s" % (item['Prefix'], candidate))
                            # main search criteria
                            if candidate.startswith(pattern):
                                full_candidate = "%s/%s" % (bucket, item['Prefix'])
                                match_ix = full_candidate.rfind(partial)
                                self.logging.info("MATCH index=%d, full=%s" % (match_ix, full_candidate))
                                if match_ix >= 0:
                                    candidate = full_candidate[match_ix:]
                                    candidate_list.append(candidate[start:])

# process file (object) matches
                if match_object is True:
                    if 'Contents' in response:
                        for item in response['Contents']:
                            # main search criteria
                            full_candidate = "/%s/%s" % (bucket, item['Key'])
                            self.logging.debug("key=%s, full=%s" % (item['Key'], full_candidate))
                            if full_candidate.startswith(fullpath):
                                match_ix = full_candidate.rfind(partial)
                                self.logging.info("MATCH index=%d, full=%s" % (match_ix, full_candidate))
                                if match_ix >= 0:
                                    candidate = full_candidate[match_ix:]
                                    candidate_list.append(candidate[start:])

        except Exception as e:
            self.logging.error(str(e))

# done
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
        self.logging.debug("path=[%s]" % path)

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

        self.logging.debug("bucket=[%r] prefix=[%r] key=[%s]" % (bucket, prefix, key))

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

# return the number and size of match
        yield count
        yield size

# nothing found - terminate iterator
        if count == 0:
            raise Exception("Could not find a match for [%s]" % pattern)

# iterate to yield the actual objects
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Delimiter=delimiter, Prefix=prefix):
            for item in page.get('Contents'):
                if fnmatch.fnmatch(item['Key'], key_pattern):
                    yield "/%s/%s" % (bucket, item['Key'])

#------------------------------------------------------------
    def get(self, remote_filepath, local_filepath=None, cb_progress=None):

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

        self.s3.download_file(str(bucket), str(fullkey), local_filepath, Callback=cb_progress)

        return(0)

#------------------------------------------------------------
    def put(self, remote_path, local_filepath, cb_progress=None):
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

        self.s3.upload_file(local_filepath, bucket, fullkey, Callback=cb_progress)

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
        bucket,prefix,pattern = self.path_convert(path)

        if bucket is not None:
            if prefix == "" and pattern == "":
# create a bucket if at top level
                self.logging.info("Creating bucket [%s]" % bucket)
                self.s3.create_bucket(Bucket=bucket)
                return
            else:
# build the full prefix
                folder = posixpath.join(prefix, pattern)
                if folder.endswith('/') is False:
                    folder = folder + '/'

# create an empty object to simulate a folder
                self.logging.info("Creating folder [%s] in bucket [%s]" % (folder, bucket))
                self.s3.put_object(Bucket=bucket, Key=folder, Body='')
                return

        raise Exception("mkdir - bad input path=%s" % path)

#------------------------------------------------------------
    def rmdir(self, path, prompt=None):
        bucket,prefix,key = self.path_convert(path)

        if bucket is not None and key == "":
# recursive get on objects that match the bucket + prefix
            results = self.get_iter(path, '')
            count = int(next(results))
            size = int(next(results))
            if (count > 0):
                if prompt is not None:
                    if prompt("Are you sure you want to delete %d objects, size=%s (y/n)" % (count, self.human_size(size))) is False:
                        return False
# delete all matching objects (if any)
                for item in results:
                    bucket,prefix,key = self.path_convert(item)
                    fullkey = posixpath.join(prefix, key)
                    self.s3.delete_object(Bucket=bucket, Key=fullkey)
# delete bucket if at root (bucket) level
            if prefix == "":
                self.logging.info("Attempting to remove empty bucket [%s]" % bucket)
                self.s3.delete_bucket(Bucket=bucket)

# this fails - no concept of 'resource' in ceph?
#                bucket_resource = boto3.resource('s3').Bucket(bucket)
#                bucket_resource.objects.all().delete()
#                bucket_resource.delete()

            return True

        raise Exception("rmdir: invalid folder name [%s]" % path)

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
    def bucket_size(self, bucket):
        count = 0
        size = 0
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket):
            if 'Contents' in page:
                count += page['KeyCount']
                for item in page.get('Contents'):
                    size += item['Size']
        return count, size

#------------------------------------------------------------
    def bucket_owner(self, bucket):
        try:
            reply = self.s3.get_bucket_acl(Bucket=bucket)
            owner = reply['Owner']['DisplayName']
        except Exception as e:
            self.logging.error(str(e))
            owner = 'unknown'
        return owner

#------------------------------------------------------------
    def whoami(self):
        return(["access=%s" % self.access])

#------------------------------------------------------------
    def info_iter(self, pattern):
        bucket,prefix,key = self.path_convert(pattern)

# bucket and/or prefix request
        if key == "":
            if prefix == "":
                if bucket is not None:
# specific bucket
                    owner = self.bucket_owner(bucket)
                    count, size = self.bucket_size(bucket)
                    yield "%20s : %s" % ('bucket', bucket)
                    yield "%20s : %s" % ('owner', owner)
                    yield "%20s : %s" % ('objects', count)
                    yield "%20s : %s" % ('size', self.human_size(size))
                else:
# nothing specified - project summary
                    response = self.s3.list_buckets()
                    total_buckets = 0
                    total_count = 0
                    total_size = 0
                    for item in response['Buckets']:
                        bucket = item['Name']
                        count, size = self.bucket_size(bucket)
                        total_buckets += 1
                        total_count += count
                        total_size += size
                    yield "%20s : %s" % ('type', 'project')
                    yield "%20s : %s" % ('buckets', total_buckets)
                    yield "%20s : %s" % ('objects', total_count)
                    yield "%20s : %s" % ('size', self.human_size(total_size))
            else:
# summarise usage for this common prefix
# NB: this call will count ALL objects, including the "placeholder" entry for folders
# ie it will return an object count = number of files + number of intermediate sub-folders that match the input prefix
                results = self.get_iter(pattern=pattern, delimiter='')
                count = int(next(results))
                size = int(next(results))
                yield "%20s : %s" % ('prefix', pattern)
                yield "%20s : %d" % ('objects', count)
                yield "%20s : %s" % ('size', self.human_size(size))
        else:
# exact key request
            fullkey = posixpath.join(prefix, key)
            response = self.s3.head_object(Bucket=bucket, Key=fullkey)
            yield "%20s : %s" % ('object', pattern)
            for item in response['ResponseMetadata']['HTTPHeaders']:
                yield "%20s : %s" % (item, response['ResponseMetadata']['HTTPHeaders'][item])

