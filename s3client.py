#!/usr/bin/python

"""
This module is a Python 3.x implementation of a simple s3 client
Author: Sean Fleming
"""

import os
import re
import json
import math
import string
import urllib
import fnmatch
import getpass
import logging
import pathlib
import datetime
# deprec in favour of pathlib?
import posixpath

try:
    import boto3
    import botocore
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    ok=True
except:
    ok=False

#------------------------------------------------------------
# technically, s3_bucket_policy
class s3_policy():
    def __init__(self, bucket, s3_client=None):
        self.bucket = bucket
        self.hash = {}
# init IAM owner
        try:
            reply = s3_client.get_bucket_acl(Bucket=bucket)
            owner = reply['Owner']['ID']
            if len(owner) > 20 and all(c in string.hexdigits for c in owner):
                self.iam_owner = 'arn:aws:iam::%s:root' % owner
            else:
                self.iam_owner = 'arn:aws:iam:::user/%s' % owner
        except:
            self.iam_owner = 'unknown'
            pass
# init policy to existing bucket policy (if any)
        try:
            reply = s3_client.get_bucket_policy(Bucket=bucket)
            self.hash = json.loads(reply['Policy'])
        except:
            self.hash['Id'] = 'pshell-%s' % datetime.datetime.now().strftime('%Y-%m-%d')
            self.hash['Statement'] = []
            pass

# --- append statement 
    def statement_add(self, statement):
        self.hash['Statement'].append(statement)

# --- construct new statement 
    def statement_new(self, resources=None, perm=None, users=None, projects=None):
        statement = {}
# generate unique sid via timestamp
# NB: AWS says Sid has to be unique within a policy, but Ceph seems to allow same Sid
# NB: colons are special characters - avoid including a time value with colons
        sid = datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')
        statement['Sid'] = sid
# permissions
        if perm is not None:
            if '-' in perm:
                statement['Effect'] = 'Deny'
            elif '+' in perm:
                statement['Effect'] = 'Allow'
            else:
                raise Exception("Unknown permission string=%s" % perm)
# NB: adding s3:* or s3:ListAllMyBuckets currently does NOT allow the user to list buckets ... although they can see the objects in the bucket
            if 'r' in perm:
                statement['Action'] = ["s3:ListBucket", "s3:GetObject"]
            if 'w' in perm:
                statement['Action'] = ["s3:PutObject", "s3:DeleteObject"]
# TODO - maybe - add options for more nuanced permissions? (eg allow PutObject, but not DeleteObject)
# users
        if users is not None:
            list_users = users.split(',')
            principal = [ 'arn:aws:iam:::user/%s' % user.strip() for user in list_users]

#NEW - not required ... as owners always have the option to remove policies
# ensure owner has access
#            principal.append(self.iam_owner)
            statement['Principal'] = {'AWS': principal}
# projects - TODO
# resources
        if resources is not None:
            statement['Resource'] = resources

        return statement

# deprecated ...
# --- construct and append a statement 
    def statement_append(self, perm, users):
        statement = self.statement_new(perm=perm, users=users)
        self.statement_add(statement)
        return

# --- return in suitable form for setting
    def get_json(self, indent=0):
        return(json.dumps(self.hash, indent=indent))


#------------------------------------------------------------
class s3_client():
    def __init__(self, url=None, access=None, secret=None, log_level=None):
        self.ok = ok
        self.type = "s3"
        self.url = url
        self.access = access
        self.secret = secret
        self.s3 = None
        self.status = "not connected"
        self.enable_polling = True
        self.logging = logging.getLogger('s3client')
# NEW - test invoke - standalone
        if log_level is not None:
            logging.basicConfig(format='%(levelname)9s %(asctime)-15s >>> %(module)s.%(funcName)s(): %(message)s', level=log_level)
        self.logging.debug("BOTO3=%r" % ok)

# --- NEW - main (pshell) invoke
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
            self.logging.info('success')
            return True

        except Exception as e:
            emsg = str(e)
            self.logging.error(emsg)
            if "InvalidAccessKeyId" in emsg:
                emsg = "access=%s and secret were invalid" % self.access
            if "Unable to locate credentials" in emsg:
                emsg = "no access/secret" 
            self.status = "not connected to %s: %s" % (self.url, emsg)
# failed to establish a verified connection
        return False

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
# given a candidate determine if it matches the cwd/partial and return the [start] based section of the match if so, else none
# TODO - can we adapt this to match objects and buckets in complete_path() -> probably have to use cwd somehow...
    def completion_match(self, cwd, partial, start, candidate):
        self.logging.debug("cwd=%s, partial=%s, start=%d, candidate=%s" % (cwd, partial, start, candidate))

# no partial pattern, just return the candidate
        plen=len(partial)
        if plen == 0:
            return candidate

# greedy search for an intersecting match of candidate with partial
        clen=len(candidate)
        for i in range(0, plen):
            self.logging.debug("compare [%s] <==> [%s]" % (candidate, partial[i:]))
            if candidate.startswith(partial[i:]):
                greedy_match=i
                match = partial[:i] + candidate
                self.logging.debug("greedy match=%s, i=%d" % (match, greedy_match))
# if match occurred part way through the string, previous char should be a / for a valid (complete) match
                if i>0:
                    if partial[:i].endswith('/') is False:
                        continue
                return match[start:]

# non-greedy concatenation return if partial is itself a complete prefix
        if partial.endswith('/'):
            return partial[start:] + candidate

# no valid match
        return None

#------------------------------------------------------------
    def complete_path(self, cwd, partial, start, match_prefix=True, match_object=True):
        # return list of completed candidates that substitute at partial[start:] 
        self.logging.debug("cwd=[%s] partial=[%s] start=[%d]" % (cwd, partial, start))
        try:
            fullpath = posixpath.join(cwd, partial)
            bucket, prefix, pattern = self.path_convert(fullpath)
            self.logging.debug("fullpath=%s, bucket=%s, prefix=%s, pattern=%s" % (fullpath, bucket, prefix, pattern))
            candidate_list = []
            if self.bucket_exists(bucket) is False:
# get results for bucket search
                self.logging.debug("bucket search: partial bucket=[%s]" % bucket)
                response = self.s3.list_buckets()
                for item in response['Buckets']:
                    if bucket is not None:
                        lb = len(bucket)
                        if item['Name'].startswith(bucket) is False:
                            continue
                    else:
                        lb = 0
                    bname = item['Name']+'/'
                    candidate = partial + bname[lb:]
                    candidate_list.append(candidate[start:])
            else:
# get results for non-bucket searches
                response = self.s3.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix) 
                prefix_len = len(prefix)
# process folder (prefix) matches
                if match_prefix is True:
                    if 'CommonPrefixes' in response:
                        for item in response['CommonPrefixes']:
# NEW - enable test driven approach to this whole mess
                            candidate = self.completion_match(cwd, partial, start, item['Prefix'])
                            if candidate is not None:
                                candidate_list.append(candidate)
# process file (object) matches
                if match_object is True:
                    if 'Contents' in response:
                        for item in response['Contents']:
                            # main search criteria
                            full_candidate = "/%s/%s" % (bucket, item['Key'])
                            self.logging.debug("key=%s, full=%s" % (item['Key'], full_candidate))
                            if full_candidate.startswith(fullpath):
                                match_ix = full_candidate.rfind(partial)
                                self.logging.debug("MATCH index=%d, full=%s" % (match_ix, full_candidate))
                                if match_ix >= 0:
                                    candidate = full_candidate[match_ix:]
                                    candidate_list.append(candidate[start:])

        except Exception as e:
            self.logging.error(str(e))

# done
        self.logging.debug(candidate_list)
        return candidate_list

#------------------------------------------------------------
    def complete_folder(self, cwd, partial, start):
        return self.complete_path(cwd, partial, start, match_prefix=True, match_object=False)

#------------------------------------------------------------
    def complete_file(self, cwd, partial, start):
        return self.complete_path(cwd, partial, start, match_prefix=True, match_object=True)

#------------------------------------------------------------
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
#            bucket = mypath.parts[1]
            bucket = mypath.parts[1].strip()
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
    def cd(self, path):
# all paths must end in /
        fullpath = posixpath.normpath(path)
        if fullpath.endswith('/') is False:
            fullpath += '/'
        self.logging.debug("input fullpath=[%s]" % fullpath)
        bucket,prefix,key = self.path_convert(fullpath)
        self.logging.debug("bucket=[%s] prefix=[%s]" % (bucket, prefix))
# check for existence
        if bucket is None:
            # root level
            return fullpath
        else:
            try:
# if a list_objects generates no exception - return as valid path
                response = self.s3.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix) 
                return fullpath

            except Exception as e:
                self.logging.debug(str(e))

        raise Exception("Could not find remote path: [%s]" % fullpath)

#------------------------------------------------------------
# implementation using list_objects_v2
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
        self.logging.debug("bucket=[%s], prefix=[%s], key=[%s]" % (bucket, prefix, key))

# match everything and recurse if no key supplied (ie get on a folder)
        if len(key) == 0:
            # match all keys
            key = '*'
            # recursively
            delimiter = ""
# build full filename (prefix+key) matching string
        key_pattern = posixpath.join(prefix, key)
        self.logging.debug("key_pattern=[%s], delimiter=[%s]" % (key_pattern, delimiter))

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

# === WORKING EXAMPLE
    def smart_open_get(self, remote_filepath, local_filepath=None, cb_progress=None):
        import smart_open
        print("get() -> smart_open() -> OPEN: %s" % remote_filepath)
# NB: 1st argument needs to strictly conform to s3://bucket/key
# eg if remote filepath is relative (ie no leading slash) this will likely fail...
        fin = smart_open.open('s3:/%s' % remote_filepath, transport_params=dict(client=self.s3))
        print("get() -> smart_open() -> READ: %r" % fin)
        for line in fin:
            print(line)
        return(0)

#------------------------------------------------------------
    def get(self, remote_filepath, local_filepath=None, cb_progress=None):

        bucket,prefix,key = self.path_convert(remote_filepath)
        fullkey = posixpath.join(prefix, key)
        self.logging.debug('remote bucket=[%r] fullkey=[%r] : local_filepath=[%r]' % (bucket, fullkey, local_filepath))

        if local_filepath is None:
            local_filepath = os.path.normpath(os.path.join(os.getcwd(), posixpath.basename(fullkey)))
        self.logging.debug('Downloading to [%s]' % local_filepath)

# make any intermediate folders required ...
        local_parent = os.path.dirname(local_filepath)
        if os.path.exists(local_parent) is False:
            self.logging.info("Creating required local folder(s): [%s]" % local_parent)
            os.makedirs(local_parent)

# can tweak this, default concurrency is 10
#        from boto3.s3.transfer import TransferConfig
#        config = TransferConfig(max_concurrency=5)
#        self.s3.download_file(str(bucket), str(fullkey), local_filepath, Config=config)

        self.s3.download_file(str(bucket), str(fullkey), local_filepath, Callback=cb_progress)

        return(0)

#------------------------------------------------------------
    def put(self, remote_path, local_filepath, cb_progress=None, metadata=False):
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
                return(-1)
        except Exception as e:
            # file doesn't exist (or couldn't get size)
            self.logging.debug(str(e))

        self.s3.upload_file(local_filepath, bucket, fullkey, Callback=cb_progress)
        return(0)

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
        bucket,prefix,key = self.path_convert(pattern)

        if len(key) == 0:
# generate policy for bucket and all objects in it
            p = s3_policy(bucket, self.s3)
            statement = p.statement_new(perm="+r")
            statement['Sid'] = "pshell-public-" + statement['Sid']
            statement['Principal'] = "*"
            statement['Resource'] = [ 'arn:aws:s3:::%s' % bucket, 'arn:aws:s3:::%s/*' % bucket ]
            p.statement_add(statement)
            self.s3.put_bucket_policy(Bucket=bucket, Policy=p.get_json())
            count = 1
        else:
#            raise Exception("publish: only supported for buckets.")
            results = self.get_iter(pattern)
            count = int(next(results))
            size = int(next(results))
# FIXME - a sensible value for this... ?
            if count > 100:
                raise Exception("Error: too many files, please restructure your data and publish the bucket.")
            print("Publishing %d files..." % count)
            for filepath in results:
                self.logging.debug("s3 publish: %s" % filepath)
                bucket,prefix,key = self.path_convert(filepath)
                fullkey = posixpath.join(prefix, key)
# try different expiry times ... no limit? ... or 7 days max?
#                url = self.s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=3600)
                url = self.s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': bucket, 'Key': fullkey}, ExpiresIn=3600000)
                print("short-lived public url = %s" % url)

        return(count)

#------------------------------------------------------------
    def unpublish(self, pattern):
        bucket,prefix,key = self.path_convert(pattern)

        if len(key) == 0:
# get the current policy
            payload = self.s3.get_bucket_policy(Bucket=bucket)
            policy = json.loads(payload['Policy'])
# strip any public- statements
            new_statement = [s for s in policy['Statement'] if s['Sid'].startswith("pshell-public-") is False]
# push the new policy or remove entirely if no statements
            if len(new_statement) == 0:
                self.s3.delete_bucket_policy(Bucket=bucket)
            else:
                policy['Statement'] = new_statement
                self.s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
        else:
            raise Exception("unpublish: only supported for buckets.")

        return(1)

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
# return the Display Name owner (for pretty printing)
    def bucket_owner(self, bucket):
        try:
            reply = self.s3.get_bucket_acl(Bucket=bucket)
            owner = reply['Owner']['DisplayName']
        except Exception as e:
            self.logging.error(str(e))
            owner = 'unknown'
        return owner

#------------------------------------------------------------
    def delegate(self, line):
        raise Exception("Not implemented") 

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
                    yield "%20s : %s" % ('bucket', bucket)
                    owner = self.bucket_owner(bucket)
                    yield "%20s : %s" % ('owner', owner)
                    count, size = self.bucket_size(bucket)
                    yield "%20s : %s" % ('objects', count)
                    yield "%20s : %s" % ('size', self.human_size(size))
# show incomplete multi-part uploads (if any)
                    try:
                        response = self.s3.list_multipart_uploads(Bucket=bucket)
                        n = len(response['Uploads'])
                        yield "%20s : %s" % ('incomplete uploads', n)
                    except Exception as e:
                        self.logging.debug(str(e))
                        yield "%20s : 0" % 'incomplete uploads'
# show versioning (if any)
# NB: default response vs enabled vs subsequent disable seems different ... for AWS/Ceph reasons I guess
# ie by default it doesn't have any metadata (enable or disable) for versioning - only after explicit versioning calls will it appear and remain
                    reply = self.s3.get_bucket_versioning(Bucket=bucket)
                    try:
                        value = json.dumps(reply['Status'])
                        yield "%20s : %s" % ('versioning', value)
                    except Exception as e:
                        yield "%20s : None" % 'versioning'
# show lifecycle (if any)
                    try:
                        yield " === Lifecycle === "
                        response = self.s3.get_bucket_lifecycle_configuration(Bucket=bucket)
                        yield json.dumps(response['Rules'], indent=4)
                    except Exception as e:
                        self.logging.debug(str(e))
                        yield "None"
# show policy (if any)
                    try:
                        yield " === Policy === "
                        response = self.s3.get_bucket_policy(Bucket=bucket)
                        hash_policy = json.loads(response['Policy'])
                        yield json.dumps(hash_policy, indent=4)
                    except Exception as e:
                        self.logging.debug(str(e))
                        yield "None"
                else:
# nothing specified - project summary
                    yield "%20s : %s" % ('type', 'project')
                    response = self.s3.list_buckets()
                    total_buckets = len(response['Buckets'])
                    yield "%20s : %s" % ('buckets', total_buckets)
                    total_count = 0
                    total_size = 0
                    for item in response['Buckets']:
                        bucket = item['Name']
                        count, size = self.bucket_size(bucket)
                        total_count += count
                        total_size += size
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

# NEW - deletion markers will generate an exception (object doesn't exist)
            try:
                response = self.s3.head_object(Bucket=bucket, Key=fullkey)
                yield "%20s : %s" % ('object', pattern)
                for item in response['ResponseMetadata']['HTTPHeaders']:
                    yield "%20s : %s" % (item, response['ResponseMetadata']['HTTPHeaders'][item])
            except Exception as e:
                self.logging.debug(str(e))

# TODO (maybe) if find 'x-amz-version-id' in the metadata -> run a list_object_versions ... display the IDs
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_object_versions.html

#------------------------------------------------------------
# ref - not sure if Ceph is the same, but, root can't lock itself from a bucket (by default) even with deny policies
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_policy.html
    def policy_bucket_set(self, text):
# process args
        args = text.split(" ", 3)
        nargs = len(args)

# TODO - this should be a bucket + (optional) object reference ...
        bucket = args[1]
        perm = args[2]

# if nothing -> implied everyone 
        try:
            users=args[3]
# special case for "everyone" 
            if users == '*':
                users = None
        except:
            users=None
            pass

# remove all policies if - and no users specified
        if perm == '-' and (users is None or users == '*'):
            print("Deleting all policies on bucket=%s" % bucket)
            self.s3.delete_bucket_policy(Bucket=bucket)
        else:
# append required policy statement to existing policy (if any) and apply
            print("Setting bucket=%s, perm=%s, for user(s)=%r" % (bucket, perm, users))
            p = s3_policy(bucket, self.s3)
# TODO - check if the root iam users really does need to be added ...
            statement = p.statement_new(resources=['arn:aws:s3:::%s' % bucket, 'arn:aws:s3:::%s/*' % bucket], perm=perm, users=users)
# CURRENT - special case when specifying everyone
            if users is None:
                statement['Principal'] = '*'

            p.statement_add(statement)

#            print(p.get_json())

            self.s3.put_bucket_policy(Bucket=bucket, Policy=p.get_json())

#------------------------------------------------------------
    def ls_deleted(self, bucket, prefix):
        """
        Show deletion markers
        """
        print("Reviewing deletions: bucket=%s, prefix=%s" % (bucket, prefix))
# support for key matching not implemented at this time
        paginator = self.s3.get_paginator('list_object_versions')
        page_list = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in page_list:
            if 'DeleteMarkers' in page:
                for item in page.get('DeleteMarkers'):
                    print(" * %s" % item['Key'])
# TODO - show sizes? ... NB: no size in delete marker itself, have to query the latest actual object version

#------------------------------------------------------------
    def restore_deleted(self, bucket, prefix, test=True):
        """
        Remove deletion markers
        """
        print("Restoring deletions: bucket=%s, prefix=%s" % (bucket, prefix))
        count = 0
        paginator = self.s3.get_paginator('list_object_versions')
        page_list = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in page_list:
            if 'DeleteMarkers' in page:
                for item in page.get('DeleteMarkers'):
                    count += 1
                    fullkey = item['Key']
                    version = item['VersionId']
                    self.logging.info("deletion marker: [%s] [%s]" % (fullkey, version))
                    print("restoring: %s" % fullkey)
                    self.s3.delete_object(Bucket=bucket, Key=fullkey, VersionId=version)
        print("Restored object count: %d" % count)

#------------------------------------------------------------
    def json_template_helper(self, hash_input):
        json_tmp1 = '{ "ID": "%s", "Status": "%s", "Filter": { "Prefix": "" }, "%s": { "%s": %d } }'
        list_rules = []
        hash_rules = {}

# cleanup multiparts
        if 'DaysAfterInitiation' in hash_input.keys():
            days = hash_input['DaysAfterInitiation'] 
            status = hash_input['Status']
            list_rules.append(json.loads(json_tmp1 % ("cleanup_multipart", status, "AbortIncompleteMultipartUpload", "DaysAfterInitiation", days)))

# cleanup versions
        if 'NoncurrentDays' in hash_input.keys():
            days = hash_input['NoncurrentDays'] 
            status = hash_input['Status']
            list_rules.append(json.loads(json_tmp1 % ("cleanup_versions", status, "NoncurrentVersionExpiration", "NoncurrentDays", days)))

        hash_rules['Rules'] = list_rules

        return hash_rules

#------------------------------------------------------------
    def bucket_lifecycle(self, text):
# NEW
# lifecycle bucket/prefix -d   ==> display deletion markers
# lifecycle bucket/prefix -u   ==> undelete all marked objects
        hash_action = {}
        hash_toggle = {}
        try:
            args = text.split(" ", 2)

# NEW - args[1] accept either bucket or fullkey, in order to act on specific objects
# NB - can't specify fullkeys in the args[2] section -> they may have '-' in the names which will mess up the arg parsing
            bucket,prefix,key = self.path_convert(args[1])
            fullkey = posixpath.join(prefix, key)
# TODO - strip this bit out and implement parsing tests
            action_list = re.findall("[+-][mv][^+-]*", args[2])
            if len(action_list) > 0:
                for action in action_list:
# find the (optional) days
                    match_days = re.search("\d+", action)
                    if match_days:
                        days = int(match_days.group(0))
                    else:
                        days = 30
# turn on/off 
                    if action.startswith('+'):
                        hash_action['Status'] = 'Enabled'
                        hash_toggle['Status'] = 'Enabled'
                    elif action.startswith('-'):
                        hash_action['Status'] = 'Disabled'
# really AWS, not 'Disabled' like everything else???
                        hash_toggle['Status'] = 'Suspended'
# versioning lifecycle 
                    if 'v' in action:
                        response = self.s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration=hash_toggle)
                        hash_action['NoncurrentDays'] = days
# multipart lifecycle
                    if 'm' in action:
                        hash_action['DaysAfterInitiation'] = days

# most common errors here will be no such bucket or no permission on bucket
                hash_payload = self.json_template_helper(hash_action)
                reply = self.s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=hash_payload)

# TODO - review/restore section
# maybe even go back to the --review .... ---restore approach ...
# alt approach
# -d -> display deletion markers
# -u -> undelete/restore
#            review_list = re.findall("[-][lu]", args[2])
            review_list = re.findall("[--]{2}[^\s]*", args[2])
            if len(review_list) > 0:
                for review in review_list:
                    if 'review' in review:
                        self.ls_deleted(bucket, fullkey)
                    if 'restore' in review:
                        self.restore_deleted(bucket, fullkey)

        except Exception as e:
            self.logging.info(str(e))
            print("Usage: lifecycle bucket (+-)(mv) <days>")

#------------------------------------------------------------
    def command(self, text):
        """
        Default passthrough method
        """

# policies 
        if text.startswith("policy"):
            self.policy_bucket_set(text)
            return
# lifecycle 
        if text.startswith("lifecycle"):
            self.bucket_lifecycle(text)
            return

        raise Exception("Bad or unsupported S3 command: %s" % text)
