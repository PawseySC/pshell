#!/usr/bin/python

"""
This module is a Python 2.7.x (standard lib only) implementation of a mediaflux client
Author: Sean Fleming
"""

import os
import re
import sys
import ssl
import time
import zlib
import shlex
import random
import string
import socket
import signal
import urllib2
import httplib
import datetime
import functools
import mimetypes
import posixpath
import multiprocessing
import xml.etree.ElementTree as ET

# Globals - multiprocess IO monitoring is hard
manage_lock = multiprocessing.Lock()
bytes_sent = multiprocessing.Value('d', 0, lock=True)
bytes_recv = multiprocessing.Value('d', 0, lock=True)

#------------------------------------------------------------
def put_jump(mfclient, data):
    """
    Global multiprocessing function for concurrent uploading

    Args:
        data: ARRAY of 2 STRINGS which are the arguments for the put() method: (remote namespace, local filepath)

    Returns:
        triplet of STRINGS (asset_ID/status, 2 input arguments) which will be concatenated on the mf_manager's summary list
    """

    try:
        mfclient.log("DEBUG", "[pid=%d] put_jump(%s,%s)" % (os.getpid(), data[0], data[1]))
        asset_id = mfclient.put(data[0], data[1])
    except Exception as e:
        mfclient.log("ERROR", "[pid=%d] put_jump(%s): %s" % (os.getpid(), data[1], str(e)))
        return (-1, data[0], data[1])

    return (int(asset_id), data[0], data[1])

#------------------------------------------------------------
def get_jump(mfclient, data):
    """
    Global (multiprocessing) function for concurrent downloading

    Args:
        data: ARRAY of 2 STRINGS which are the arguments for the get() method: (asset_ID, local filepath)

    Returns:
        A triplet of STRINGS (status, 2 input arguments) which will be concatenated on the mf_manager's summary list
    """

    try:
        mfclient.log("DEBUG", "[pid=%d] get_jump(%s,%s)" % (os.getpid(), data[0], data[1]))
        mfclient.get(data[0], data[1])
    except Exception as e:
        mfclient.log("ERROR", "[pid=%d] get_jump(): %s" % (os.getpid(), str(e)))
        return (-1, data[0], data[1])

    return (0, data[0], data[1])

#------------------------------------------------------------
def init_jump(recv, sent):
    """
    Global (multiprocessing) initialiser for byte transfer counts
    """
    global bytes_sent
    global bytes_recv
# initialize the globals in this process with the main process globals O.O
    bytes_sent = sent
    bytes_recv = recv


#########################################################
class mf_client:
    """
    Base Mediaflux authentication and communication client
    Parallel transfers are handled by multiprocessing (urllib2 and httplib are not thread-safe)
    All unexpected failures are handled by raising exceptions
    """

    def __init__(self, protocol, port, server, domain="system", session="", timeout=120, enforce_encrypted_login=True, debug=0, dummy=False):
        """
        Create a Mediaflux server connection instance. Raises an exception on failure.

        Args:
                           protocol: a STRING which should be either "http" or "https"
                               port: a STRING which is usually "80" or "443"
                             server: a STRING giving the FQDN of the server
                             domain: a STRING giving the authentication domain to use when authenticating
                            session: a STRING supplying the session ID which, if it exists, enables re-use of an existing authenticated session
                            timeout: an INTEGER specifying the connection timeout
            enforce_encrypted_login: a BOOLEAN that should only be False on a safe internal dev/test network
                              debug: an INTEGER which controls output of troubleshooting information
                              dummy: a BOOLEAN used for testing only (no actual server connection)

        Returns:
            A reachable mediaflux server object that has not been tested for its authentication status

        Raises:
            Error if server appears to be unreachable
        """
# configure interfaces
        self.protocol = protocol
        self.port = int(port)
        self.server = server
        self.domain = domain
        self.timeout = timeout
        self.session = session
        self.dummy = dummy
        self.debug = debug
        self.encrypted_post = bool(enforce_encrypted_login)
# service call URL
        self.post_url = "%s://%s/__mflux_svc__" % (protocol, server)
# download/upload buffers
        self.get_buffer = 8192
        self.put_buffer = 8192
# XML pretty print hack
        self.indent = 0
        if dummy:
            return

# initial connection check 
        s = socket.socket()
        s.settimeout(7)
        s.connect((self.server, self.port))
        s.close()

# prefer (faster) unecrypted data transfer, if available
        if self.protocol == 'https':
            self.encrypted_data = True
# FIXME - I think some networks (eg UWA) will allow an 80 connection - but you can't actually use it
# FIXME - in this case, have to try and extract a valid response from the server on 80
            try:
                s = socket.socket()
                s.settimeout(2)
                s.connect((self.server, 80))
                s.close()
                self.encrypted_data = False
            except Exception as e:
                pass
        else:
            self.encrypted_data = False

# build data URLs
        if self.encrypted_data:
            self.data_get = "https://%s/mflux/content.mfjp" % server
            self.data_put = "%s:%s" % (server, 443)
        else:
            self.data_get = "http://%s/mflux/content.mfjp" % server
            self.data_put = "%s:%s" % (server, 80)

# if required, attempt to display more connection info
        if self.debug > 0:
            print "POST-URL: %s" % self.post_url
            print "DATA-GET: %s" % self.data_get
            print "DATA-PUT: %s" % self.data_put
            if self.protocol == "https":
# first line of python version info is all we're interested in
                version = sys.version
                i = version.find("\n")
                print "  PYTHON: %s" % version[:i]
                print " OPENSSL:", ssl.OPENSSL_VERSION
# early versions of python 2.7.x are missing the SSL context method
                try:
                    context = ssl.create_default_context()
                    context.verify_mode = ssl.CERT_REQUIRED
                    context.check_hostname = True
                    c = context.wrap_socket(socket.socket(socket.AF_INET), server_hostname=self.server)
                    c.connect((self.server, self.port))
                    print "  CIPHER:", c.cipher()
                except Exception as e:
                    print " WARNING: %s" % str(e)

#------------------------------------------------------------
    @staticmethod
    def _xml_succint_error(xml):
        """
        Primitive for extracting sensible error messages from Java stack traces
        """
#        print " === error start\n%s\n === error stop" % xml
# look for patterns..
        match = re.search(r"Syntax error.*Error", xml, re.DOTALL)
        if match:
            message = match.group(0)[:-6]
            return message

        match = re.search(r"failed:.*", xml)
        if match:
            message = match.group(0)[7:]
            return message

        return xml[:600]

#------------------------------------------------------------
    def _post(self, xml_string):
        """
        Primitive for sending an XML message to the Mediaflux server
        """

# dummy mode passback for pshell offline tests
        if self.dummy:
            raise Exception(xml_string)

# NB: timeout exception if server is unreachable
        request = urllib2.Request(self.post_url, data=xml_string, headers={'Content-Type': 'text/xml'})
        response = urllib2.urlopen(request, timeout=self.timeout)
        xml = response.read()
        tree = ET.fromstring(xml)

# if error - attempt to extract a useful message
        elem = tree.find(".//reply/error")
        if elem is not None:
            elem = tree.find(".//message")
            error_message = self._xml_succint_error(elem.text)
            self.log("DEBUG", "_post() raise: [%s]" % error_message)
            raise Exception(error_message)

        return tree

#------------------------------------------------------------
    def _post_multipart_buffered(self, xml, filepath, upload_timeout=600):
        """
        Primitive for doing buffered upload on a single file. Used by the put() method
        Sends a multipart POST to the server; consisting of the initial XML, followed by a streamed, buffered read of the file contents
        """
        global bytes_sent

# mediaflux seems to have random periods of unresponsiveness - particularly around final ACK of transfer
        retry_count = 9
# setup
        pid = os.getpid()
        boundary = ''.join(random.choice(string.digits + string.ascii_letters) for i in range(30))
        filename = os.path.basename(filepath)

# if we get anything other than a single clear mimetype to use - default to generic
        mimetype = mimetypes.guess_type(filepath, strict=True)
        if len(mimetype) != 1:
            mimetype = 'application/octet-stream'

# multipart - request xml and file
        lines = []
        lines.extend(('--%s' % boundary, 'Content-Disposition: form-data; name="request"', '', str(xml),))
# specifying nb-data-attachments is the key for getting the data direct to the store
        lines.extend(('--%s' % boundary, 'Content-Disposition: form-data; name="nb-data-attachments"', '', "1",))
# file
        lines.extend(('--%s' % boundary, 'Content-Disposition: form-data; name="filename"; filename="%s"' % filename, 'Content-Type: %s' % mimetype, '', ''))
        body = '\r\n'.join(lines)
# NB - should include everything AFTER the first /r/n after the headers
        total_size = len(body) + os.path.getsize(filepath) + len(boundary) + 8
        infile = open(filepath, 'rb')

# DEBUG
#        print "body size = %r" % len(body)
#        print "file size = %r" % os.path.getsize(filepath)
#        print "term size = %r" % (len(boundary) + 6)
#        print "================="
#        print "Total size = %r" % total_size
#        print "================="

# different connection object for HTTPS vs HTTP
        if self.encrypted_data is True:
            conn = httplib.HTTPSConnection(self.data_put, timeout=upload_timeout)
        else:
            conn = httplib.HTTPConnection(self.data_put, timeout=upload_timeout)

# kickoff
        self.log("DEBUG", "[pid=%d] File send starting: %s" % (pid, filepath))
        conn.putrequest('POST', "/__mflux_svc__")
# headers
        conn.putheader('Connection', 'keep-alive')
        conn.putheader('Cache-Control', 'no-cache')
        conn.putheader('Content-Length', str(total_size))
        conn.putheader('Content-Type', 'multipart/form-data; boundary=%s' % boundary)
        conn.putheader('Content-Transfer-Encoding', 'binary')
        conn.endheaders()

# data start
        conn.send(body)

# data stream of file contents
        try:
            can_recover = True
            while can_recover:
                chunk = infile.read(self.put_buffer)
                if not chunk:
                    break
# retry...
                i = 0
                while True:
                    try:
                        conn.send(chunk)
                        break

                    except Exception as e:
                        i = i+1
                        if i < retry_count:
                            self.log("DEBUG", "[pid=%d] Chunk send error [count=%d]: %s" % (pid, i, str(e)))
                            can_recover = False
                            break
                        else:
                            self.log("ERROR", "[pid=%d] Chunk retry limit reached [count=%d], giving up: %s" % (pid, i, str(e)))
# multiprocessing-safe byte counter
                with bytes_sent.get_lock():
                    bytes_sent.value += len(chunk)
        except Exception as e:
            self.log("ERROR", "[pid=%d] Fatal send error: %s" % (pid, str(e)))
            raise

        finally:
            self.log("DEBUG", "[pid=%d] Closing file: %s" % (pid, filepath))
            infile.close()

# terminating line (len(boundary) + 8)
        chunk = "\r\n--%s--\r\n" % boundary
        conn.send(chunk)

        self.log("DEBUG", "[pid=%d] File send completed, waiting for server..." % pid)

# NB - past source of problems, seems better these days ...
        message = "response did not contain an asset ID."
        for i in range(0, retry_count):
            try:
                resp = conn.getresponse()
                reply = resp.read()
                conn.close()
                tree = ET.fromstring(reply)

# return asset id of uploaded filed or any (error) message
                for elem in tree.iter():
                    if elem.tag == 'id':
                        return int(elem.text)
                    if elem.tag == 'message':
                        message = elem.text
                raise Exception(message)

# re-try if we have a slow server (final ack timeout) after a delay
            except socket.timeout:
                delay = upload_timeout / (retry_count+1)
                self.log("DEBUG", "[pid=%d] No response from server [count=%d] trying again in [%d seconds] ..." % (pid, i, delay))
                time.sleep(delay)

        raise Exception("[pid=%d] Giving up on final server ACK." % pid)

#------------------------------------------------------------
    @staticmethod
    def _xml_sanitise(text):
        """
        Helper method to sanitise text for the server XML parsing routines
        """
        if isinstance(text, str):
            text = text.replace('&', "&amp;")
            text = text.replace('<', "&lt;")
            text = text.replace('>', "&gt;")
            text = text.replace('"', "&quot;")
        return text

#------------------------------------------------------------
    def aterm_run(self, aterm_line, post=True):
        """
        Method for parsing aterm's compressed XML syntax and sending to the Mediaflux server

        Args:
             aterm_line: raw input text that is assumed to be in aterm syntax
               post: if False will just return the argument part of the serialized XML, if True will post and return reply

        Returns:
            A STRING containing the server reply (if post is TRUE, if false - just the XML for test comparisons)
        """

# NB - use posix=True as it's closest to the way aterm processes input strings
        lexer = shlex.shlex(aterm_line, posix=True)
        lexer.whitespace_split = True

        xml_root = ET.Element(None)
        xml_node = xml_root
        child = None
        stack = []

# first token is the service call, the rest are child arguments
        service_call = lexer.get_token()
        token = lexer.get_token()

        if service_call == "system.logon":
            self.log("DEBUG", "aterm_run() input: system.login ...", level=2)
        else:
            self.log("DEBUG", "aterm_run() input: %s" % aterm_line, level=2)

        try:
            while token:
                if token[0] == ':':
                    child = ET.SubElement(xml_node, '%s' % token[1:])
                    self.log("DEBUG", "XML elem [%s]" % token[1:], level=2)
# if element contains : (eg csiro:seismic) then we need to inject the xmlns stuff
                    if ":" in token[1:]:
                        item_list = token[1:].split(":")
                        self.log("DEBUG", "XML associate namespace [%s] with element [%s]" % (item_list[0], token[1:]), level=2)
                        child.set("xmlns:%s" % item_list[0], item_list[0])
                elif token[0] == '<':
                    stack.append(xml_node)
                    xml_node = child
                elif token[0] == '>':
                    xml_node = stack.pop()
                elif token[0] == '-':
                    try:
# -number => it's a text value
                        number = float(token)
                        child.text = token
                        self.log("DEBUG", "XML text [%s]" % child.text, level=2)
                    except:
# -other => it's an XML attribute/property
                        key = token[1:]
                        value = lexer.get_token()
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        child.set(key, value)
                        self.log("DEBUG", "XML prop [%r = %r]" % (key, value), level=2)
                else:
# FIXME - potentially some issues here with data strings with multiple spaces (ie we are doing a whitespace split & only adding one back)
                    if child.text is not None:
                        child.text += " " + token
                    else:
                        if token.startswith('"') and token.endswith('"'):
                            child.text = token[1:-1]
                        else:
                            child.text = token
# don't display sensitive info
                    if child.tag.lower() == "password" or child.tag.lower() == 'token':
                        self.log("DEBUG", "XML text [xxxxxxxx]", level=2)
                    else:
                        self.log("DEBUG", "XML text [%s]" % child.text, level=2)
# don't treat quotes as special characters in password string
                if "password" in token:
                    save_lexer_quotes = lexer.quotes
                    lexer.quotes = iter('') 
                    token = lexer.get_token()
                    lexer.quotes = save_lexer_quotes
                else:
                    token = lexer.get_token()

        except Exception as e:
            self.log("DEBUG", "aterm_run() error: %s" % str(e))
            raise SyntaxError

# testing hook
        if post is not True:
            tmp = ET.tostring(xml_root, method = 'xml')
# password hiding for system.logon ...
            if service_call != "system.logon":
                self.log("DEBUG", "XML out: %s" % tmp, level=2)
            return tmp

# wrap with session/service call
        xml = ET.Element("request")
        child = ET.SubElement(xml, "service")
        child.set("name", service_call)
        if service_call != "system.logon":
            child.set("session", self.session)
        args = ET.SubElement(child, "args")
        args.append(xml_root)
        xml_text = ET.tostring(xml, method = 'xml')
# debug - password hiding for system.logon ...
        if service_call != "system.logon":
            tmp = re.sub(r'session=[^>]*', 'session="..."', xml_text)
            self.log("DEBUG", "XML out: %s" % tmp, level=2)
# post
        reply = self._post(xml_text)
        return reply

#------------------------------------------------------------
    def _xml_recurse(self, elem, text=""):
        """
        Helper method for traversing XML and generating formatted output
        """

        if elem.text is not None:
            text += ' '*self.indent + '%s="%s"    ' % (elem.tag, elem.text)
        else:
            text += ' '*self.indent + '%s    ' % elem.tag
        for key, value in elem.attrib.iteritems():
            text += ' -%s="%s"' % (key, value)
        text += '\n'

        self.indent += 4
        for child in elem.getchildren():
            text = self._xml_recurse(child, text)
        self.indent -= 4

        return text

#------------------------------------------------------------
    def xml_print(self, xml_tree, trim=True):
        """
        Helper method for displaying XML nicely, as much as is possible
        """
# seek for "normal" response
        elem = None
        if trim is True:
            elem = xml_tree.find(".//result")
# seek for error message
        if elem is None:
            elem = xml_tree.find(".//message")
# still nothing? give up and print the whole thing
        if elem is None:
            elem = xml_tree
        if elem is not None:
            for child in list(elem):
                print self._xml_recurse(child).strip('\n')
        else:
            print "Empty XML document"
        return

#------------------------------------------------------------
    def log(self, prefix, message, level=1):
        """
        Timestamp based message logging.
        """
        if "DEBUG" in prefix:
            if level > self.debug:
                return

        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        message = st + " >>> " + message
        print "%8s: %s" % (prefix, message)

#------------------------------------------------------------
    def logout(self):
        """
        Destroy the current session (NB: delegate can auto-create a new session if available)
        """
        self.aterm_run("system.logoff")
        self.session = ""

#------------------------------------------------------------
    def login(self, user=None, password=None, token=None):
        """
        Authenticate to the current Mediaflux server and record the session ID on success

        Input:
            user, password: STRINGS specifying user login details
            token: STRING specifying a delegate credential

        Raises:
            An error if authentication fails
        """
# security check
        if self.protocol != "https":
            if self.encrypted_post:
                raise Exception("Forbidding unencrypted password post")
            else:
                self.log("DEBUG", "Permitting unencrypted login; I hope you know what you're doing.")
# attempt token authentication first (if supplied)
        if token is not None:
            reply = self.aterm_run("system.logon :token %s" % token)
        else:
            reply = self.aterm_run("system.logon :domain %s :user %s :password %s" % (self.domain, user, password))
# extract session key
        elem = reply.find(".//session")
        if elem is not None:
            self.session = elem.text
            return

        raise Exception("Login failed")

#------------------------------------------------------------
    def authenticated(self):
        """
        Check client authentication state

        Returns:
             A BOOLEAN value depending on the current authentication status of the Mediaflux connection
        """
        if self.dummy:
            return True
        try:
            self.aterm_run("actor.self.describe")
            return True
        except Exception as e:
            self.session = ""
# NB: max licence error can occur here
            self.log("DEBUG", str(e))

        return False

#------------------------------------------------------------
    def namespace_exists(self, namespace):
        """
        Wrapper around the generic service call mechanism (for testing namespace existence) that parses the result XML and returns a BOOLEAN
        """
        reply = self.aterm_run('asset.namespace.exists :namespace "%s"' % namespace.replace('"', '\\\"'))
        elem = reply.find(".//exists")
        if elem is not None:
            if elem.text == "true":
                return True

        return False

#------------------------------------------------------------
    def get_local_checksum(self, filepath):
        current = 0
        with open(filepath, 'rb') as fd:
            while True:
                data = fd.read(self.put_buffer)
                if not data:
                    break
                current = zlib.crc32(data, current)
        fd.close()
        return current & 0xFFFFFFFF

#------------------------------------------------------------
    def get(self, asset_id, filepath, overwrite=False):
        """
        Download an asset to a local filepath

        Args:
            asset_id: an INTEGER representing the Mediaflux asset ID on the server
            filepath: a STRING representing the full path and filename to download the asset content to
            overwrite: a BOOLEAN indicating action if local copy exists

        Raises:
            An error on failure
        """
        global bytes_recv

# build download URL
        url = self.data_get + "?_skey=%s&id=%s" % (self.session, asset_id)
#        print "GET url [%s]" % url
        req = urllib2.urlopen(url)

# distinguish between file data and mediaflux error message
# DEBUG
#        info = req.info()
#        print "get info: %s" % info
#        print "encoding: " , info.getencoding()
#        print "type: " , info.gettype()

# TODO - auto overwrite if different? (CRC)
        if os.path.isfile(filepath) and not overwrite:
            self.log("DEBUG", "Local file of that name (%s) already exists, skipping." % filepath)

# FIXME - this should lower the expected total_bytes by the size of the file ...
            with bytes_recv.get_lock():
                bytes_recv.value += os.path.getsize(filepath)

            req.close()
            return

# buffered write to open file
        with open(filepath, 'wb') as output:
            while True:
                data = req.read(self.get_buffer)
                if data:
                    output.write(data)
# multiprocessing safe byte counter
# NOTE - tried decreasing frequency of locks -> had no impact on transfer speed
                    with bytes_recv.get_lock():
                        bytes_recv.value += len(data)
                else:
                    break
        output.close()

#------------------------------------------------------------
    def get_managed(self, list_asset_filepath, total_bytes, processes=4):
        """
        Managed multiprocessing download of a list of assets from the Mediaflux server. Uses get() as the file transfer primitive

        Args:
            list_asset_filepath: a LIST of pairs representing the asset ID and local filepath destination
                    total_bytes: the total bytes to download
                      processes: the number of processes the multiprocessing manager should use

        Returns:
            A queryable mf_manager object
        """

# shenanigans to enable mfclient method to be called from the global process pool (python can't serialize instance methods)
        get_alias = functools.partial(get_jump, self)

        return mf_manager(function=get_alias, arguments=list_asset_filepath, processes=processes, total_bytes=total_bytes)

#------------------------------------------------------------
    def put(self, namespace, filepath, overwrite=True):
        """
        Creates a new asset on the Mediaflux server and uploads from a local filepath to supply its content

        Args:
            namespace: a STRING representing the remote destination in which to create the asset
             filepath: a STRING giving the absolute path and name of the local file
            overwrite: a BOOLEAN indicating action if remote copy exists

        Returns:
            asset_id: an INTEGER representing the mediaflux asset ID

        Raises:
            An error message if unsuccessful
        """
        global bytes_sent

# construct destination argument
        filename = os.path.basename(filepath)
        filename = self._xml_sanitise(filename)
        namespace = self._xml_sanitise(namespace)
        remotepath = posixpath.join(namespace, filename)
        asset_id = -1
# query the remote server for file details (if any)
        try:
            result = self.aterm_run('asset.get :id "path=%s" :xpath -ename id id :xpath -ename crc32 content/csum :xpath -ename size content/size' % remotepath)
        except Exception as e:
            self.log("DEBUG", "Not found - creating: [%s]" % remotepath)
            xml_string = '<request><service name="service.execute" session="%s" seq="0"><args><service name="asset.set">' % self.session
            xml_string += '<id>path=%s</id><create>true</create></service></args></service></request>' % remotepath
            asset_id = self._post_multipart_buffered(xml_string, filepath)
            return asset_id

# attempt checksum compare
        try:
            elem = result.find(".//id")
            asset_id = int(elem.text)
            elem = result.find(".//crc32")
            remote_crc32 = int(elem.text, 16)
            elem = result.find(".//size")
            remote_size = int(elem.text)
            local_crc32 = self.get_local_checksum(filepath)
            if local_crc32 == remote_crc32:
# if local and remote are identical -> update progress and exit
                self.log("DEBUG", "Checksum match, skipping [%s] -> [%s]" % (filepath, remotepath))
                with bytes_sent.get_lock():
                    bytes_sent.value += remote_size
                return asset_id
        except Exception as e:
            self.log("ERROR", "Failed to compute checksum: %s" % str(e))

# local and remote crc32 don't match -> decision time ...
        if overwrite is True:
            self.log("DEBUG", "Overwriting: [%s]" % remotepath)
            xml_string = '<request><service name="service.execute" session="%s" seq="0"><args><service name="asset.set">' % self.session
            xml_string += '<id>path=%s</id><create>true</create></service></args></service></request>' % remotepath
            asset_id = self._post_multipart_buffered(xml_string, filepath)

        return asset_id

#------------------------------------------------------------
    def put_managed(self, list_namespace_filepath, total_bytes=None, processes=4):
        """
        Managed multiprocessing upload of a list of files to the Mediaflux server. Uses put() as the file transfer primitive

        Args:
            list_namespace_filepath: a LIST of STRING pairs representing the remote namespace destination and the local filepath source
                        total_bytes: the total bytes to upload
                          processes: the number of processes the multiprocessing manager should use

        Returns:
            A queryable mf_manager object
        """

# CURRENT - require total_bytes to be pre-computed
# if not supplied - count (potentially a lot slower)
        if total_bytes is None:
            total_bytes = 0
            self.log("DEBUG", "Total upload bytes not supplied, counting...")
            for namespace, filepath in list_namespace_filepath:
                try:
                    total_bytes += os.path.getsize(filepath)
                except:
# FIXME - this should lower the expected total_bytes by the size of the file ...
                    self.log("DEBUG", "Can't read %s, skipping." % filepath)

        self.log("DEBUG", "Total upload bytes: %d" % total_bytes)
        if total_bytes == 0:
            print
            raise Exception("No data to upload")

# shenanigans to enable mfclient method to be called from the global process pool (python can't serialize instance methods)
        put_alias = functools.partial(put_jump, self)

        return mf_manager(function=put_alias, arguments=list_namespace_filepath, processes=processes, total_bytes=total_bytes)


#############################################################
class mf_manager:
    """
    Multiprocessing file transfer management object.
    """

# a list which is appended to as individual transfers are completed
    summary = None
    task = None
    pool = None

# TODO - will probably need an offline list added to signature ...
    def __init__(self, function, arguments, processes=1, total_bytes=0):
        """
        Args:
               function: the primitive transfer METHOD put() or get() to invoke in transferring a single file
              arguments: a LIST of STRING pairs to be supplied to the transfer function primitive
              processes: INTEGER number of processes to spawn to deal with the input list
            total_bytes: INTEGER size of the transfer, for progress reporting

        Returns:
            manager object which can be queried for progress (see methods below) and final status
        """
        global bytes_sent
        global bytes_recv

# fail if there is already a managed transfer (there can only be one!)
# FIXME - not really an informative exception ...
        if not manage_lock.acquire(block=False):
            raise TypeError

# init monitoring
        self.start_time = time.time()
        self.bytes_total = total_bytes
        self.summary = []
        bytes_sent.value = 0
        bytes_recv.value = 0

# ref:http://stackoverflow.com/questions/11312525/catch-ctrlc-sigint-and-exit-multiprocesses-gracefully-in-python
# force control-C to be ignored by process pool
        handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
# NB: urllib2 and httplib are not thread safe -> use process pool instead of threads
# shared mem globals aren't preserved with a Windows fork() - need an explicit init
        self.pool = multiprocessing.Pool(processes, init_jump, (bytes_recv, bytes_sent))

# restore control-C
        signal.signal(signal.SIGINT, handler)
        self.task = self.pool.map_async(function, arguments, callback=self.summary.extend)
        self.pool.close()

# cleanup - normal or interrupted
    def cleanup(self):
        """
        Invoke to properly terminate the process pool (eg if user cancels via control-C)
        """
        self.pool.terminate()
        self.pool.join()
        manage_lock.release()

    def byte_sent_rate(self):
        """
        Returns the upload transfer rate
        """
        elapsed = time.time() - self.start_time
        try:
            rate = bytes_sent.value / elapsed
            rate /= 1000000.0
        except:
            rate = 0.0
        return rate

    def byte_recv_rate(self):
        """
        Returns the download transfer rate
        """
        elapsed = time.time() - self.start_time
        try:
            rate = bytes_recv.value / elapsed
            rate /= 1000000.0
        except:
            rate = 0.0
        return rate

    @staticmethod
    def bytes_sent():
        """
        Returns the total bytes sent for the current process
        """
        return bytes_sent.value

    @staticmethod
    def bytes_recv():
        """
        Returns the total bytes recieved for the current process
        """
        return bytes_recv.value

    def is_done(self):
        """
        BOOLEAN test for transfer completion
        """
        return self.task.ready()
