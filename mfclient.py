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
        self.token = None
        self.dummy = dummy
        self.debug = int(debug)
        self.encrypted_post = bool(enforce_encrypted_login)
        self.encrypted_data = self.encrypted_post
# service call URL
        self.post_url = "%s://%s/__mflux_svc__" % (protocol, server)
# download/upload buffers
        self.get_buffer = 8192
        self.put_buffer = 8192
# XML pretty print hack
        self.indent = 0
# build data URLs
        if self.encrypted_data:
            self.data_get = "https://%s/mflux/content.mfjp" % server
            self.data_put = "%s:%s" % (server, 443)
        else:
            self.data_get = "http://%s/mflux/content.mfjp" % server
            self.data_put = "%s:%s" % (server, 80)

# test mode - don't check server connection
        if dummy:
            return

# initial connection check 
# FIXME - this does not properly raise a timeout exception in some rare cases when the server is having issues
        s = socket.socket(socket.AF_INET)
        s.settimeout(7)
        s.connect((self.server, self.port))
        s.close()

# check if we're on the internal network (ie https, but can do http as well)
        if self.protocol == 'https':
            try:
                s = socket.socket()
                s.settimeout(2)
                s.connect((self.server, 80))
                s.close()
# yes - do unencrypted data transfer (significantly faster)
                self.encrypted_data = False
                self.data_put = "%s:%s" % (server, 80)
                self.data_get = "http://%s/mflux/content.mfjp" % server
            except Exception as e:
                pass

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
        Primitive for extracting more concise error messages from Java stack traces
        """
        max_size = 600

# pattern 1 - remove context
        match = re.search(r"Syntax error.*Context", xml, re.DOTALL)
        if match:
            message = match.group(0)[:-7]
            return message[:max_size]

# pattern 2 - other
        match = re.search(r"failed:.*", xml)
        if match:
            message = match.group(0)[7:]
            return message[:max_size]

# give up
        return xml[:max_size]

#------------------------------------------------------------
    def _post(self, xml_string, output_local_filepath=None):
        """
        Primitive for sending an XML message to the Mediaflux server
        """
        global bytes_recv

# dummy mode passback for pshell offline tests
        if self.dummy:
            raise Exception(xml_string)

#        print "\nINPUT===================\n"
#        print xml_string
#        print "\n===================\n"

# NB: timeout exception if server is unreachable
        request = urllib2.Request(self.post_url, data=xml_string, headers={'Content-Type': 'text/xml'})
        response = urllib2.urlopen(request, timeout=self.timeout)
        xml = response.read()

#        print "\nOUTPUT===================\n"
#        print xml
#        print "\n===================\n"

        tree = ET.fromstring(xml)

# NEW - outputs-via = session
# HACK - MF returns multiple outputs (some are metadata I think?) lets go with the first as the requested data
        elem = tree.find(".//outputs/id")
        if elem is not None:
            output_id = elem.text

# CURRENT - download
# BUT - can't use token with outputs-via=session
# TODO - aternative is outputs-via=response ... but this returns as attachment(s) which seem to be handled differently
            url = self.data_get + "?_skey=%s&id=%s" % (self.session, output_id)
            url = url.replace("content", "output")
            filepath = output_local_filepath.replace("file:/", "")

# buffered write to open file
            response = urllib2.urlopen(url)
            with open(filepath, 'wb') as output:
                while True:
# NEW - specifically trap network IO issues
                    try:
                        data = response.read(self.get_buffer)
                    except Exception as e:
                        raise Exception("Network read error: %s" % str(e))
# exit condition
                    if not data:
                        break
# NEW - specifically trap disk IO issues
                    try:
                        output.write(data)
                    except Exception as e:
                        raise Exception("File write error: %s" % str(e))
# record progress
                    with bytes_recv.get_lock():
                        bytes_recv.value += len(data)

# if error - attempt to extract a useful message
        elem = tree.find(".//reply/error")
        if elem is not None:
            elem = tree.find(".//message")
            error_message = self._xml_succint_error(elem.text)
            self.log("DEBUG", "_post() raise: [%s]" % error_message)
            raise Exception(error_message)

        return tree

#------------------------------------------------------------
    def _post_multipart_buffered(self, xml, filepath):
        """
        Primitive for doing buffered upload on a single file. Used by the put() method
        Sends a multipart POST to the server; consisting of the initial XML, followed by a streamed, buffered read of the file contents
        """
        global bytes_sent

# mediaflux seems to have random periods of unresponsiveness - particularly around final ACK of transfer
# retries don't seem to work at all, but increasing the timeout seems to help cover the problem 
        upload_timeout = 1800

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

# different connection object for HTTPS vs HTTP
        if self.encrypted_data is True:
            self.log("DEBUG", "Using https for data: [%s]" % self.data_put, level=2)
            conn = httplib.HTTPSConnection(self.data_put, timeout=upload_timeout)
        else:
            self.log("DEBUG", "Using http for data: [%s]" % self.data_put, level=2)
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

# start sending the file
        conn.send(body)
        with open(filepath, 'rb') as infile:
            while True:
# NEW - specifically trap disk IO issues
                try:
                    chunk = infile.read(self.put_buffer)
                except Exception as e:
                    raise Exception("File read error: %s" % str(e))
# exit condition
                if not chunk:
                    break
# NEW - specifically trap network IO issues
                try:
                    conn.send(chunk)
                except Exception as e:
                    raise Exception("Network send error: %s" % str(e))
# record progress
                with bytes_sent.get_lock():
                    bytes_sent.value += len(chunk)

# terminating line (len(boundary) + 8)
        chunk = "\r\n--%s--\r\n" % boundary
        conn.send(chunk)
        self.log("DEBUG", "[pid=%d] File send completed, waiting for server..." % pid)

# get ACK from server (asset ID) else error (raise exception)
        resp = conn.getresponse()
        reply = resp.read()
        conn.close()
        tree = ET.fromstring(reply)

        message = "response did not contain an asset ID."
        for elem in tree.iter():
            if elem.tag == 'id':
                return int(elem.text)
            if elem.tag == 'message':
                message = elem.text

        raise Exception(message)

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
    @staticmethod
    def _xml_cloak(text):
        """
        Helper method for hiding sensitive text in XML posts
        """
        text1 = re.sub(r'session=[^>]*', 'session="..."', text)
        text2 = re.sub(r'<password>.*?</password>', '<password>xxxxxxx</password>', text1)
        text3 = re.sub(r'<token>.*?</token>', '<token>xxxxxxx</token>', text2)
        return text3

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

# NEW - intercept (before lexer!) and remove ampersand at end of line -> background job
        if aterm_line[-1:] == '&':
            flag_background = True
            aterm_line = aterm_line[:-1]
        else:
            flag_background = False

# NB - use posix=True as it's the closest to the way aterm processes input strings
        lexer = shlex.shlex(aterm_line, posix=True)
        lexer.whitespace_split = True
        xml_root = ET.Element(None)
        xml_node = xml_root
        child = None
        stack = []
        data_out_min = 0
        data_out_name = None
        flag_login = False

# first token is the service call, the rest are child arguments
        service_call = lexer.get_token()
        token = lexer.get_token()

# special cases for login
        if service_call == "system.logon":
            flag_login = True
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

# special case - out element - needs to be removed (replaced with outputs-via and an outputs-expected attribute)
                    if child.tag.lower() == "out":
                        data_out_name = child.text
                        data_out_min = 1
                        xml_node.remove(child)

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

# wrap with session/service call
        xml = ET.Element("request")
# special case for "system.login" as it does not work with "service.excute" - due to the requirement of a valid session
# TODO - this might be a workaround for expired sessions -> use token if exists
        child = ET.SubElement(xml, "service")
        if flag_login is True:
            child.set("name", service_call)
            args = ET.SubElement(child, "args")
            args.append(xml_root)
        else:
# wrap the service call in a service.execute to allow background execution, if desired 
            child.set("name", "service.execute")
# NB: use of token will bork download via session
            child.set("session", self.session)
            args = ET.SubElement(child, "args")
# NEW - background execution
            if flag_background is True:
                bg = ET.SubElement(args, "background")
                bg.text = "True"

            call = ET.SubElement(args, "service")
            call.set("name", service_call)
            call.append(xml_root)
# KEY change
            if data_out_min > 0:
                call.set("outputs", "%s" % data_out_min)
                output = ET.SubElement(args, "outputs-via")
# this returns data via the output URL
                output.text = "session"

        xml_text = ET.tostring(xml, method = 'xml')

# debug - password hiding for system.logon ...
        xml_hidden = self._xml_cloak(xml_text) 
        self.log("DEBUG", "XML out: %s" % xml_hidden, level=2)

# testing hook
        if post is not True:
            return xml_hidden

# send the service call and see what happens ...
        message = "This shouldn't happen"
        while True:
            try:
                reply = self._post(xml_text, output_local_filepath=data_out_name)
                if flag_background is True:
                    elem = reply.find(".//id")
                    job = elem.text

                    while True:
                        self.log("DEBUG", "Background job [%s] poll..." % job)
                        xml_poll = self.aterm_run("service.background.describe :id %s" % job)
                        elem = xml_poll.find(".//task/state")
#                        print elem.text
                        if "executing" in elem.text:
                            time.sleep(5)
                            continue
                        else:
                            break
# NB: it is an exception (error) to get results BEFORE completion
                    self.log("DEBUG", "Background job [%s] complete, getting results" % job)
                    xml_poll = self.aterm_run("service.background.results.get :id %s" % job)
                    return xml_poll
                else:
                    return reply

            except Exception as e:
                message = str(e)
                self.log("DEBUG", "POST exception: %s" % message)
                if "session is not valid" in message:
# FIXME - if mf_config has a valid session it will stop and not read the token (even if the token is valid)
# TODO - we could restart the session if we can implement a way to always grab the token if it's there
                    if self.token is not None:
                        self.log("DEBUG", "We have a token, attempting to establish new session")
                        self.login(token=self.token)
                        xml_text = re.sub('session=[^>]*', 'session="%s"' % self.session, xml_text)
                        pass
                break

# couldn't post without an error - give up
        raise Exception(message)

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
            if level > int(self.debug):
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
            self.token = token
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

# already exists
        if os.path.isfile(filepath) and not overwrite:
            self.log("DEBUG", "Local file of that name (%s) already exists, skipping." % filepath)
            with bytes_recv.get_lock():
                bytes_recv.value += os.path.getsize(filepath)
            return

# NEW - normal service call now that _post() supports :out 
        self.aterm_run("asset.get :id %s :out %s" % (asset_id, filepath))

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

# NEW -only-if-exists=true -> stops the exception / returns none if asset doesn't exists
        result = self.aterm_run('asset.get :id -only-if-exists true "path=%s" :xpath -ename id id :xpath -ename crc32 content/csum :xpath -ename size content/size' % remotepath)

# attempt checksum compare
        try:
            elem = result.find(".//id")
            asset_id = int(elem.text)
            elem = result.find(".//crc32")
            remote_crc32 = int(elem.text, 16)
            elem = result.find(".//size")
            remote_size = int(elem.text)
# NB: checksum calc on large files (several GB+) on an external HDD can be SLOW - slower than uploading the file again
#            local_crc32 = self.get_local_checksum(filepath)
            local_size = int(os.path.getsize(filepath))
#            if local_crc32 == remote_crc32:
            if local_size == remote_size:
                self.log("DEBUG", "Match; skipping [%s] -> [%s]" % (filepath, remotepath))
                with bytes_sent.get_lock():
                    bytes_sent.value += remote_size
                return asset_id
            else:
                self.log("DEBUG", "Mismatch; local=%r -> remote=%r" % (local_size, remote_size))

        except Exception as e:
            self.log("DEBUG", "Mismatch: %s" % str(e))
# file is missing -> exception -> flag upload
            overwrite = True

# local and remote crc32 don't match -> decision time ...
        if overwrite is True:
            self.log("DEBUG", "Uploading: [%s] -> [%s]" % (filepath, remotepath))
            xml_string = '<request><service name="service.execute" session="%s"><args><service name="asset.set">' % self.session
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
