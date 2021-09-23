#!/usr/bin/python

"""
This module is a Python 3.x (standard lib only) implementation of a mediaflux client
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
import urllib.request, urllib.error, urllib.parse
import http.client
import logging
import datetime
import platform
import functools
import mimetypes
import posixpath
import multiprocessing
import xml.etree.ElementTree as ET
import configparser

# Globals - multiprocess IO monitoring is hard
manage_lock = multiprocessing.Lock()
bytes_sent = multiprocessing.Value('d', 0, lock=True)
bytes_recv = multiprocessing.Value('d', 0, lock=True)
build= "20210923131216"

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
        asset_id = mfclient.put(data[0], data[1])
        return (int(asset_id), data[0], data[1])
    except Exception as e:
# FIXME - loggers don't work in separate process
        mfclient.logger.debug("put_jump(%s): %s" % (data[1], str(e)))

# report failure
    return (-1, data[0], data[1])

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
        mfclient.get(data[0], data[1])
        return (0, data[0], data[1])
    except Exception as e:
# FIXME - loggers don't work in separate process
        mfclient.logger.error("get_jump(): %s" % str(e))

# report failure
    return (-1, data[0], data[1])

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

#    def __init__(self, protocol, port, server, domain="system", session="", timeout=120, debug=0):
    def __init__(self, protocol, port, server, domain="system"):
        """
        Create a Mediaflux server connection instance. Raises an exception on failure.

        Args:
                           protocol: a STRING which should be either "http" or "https"
                               port: a STRING which is usually "80" or "443"
                             server: a STRING giving the FQDN of the server
                             domain: a STRING giving the authentication domain to use when authenticating
                            session: a STRING supplying the session ID which, if it exists, enables re-use of an existing authenticated session
                            timeout: an INTEGER specifying the connection timeout
                              debug: an INTEGER which controls output of troubleshooting information

        Returns:
            A reachable mediaflux server object that has not been tested for its authentication status

        Raises:
            Error if server appears to be unreachable
        """
# configure interfaces
        self.protocol = protocol
        self.server = server
        self.port = int(port)
        self.domain = domain
        self.timeout = 120

# NB: there can be some subtle bugs in python library handling if these are "" vs None
        self.session = ""
        self.token = ""

        self.logger = logging.getLogger('mfclient')
# TODO - set log level based on debug
#        self.logger.setLevel(logging.DEBUG)

        self.config_filepath = None
        self.config_section = None
        global build

# can override to test fast http data transfers (with https logins)
        if protocol == 'https':
            self.encrypted_data = True
        else:
            self.encrypted_data = False
# service call URL
        self.post_url = "%s://%s/__mflux_svc__" % (protocol, server)
# download/upload buffers
        self.get_buffer = 8192
        self.put_buffer = 8192
# XML pretty print hack
        self.indent = 0

# check server connection - unless in offline testing mode
        if server is not None:
            s = socket.socket()
            s.settimeout(7)
            s.connect((self.server, self.port))
            s.close()
# check for unecrypted connection (faster data transfers)
            try:
                response = urllib.request.urlopen("http://%s" % server, timeout=2)
                if response.code == 200:
                    self.encrypted_data = False
            except Exception as e:
                pass

# build data URLs
        if self.encrypted_data:
            self.data_get = "https://%s/mflux/content.mfjp" % server
            self.data_put = "%s:%s" % (server, 443)
        else:
            self.data_get = "http://%s/mflux/content.mfjp" % server
            self.data_put = "%s:%s" % (server, 80)

# more info
        self.logger.info("PLATFORM=%s" % platform.system())
        self.logger.info("MFCLIENT=%s" % build)
        self.logger.info("POST=%s" % self.post_url)
        self.logger.info("GET=%s" % self.data_get)
        self.logger.info("PUT=%s" % self.data_put)
        version = sys.version
        i = version.find("\n")
        self.logger.info("PYTHON=%s" % version[:i])
        self.logger.info("OpenSSL=%s", ssl.OPENSSL_VERSION)

#------------------------------------------------------------
    def config_init(self, config_filepath=None, config_section=None):
        """
        Setup config file and section to update with session/token info
        """
        self.logger.info("config filepath=[%s], config_section=[%s]" % (config_filepath, config_section))
        self.config_filepath = config_filepath
        self.config_section = config_section

#------------------------------------------------------------
    def config_save(self, refresh_token=False, refresh_session=False):
        """
        Update session/token info in config file
        """
        if self.config_filepath:
            config = configparser.ConfigParser()
            config.read(self.config_filepath)

            if refresh_token is True:
                self.logger.info("Updating token")
                config.set(self.config_section, 'token', self.token)

            if refresh_session is True:
                self.logger.info("Updating session")
                config.set(self.config_section, 'session', self.session)

            with open(self.config_filepath, 'w') as f:
                config.write(f)

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
    def _post(self, xml_string, out_filepath=None):
        """
        Primitive for sending an XML message to the Mediaflux server
        """

# no server - pass result back for offline testing
        if self.server is None:
            raise Exception(xml_string)

# NB: timeout exception if server is unreachable
        request = urllib.request.Request(self.post_url, data=xml_string, headers={'Content-Type': 'text/xml'})
        response = urllib.request.urlopen(request, timeout=self.timeout)
        xml = response.read()
        tree = ET.fromstring(xml)

# if error - attempt to extract a useful message
        elem = tree.find(".//reply/error")
        if elem is not None:
            elem = tree.find(".//message")
            error_message = self._xml_succint_error(elem.text)
            self.logger.debug("_post() raise: [%s]" % error_message)
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
            self.logger.debug("Using https for data: [%s]" % self.data_put)
            conn = http.client.HTTPSConnection(self.data_put, timeout=upload_timeout)
        else:
            self.logger.debug("Using http for data: [%s]" % self.data_put)
            conn = http.client.HTTPConnection(self.data_put, timeout=upload_timeout)

# kickoff
        self.logger.debug("[pid=%d] File send starting: %s" % (pid, filepath))
        conn.putrequest('POST', '/__mflux_svc__')
# headers
        conn.putheader('Connection', 'keep-alive')
        conn.putheader('Cache-Control', 'no-cache')
        conn.putheader('Content-Length', str(total_size))
        conn.putheader('Content-Type', 'multipart/form-data; boundary=%s' % boundary)
        conn.putheader('Content-Transfer-Encoding', 'binary')
        conn.endheaders()

# start sending the file
        conn.send(body.encode())
        with open(filepath, 'rb') as infile:
            while True:
# trap disk IO issues
                try:
                    chunk = infile.read(self.put_buffer)
                except Exception as e:
                    raise Exception("File read error: %s" % str(e))
# exit condition
                if not chunk:
                    break
# trap network IO issues
                try:
                    conn.send(chunk)
                except Exception as e:
                    raise Exception("Network send error: %s" % str(e))
# record progress
                with bytes_sent.get_lock():
                    bytes_sent.value += len(chunk)

# terminating line (len(boundary) + 8)
        chunk = "\r\n--%s--\r\n" % boundary
        conn.send(chunk.encode())
        self.logger.debug("[pid=%d] File send completed, waiting for server..." % pid)

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
    def aterm_run(self, input_line, background=False, post=True):
        """
        Method for parsing aterm's compressed XML syntax and sending to the Mediaflux server

        Args:
             service_call: raw input text that is assumed to be in aterm syntax
               post: if False will just return the argument part of the serialized XML, if True will post and return reply

        Returns:
            A STRING containing the server reply (if post is TRUE, if false - just the XML for test comparisons)
        """

# intercept (before lexer!) and remove ampersand at end of line -> background job
        if input_line[-1:] == '&':
            background = True
            input_line = input_line[:-1]

# use posix=True as it's the closest to how aterm processes input strings
# encoding the line (which is a str) creates an object with no read() method
# this input now has no read() method I guess ...
#        lexer = shlex.shlex(input_line.encode('utf-8'), posix=True)

# dropping the encode gets rid of the previous error
        lexer = shlex.shlex(input_line, posix=True)

# DS-421 fixes lexer dropping XML text payload starting with #, thinking it's a comment
        lexer.commenters=""
        lexer.whitespace_split = True
        xml_root = ET.Element(None)
        xml_node = xml_root
        child = None
        stack = []
        data_out_min = 0
        data_out_name = None
        flag_no_wrap = False

# first token is the service call, the rest are child arguments
        service_call = lexer.get_token()
        token = lexer.get_token()

# better handling of deletions to the XML
        xml_unwanted = None
        try:
#            while token:
            while token is not None:
                if token[0] == ':':
                    child = ET.SubElement(xml_node, '%s' % token[1:])
                    self.logger.debug("XML elem [%s]" % token[1:])
# if element contains : (eg csiro:seismic) then we need to inject the xmlns stuff
                    if ":" in token[1:]:
                        item_list = token[1:].split(":")
                        self.logger.debug("XML associate namespace [%s] with element [%s]" % (item_list[0], token[1:]))
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
                        self.logger.debug("XML text [%s]" % child.text)
                    except:
# -other => it's an XML attribute/property
                        key = token[1:]
                        value = lexer.get_token()
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        child.set(key, value)
                        self.logger.debug("XML prop [%r = %r]" % (key, value))
                else:
# FIXME - some issues here with data strings with multiple spaces (ie we are doing a whitespace split & only adding one back)
                    if child.text is not None:
                        child.text += " " + token
                    else:
                        if token.startswith('"') and token.endswith('"'):
                            child.text = token[1:-1]
                        else:
                            child.text = token

# don't display sensitive info
                    if child.tag.lower() == "password" or child.tag.lower() == 'token':
                        self.logger.debug("XML text [xxxxxxxx]")
# NEW - cope with special characters that may bork parsing
# NB: assumes :password is the LAST element in the service call
# use everything (to EOL) after :password as the password
                        index = input_line.find(" :password")
                        if index > 10:
                            child.text = input_line[index+11:]
                    else:
                        self.logger.debug("XML text [%s]" % child.text)

# special case - out element - needs to be removed (replaced with outputs-via and an outputs-expected attribute)
                    if child.tag.lower() == "out":
                        data_out_name = child.text
                        data_out_min = 1
# schedule for deletion but don't delete yet due to potentially multiple passthroughs 
                        xml_unwanted = child

# don't treat quotes as special characters in password string
                if "password" in token:
                    save_lexer_quotes = lexer.quotes
                    lexer.quotes = iter('') 
                    token = lexer.get_token()
                    lexer.quotes = save_lexer_quotes
                else:
                    token = lexer.get_token()

        except Exception as e:
            self.logger.error(str(e))
            raise SyntaxError

# do any deletions to the tree after processing 
        if xml_unwanted is not None:
            xml_node.remove(xml_unwanted)

# build the request XML tree
        xml = ET.Element("request")
        child = ET.SubElement(xml, "service")

# NEW - xmltree append() doesn't like it if xml_root contains *multiple* elements ... so it injects a <None> parent ...
# NEW - xmltree extend() works as intended ... but it's not available in python 2.6

# special case for "system.login" as it does not work when wrapped with "service.execute" - which requires a valid session
        if service_call == "system.logon":
            child.set("name", service_call)
            args = ET.SubElement(child, "args")
            for item in xml_root.findall("*"):
                args.append(item)

# special case for calls that are already wrapped in a service.execute 
        elif service_call == 'service.execute':
            child.set("name", service_call)
            child.set("session", self.session)
            args = ET.SubElement(child, "args")
            for item in xml_root.findall("*"):
                args.append(item)
# FIXME - this should better merge with below so we also cover the case with outputs ...
        else:
# wrap the service call in a service.execute to allow background execution, if desired 
            child.set("name", "service.execute")
            child.set("session", self.session)
            args = ET.SubElement(child, "args")
            if background is True:
                bg = ET.SubElement(args, "background")
                bg.text = "True"
            call = ET.SubElement(args, "service")
            call.set("name", service_call)
            for item in xml_root.findall("*"):
                call.append(item)

# return data via the output URL
            if data_out_min > 0:
                call.set("outputs", "%s" % data_out_min)
                output = ET.SubElement(args, "outputs-via")
                output.text = "session"

# convert XML to string for posting ...
        xml_text = ET.tostring(xml)

# password hiding for system.logon ...
#        xml_hidden = self._xml_cloak(xml_text) 
# PYTHON3 - bytes v strings
        xml_hidden = self._xml_cloak(xml_text.decode()).encode() 
        self.logger.debug("XML out: %s" % xml_hidden)

# testing hook
        if post is not True:
            return xml_text

# send the service call and see what happens ...
        message = "This shouldn't happen"
        while True:
            try:
                reply = self._post(xml_text)
                if background is True:
                    elem = reply.find(".//id")
                    job = elem.text
                    while True:
                        self.logger.debug("background job [%s] poll..." % job)
                        xml_poll = self.aterm_run("service.background.describe :id %s" % job)
                        elem = xml_poll.find(".//task/state")
                        item = xml_poll.find(".//task/exec-time")
#                        text = elem.text + " [ " + item.text + " " + item.attrib['unit'] + "(s) ]"
                        text = elem.text + " id=" + job + " [ " + item.text + " " + item.attrib['unit'] + "(s) ]"
                        if "executing" in elem.text:
                            sys.stdout.write("\r"+text)
                            sys.stdout.flush()
                            time.sleep(5)
                            continue
                        else:
                            print("\r%s    " % text)
                            break
# NB: it is an exception (error) to get results BEFORE completion
                    self.logger.debug("background job [%s] complete, getting results" % job)
                    xml_poll = self.aterm_run("service.background.results.get :id %s" % job)
# NB: mediaflux seems to not return any output if run in background (eg asset.get :id xxxx &)
# this seems like a bug?
#                    self.xml_print(xml_poll)
                    return xml_poll
                else:
# CURRENT - process reply for any output
# NB - can only cope with 1 output
                    if data_out_name is not None:
                        self.logger.debug("aterm_run(): output filename [%s]" % data_out_name)
                        elem_output = reply.find(".//outputs")
                        if elem_output is not None:
                            elem_id = elem_output.find(".//id")
                            output_id = elem_id.text
                            url = self.data_get + "?_skey=%s&id=%s" % (self.session, output_id)
                            url = url.replace("content", "output")
                            response = urllib.request.urlopen(url)
                            with open(data_out_name, 'wb') as output:
                                while True:
# trap network IO issues
                                    try:
                                        data = response.read(self.get_buffer)
                                    except Exception as e:
                                        raise Exception("Network read error: %s" % str(e))
# exit condition
                                    if not data:
                                        break
# trap disk IO issues
                                    try:
                                        output.write(data)
                                    except Exception as e:
                                        raise Exception("File write error: %s" % str(e))
# record progress
                                    with bytes_recv.get_lock():
                                        bytes_recv.value += len(data)
                        else:
                            self.logger.debug("missing output data in XML server response")
# successful
                    return reply

            except Exception as e:
                message = str(e)
                self.logger.debug(message)
                if "session is not valid" in message:
# restart the session if token exists
#                    if self.token is not None:
                    if len(self.token) > 0:
                        self.logger.debug("attempting login with token")
                        # FIXME - need to put this in a separate exception handling ...
                        self.login(token=self.token)

# PYTHON3 - due to the strings vs bytes change (ie xml_text is bytes rather than string) 
#                        xml_text = re.sub('session=[^>]*', 'session="%s"' % self.session, xml_text)
                        xml_text = re.sub('session=[^>]*', 'session="%s"' % self.session, xml_text.decode()).encode()
                        self.logger.debug("session restored, retrying command")
                        self.config_save(refresh_session=True)
                        continue
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
        for key, value in elem.attrib.items():
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
                print(self._xml_recurse(child).strip('\n'))
        else:
            print("Empty XML document")
        return

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
            self.logger.debug("Permitting unencrypted login; I hope you know what you're doing.")

# NEW - priority order and auto lookup of token or session in appropriate config file section
# NB: failed login calls raise an exception in aterm_run post XML handling
        reply = None
        if user is not None and password is not None:
            reply = self.aterm_run("system.logon :domain %s :user %s :password %s" % (self.domain, user, password))
        elif len(token) > 0: 
            reply = self.aterm_run("system.logon :token %s" % token)
            self.token = token
        else:
            raise Exception("Invalid login call.")

# if no exception has been raised, we should have a valid reply from the server at this point
        elem = reply.find(".//session")
        self.session = elem.text

#------------------------------------------------------------
    def authenticated(self):
        """
        Check client authentication state

        Returns:
             A BOOLEAN value depending on the current authentication status of the Mediaflux connection
        """
        if self.server is None:
            return True
        try:
# CURRENT - I suspect this is not multiprocessing safe ...  resulting in the false "session expired" problem during downloads
            self.aterm_run("system.session.self.describe")
            return True

        except Exception as e:
# NB: max licence error can occur here
            self.logger.debug(str(e))

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

        if os.path.isfile(filepath) and not overwrite:
            self.logger.debug("Local file of that name (%s) already exists, skipping." % filepath)
            with bytes_recv.get_lock():
                bytes_recv.value += os.path.getsize(filepath)
            return

# Windows path names and the posix lexer in aterm_run() are not good friends
        if "Windows" in platform.system():
            filepath = filepath.replace("\\", "\\\\")

        reply = self.aterm_run("asset.get :id %s :out %s" % (asset_id, filepath))

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

# find asset ID if exists, else create
        result = self.aterm_run('asset.get :id -only-if-exists true "path=%s" :xpath -ename id id :xpath -ename crc32 content/csum :xpath -ename size content/size' % remotepath)
        xml_id = result.find(".//id")
        if xml_id is None:
            self.logger.debug("No remote file found: [%s]" % remotepath)
# NB: must create intermediate directories if they don't exist (mediaflux won't do it by default)
            reply = self.aterm_run('asset.create :namespace -create "true" %s :name %s' % (namespace, filename))
            xml_id = reply.find(".//id")
        else:
# NB: assets with no content can have either the root element or the text set to None
            remote_size = 0
            xml_size = result.find(".//size")
            if xml_size is not None:
                if xml_size.text is not None:
                    remote_size = int(xml_size.text)
# if sizes match (checksum compare is excrutiatingly slow) don't overwrite
            local_size = int(os.path.getsize(filepath))
            if remote_size == local_size:
                self.logger.debug("Match; skipping [%s] -> [%s]" % (filepath, remotepath))
                overwrite = False
                with bytes_sent.get_lock():
                    bytes_sent.value += remote_size
            else:
                self.logger.debug("Mismatch; local=%r -> remote=%r" % (local_size, remote_size))

        asset_id = int(xml_id.text)
        # NB: create=true to generate intermediate directories (if needed)
        if overwrite is True:
            self.logger.debug("Uploading asset=%d: [%s] -> [%s]" % (asset_id, filepath, remotepath))
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
            self.logger.debug("Total upload bytes not supplied, counting...")
            for namespace, filepath in list_namespace_filepath:
                try:
                    total_bytes += os.path.getsize(filepath)
                except:
                    self.logger.debug("Can't read %s, skipping." % filepath)
# shenanigans to enable mfclient method to be called from the global process pool (python can't serialize instance methods)
        put_alias = functools.partial(put_jump, self)
        self.logger.debug("Total upload bytes: %d" % total_bytes)
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
        global manage_lock

# fail if there is already a managed transfer (there can only be one!)
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
