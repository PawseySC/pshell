#!/usr/bin/env python3

"""
This module is a Python 3.x (standard lib only) implementation of a mediaflux client
Author: Sean Fleming
"""

import os
import re
import sys
import ssl
import math
import time
import zlib
import shlex
import random
import string
import getpass
import logging
import datetime
import platform
import mimetypes
import posixpath
import threading
import http.client
import configparser
import xml.etree.ElementTree as ET
import urllib.request, urllib.error, urllib.parse
from pathlib import PurePath

#------------------------------------------------------------
class mf_client():
    """
    Base Mediaflux authentication and communication client
    All unexpected failures are handled by raising exceptions
    """
    def __init__(self, protocol="http", port="80", server="localhost", domain="system", encrypted_data=True):
        """
        Create a Mediaflux server connection instance. Raises an exception on failure.

        Args:
                           protocol: a STRING which should be either "http" or "https"
                               port: a STRING which is usually "80" or "443"
                             server: a STRING giving the FQDN of the server
                             domain: a STRING giving the authentication domain to use when authenticating

        Returns:
            A reachable mediaflux server object that has not been tested for its authentication status

        Raises:
            Error if server appears to be unreachable
        """

# configure interfaces
        self.type = "mflux"
        self.protocol = protocol
        self.server = server
        self.port = int(port)
        self.domain = domain
        self.timeout = 120
        self.status = "not connected"

# NB: there can be some subtle bugs in python library handling if these are "" vs None
        self.session = ""
        self.token = ""
        self.logging = logging.getLogger('mfclient')

# download/upload buffers
        self.get_buffer = 8192
        self.put_buffer = 8192
# XML pretty print hack
        self.indent = 0

# NEW
        self.enable_polling = True

# POST URL
        self.post_url = "%s://%s/__mflux_svc__" % (protocol, server)

# can override to test fast http data transfers (with https logins)
        if protocol == 'https':
            self.encrypted_data = encrypted_data
        else:
            self.encrypted_data = False

# data URLs ... a bit hacky (hard coded ports) but the speed improvement is huge
        if self.encrypted_data:
            self.data_get = "https://%s/mflux/content.mfjp" % server
            self.data_put = "%s:%s" % (server, 443)
        else:
            self.data_get = "http://%s/mflux/content.mfjp" % server
            self.data_put = "%s:%s" % (server, 80)

# more info
        self.logging.debug("POST=%s" % self.post_url)
        self.logging.debug("GET=%s" % self.data_get)
        self.logging.debug("PUT=%s" % self.data_put)
        self.logging.debug("OpenSSL=%s", ssl.OPENSSL_VERSION)

# --- NEW
    @classmethod
    def from_endpoint(cls, endpoint):
        """
        Create mfclient using an endpoint description
        """
        if 'url' in endpoint:
            url = urllib.parse.urlparse(endpoint['url'])
            endpoint['port'] = url.port
            endpoint['server'] = url.hostname
            endpoint['protocol'] = url.scheme

        if 'encrypt' in endpoint:
            encrypt = endpoint['encrypt']
        else:
            encrypt = True

        client = cls(protocol=endpoint['protocol'], server=endpoint['server'], port=endpoint['port'], encrypted_data=encrypt)

        client.status = "not connected"

        if 'domain' in endpoint:
            client.domain = endpoint['domain']
        if 'session' in endpoint:
            client.session = endpoint['session']
        if 'token' in endpoint:
            client.token = endpoint['token']

        return client

#------------------------------------------------------------
    def endpoint(self):
        """
        Return configuration as endpoint description
        """
        endpoint = { 'type':self.type, 'protocol':self.protocol, 'server':self.server, 'port':self.port, 'domain':self.domain }
        endpoint['encrypt'] = self.encrypted_data
        endpoint['session'] = self.session
        endpoint['token'] = self.token

        return endpoint

#------------------------------------------------------------
    def connect(self):
        """
        Acquire connection status via session or token
        """
# NEW - added /aterm path to connection test 
# without this it will be attempting to connect to the web server - which may not be configured and doesn't need to be for API access
        url = "%s://%s:%d/aterm" % (self.protocol, self.server, self.port)
        self.logging.info("url=[%s]" % url)

# reachability check
        try:
            code = urllib.request.urlopen(url, timeout=5).getcode()
            self.logging.info("connection code=%r" % code)
        except Exception as e:
# TODO - more precise messaging, eg using str(e) content
# TODO - eg timeout status
            self.status = "not connected" 
            self.logging.info(str(e))
            return False

# fast data channel check
        if self.protocol == 'https' and self.encrypted_data == True:
            try:
# updated connection path test
                response = urllib.request.urlopen("http://%s:80/aterm" % self.server, timeout=5)
                if response.code == 200:
                    self.logging.info("Setting data channel to http")
                    self.encrypted_data = False
# override data channel only
                    self.data_get = "http://%s/mflux/content.mfjp" % self.server
                    self.data_put = "%s:%s" % (self.server, 80)
            except Exception as e:
                self.logging.debug(str(e))

# convert session into a connection description
        try:
# NEW - better baseline check in terms of permissions
#NB: don't use actor[name] as this might be an internal mediaflux ID
            reply = self.aterm_run("actor.self.describe")
            elem = reply.find(".//actor")
            if elem is not None:
#                self.status = "authenticated to: %s" % url
# NEW - check for expired/destroyed token
                if 'destroyed' in elem.attrib:
                    if elem.attrib['destroyed'] == 'true':
                        raise Exception("Delegate destroyed")
                self.status = "connected"
                return True

        except Exception as e:
            message = str(e)
            self.logging.info(message)
            if "maintenance mode" in message:
                self.status = "maintenance"
                return False

        self.status = "login required"
        return False

#------------------------------------------------------------
    def login(self, user=None, password=None, domain=None, token=None):
        """
        Authenticate to the server and record the session on success

        Input:
            user, password: STRINGS specifying user login details
                     token: STRING specifying a delegate credential

        Raises:
            An error if authentication fails
        """

# security check
        if self.protocol != "https":
            self.logging.debug("Permitting unencrypted login; I hope you know what you're doing.")

# NEW - priority order: use preset domain, otherwise API argument, finally prompt
        if self.domain is not None:
            domain = self.domain
        else:
            if domain is None:
                domain = input("Domain: ")

# command prompt entry
        if user is None and token is None:
            logging.info("Authentication domain [%s]" % domain)
            user = input("Username: ")
            password = getpass.getpass("Password: ")

# create a session - failed aterm_run calls should raise an exception that gets handed back up
# priority order: user/password followed by token
        reply = None
        if user is not None:
            reply = self.aterm_run("system.logon :domain %s :user %s :password %s" % (domain, user, password))
        else:
            if token is not None:
                if len(token) > 0:
                    logging.info("Secure token login.")
                    reply = self.aterm_run("system.logon :token %s" % token)
                    self.token = token
# attempt to extract a session
        try:
            elem = reply.find(".//session")
            self.session = elem.text
            self.logging.info("Established session: %s" % self.session)
# refresh connection info
            self.connect()
        except Exception as e:
            self.logging.error(str(e))
            raise Exception("Invalid login call")

#------------------------------------------------------------
    def polling(self, polling_state=True):
        """
        Set the current polling state, intended for terminating threads
        """
        self.logging.info("Set polling: %r" % polling_state)
        with threading.Lock():
            self.enable_polling = polling_state

#------------------------------------------------------------
    def logout(self):
        """
        Destroy the current session (NB: delegate can auto-create a new session if available)
        """
        self.aterm_run("system.logoff")
        self.session = ""
        self.status = "login required"

#------------------------------------------------------------
    def delegate(self, line):
        """
        Create a secure token for use in authenticating
        """
        delegate_default = 7
        delegate_min = 1
        delegate_max = 365

# destroy
        if line.startswith('off'):
            self.logging.debug("Destroying secure tokens...")
            self.aterm_run("secure.identity.token.all.destroy")
            self.token = ""
            return True

# expiry date setup
        try:
            dt = max(min(float(line), delegate_max), delegate_min)
        except:
            dt = delegate_default
        d = datetime.datetime.now() + datetime.timedelta(days=dt)
        expiry = d.strftime("%d-%b-%Y %H:%M:%S")

        try:
# query current authenticated identity
            result = self.aterm_run("actor.self.describe")
            elem = result.find(".//actor")
            actor = elem.attrib['name']
            i = actor.find(":")
            domain = actor[0:i]
            user = actor[i+1:]
            self.logging.debug("Attempting to delegate for: domain=%s, user=%s, until=%r" % (domain, user, expiry))
# attempt to delegate as current identity
            result = self.aterm_run('secure.identity.token.create :to "%s" :role -type user "%s" :role -type domain "%s" :min-token-length 16 :wallet true' % (expiry, actor, domain))
            elem = result.find(".//token")
            self.token = elem.text
            return True

        except Exception as e:
            self.logging.error(str(e))

        return False

#------------------------------------------------------------
    def whoami(self):
        """
        Display information about the authenticated identity
        """
        xml_reply = self.aterm_run("actor.self.describe")
        result = []
# main identity
        for elem in xml_reply.iter('actor'):
            user_name = elem.attrib['name']
            user_type = elem.attrib['type']
            if 'identity' in user_type:
# MFLUX BUG - can run a describe ... but if specify an id - even for a token I own - it generates a permission error
# workaround - run and search for the right token to get it's validity
                xml_expiry = self.aterm_run("secure.identity.token.describe")
                expiry = "Never"
                for elem_id in xml_expiry.findall(".//identity"):
                    elem_actor = elem_id.find(".//actor")
                    if elem_actor is not None:
                        if user_name in elem_actor.text:
                            elem_valid = elem_id.find(".//validity/to")
                            expiry = elem_valid.text
                result.append("user = delegate (expires %s)" % expiry)
            else:
                result.append("%s = %s" % (user_type, user_name))
# associated roles
        for elem in xml_reply.iter('role'):
            result.append("  role = %s" % elem.text)

        return result

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
    def _post(self, xml_bytes, out_filepath=None):
        """
        Primitive for sending an XML message to the Mediaflux server
        """
# NB: timeout exception if server is unreachable
        elem=None
        try:
#            request = urllib.request.Request(self.post_url, data=xml_bytes, headers={'Content-Type': 'text/xml'})
            request = urllib.request.Request(self.post_url, data=xml_bytes, headers={'Content-Type': 'text/xml', 'charset': 'utf-8'})

            response = urllib.request.urlopen(request, timeout=self.timeout)
            xml = response.read()
            tree = ET.fromstring(xml.decode())
            elem = tree.find(".//reply/error")
# process connection error
        except Exception as e:
            self.logging.debug(str(e))
            raise Exception(str(e))
# process server response error
        if elem is not None:
            elem = tree.find(".//message")
            error_message = self._xml_succint_error(elem.text)
            self.logging.debug("raise: [%s]" % error_message)
            raise Exception(error_message)

        return tree

#------------------------------------------------------------
    def _post_multipart_buffered(self, xml, filepath, cb_progress=None):
        """
        Primitive for doing buffered upload on a single file. Used by the put() method
        Sends a multipart POST to the server; consisting of the initial XML, followed by a streamed, buffered read of the file contents
        """
# mediaflux seems to have random periods of unresponsiveness - particularly around final ACK of transfer
# retries don't seem to work at all, but increasing the timeout seems to help cover the problem 
        upload_timeout = 1800

# setup
        pid = os.getpid()
        boundary = ''.join(random.choice(string.digits + string.ascii_letters) for i in range(30))
        filename = os.path.basename(filepath)
# default
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
            self.logging.debug("Using https for data: [%s]" % self.data_put)
            conn = http.client.HTTPSConnection(self.data_put, timeout=upload_timeout)
        else:
            self.logging.debug("Using http for data: [%s]" % self.data_put)
            conn = http.client.HTTPConnection(self.data_put, timeout=upload_timeout)

# kickoff
        self.logging.debug("[pid=%d] File send starting: %s" % (pid, filepath))
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
# TODO - we *could* allow ctrl-C interruption here via enable_polling state, but could create a mess on the server
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
                    if cb_progress is not None:
                        cb_progress(len(chunk))
                except Exception as e:
                    raise Exception("Network send error: %s" % str(e))

# terminating line (len(boundary) + 8)
        chunk = "\r\n--%s--\r\n" % boundary
        conn.send(chunk.encode())
        self.logging.debug("[pid=%d] File send completed, waiting for server..." % pid)

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
        Helper method for hiding sensitive text in XML posts so they can be displayed
        """
        text1 = re.sub(r'session=[^>]*', 'session="..."', text)
        text2 = re.sub(r'<password>.*?</password>', '<password>xxxxxxx</password>', text1)
        text3 = re.sub(r'<token>.*?</token>', '<token>xxxxxxx</token>', text2)
        text4 = re.sub(r'<service name="secure.wallet.set">.*?</service>', '<service name="secure.wallet.set">xxxxxxx</service>', text3)
        return text4

#------------------------------------------------------------
# TODO - convert to background always true -> will need to fix :out first (see below) and a whole lot of other things
    def aterm_run(self, input_line, background=False, post=True, description=None, show_progress=False):
        """
        Method for parsing aterm's compressed XML syntax and sending to the Mediaflux server

        Args:
             service_call: raw input text that is assumed to be in aterm syntax
               post: if False will just return the argument part of the serialized XML, if True will post and return reply

        Returns:
            A STRING containing the server reply (if post is TRUE, if false - just the XML for test comparisons)
        """

# TODO - I suspect parser should do this (if appropriate) and pass background=True to the module implementations
# intercept (before lexer!) and remove ampersand at end of line -> background job
        if input_line[-1:] == '&':
            background = True
            input_line = input_line[:-1]
            self.logging.info("running in background: [%s]" % input_line)

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
        flag_password = False

# first token is the service call, the rest are child arguments
        service_call = lexer.get_token()
        token = lexer.get_token()

# better handling of deletions to the XML
        xml_unwanted = None
        try:
            while token is not None:
#                print("token=[%s], child=[%r], flag_pwd=%r" % (token, child, flag_password))
                if token[0] == ':':
                    child = ET.SubElement(xml_node, '%s' % token[1:])
# if element contains : (eg csiro:seismic) then we need to inject the xmlns stuff
                    if ":" in token[1:]:
                        item_list = token[1:].split(":")
                        self.logging.debug("XML associate namespace [%s] with element [%s]" % (item_list[0], token[1:]))
                        child.set("xmlns:%s" % item_list[0], item_list[0])
# NEW - flag that we expect the next token to be password data
                    if token == ':password':
                        flag_password = True

# these are special XML attribute/nesting characters
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
                    except:
# -other => it's an XML attribute/property
                        key = token[1:]
                        value = lexer.get_token()
                        if value is not None:
                            if value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            child.set(key, value)
                        else:
# someone put in something silly, I think...
                            raise Exception ("Malformed input command")
                else:
# if not a new element, or a special characters, should be text for the child element
                    if child is not None:
# FIXME - some issues here with data strings with multiple spaces (ie we are doing a whitespace split & only adding one back)
                        if child.text is not None:
                            child.text += " " + token
                        else:
                            if token.startswith('"') and token.endswith('"'):
                                child.text = token[1:-1]
                            else:
                                child.text = token

# special case - out element - needs to be removed (replaced with outputs-via and an outputs-expected attribute)
                        if child.tag.lower() == "out":
                            data_out_name = child.text
                            data_out_min = 1
# schedule for deletion but don't delete yet due to potentially multiple passthroughs 
                            xml_unwanted = child

# NEW - special case handling for password element text 
                if flag_password is True:
# HACK - in order to bypass lexer tokenisation (which destroys any multiple white spaces) and avoid special characters - assume :password is last entry
                    n = input_line.find(':password ')
                    child.text = input_line[n+10:]
# since we're assuming the password was the very last element - we're done
                    break

# next token
                token = lexer.get_token()

        except Exception as e:
            self.logging.error(str(e))
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
# the case where the call can't be wrapped in a service.execute
            child.set("name", service_call)
            args = ET.SubElement(child, "args")
            for item in xml_root.findall("*"):
                args.append(item)
        elif service_call == 'service.execute':
# the case where the call is already wrapped in a service.execute
            child.set("name", service_call)
            child.set("session", self.session)
            args = ET.SubElement(child, "args")
            for item in xml_root.findall("*"):
                args.append(item)
        else:
# wrap the service call in a service.execute to allow background execution, if desired 
            child.set("name", "service.execute")
            child.set("session", self.session)
            args = ET.SubElement(child, "args")

            if background is True:
                bg = ET.SubElement(args, "background")
                bg.text = "True"

            if description is not None:
                desc = ET.SubElement(args, "description")
                desc.text = description

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

# PYTHON3 - bytes v strings
        xml_hidden = self._xml_cloak(xml_text.decode()).encode() 
        self.logging.debug("XML out: %r" % xml_hidden)

# testing hook
        if post is not True:
            return xml_text

# send the service call and see what happens ...
        message = "This shouldn't happen"
#        while True:
# CURRENT - only 2 tries - 1st ... possibly second if session has expired (and we can regen with token) ... after that - done
        post_count = 0
        post_retry = True

        while post_retry is True:
            self.logging.debug("loop: post_count=%d, post_retry=%r" % (post_count, post_retry))
            post_count += 1
            post_retry = False
            try:
# NEW - INFO on timing for mflux service calls
                start_time = time.time()
# main POST to server
                reply = self._post(xml_text)
                if background is True:
                    elem = reply.find(".//id")
                    job = elem.text
                    done = False
                    while done is False:
                        self.logging.debug("background task [%s] poll..." % job)

# CURRENT - an issue with calling self in some edge cases?
# TODO - switch to plain _post ... ?
                        state = "unknown"
                        description = "unknown"

                        xml_poll = None
                        try:
                            xml_poll = self.aterm_run("service.background.describe :id %s" % job)

#                            self.xml_print(xml_poll)
# try and build a consistent user report using wildly different mediaflux reports
                            text = "task id=%s, " % job

                            elem = xml_poll.find(".//task/description")
                            if elem is not None:
                                description = elem.text
                                text += "%s, " % description

                            elem = xml_poll.find(".//task/state")
                            if elem is not None:
                                state = elem.text
                                text += "%s " % state

# set exit flag
                            if "complete" in state:
                                xml_poll = self.aterm_run("service.background.results.get :id %s" % job)
                                done = True
                            elif "fail" in state:
                                done = True

# TODO
#                            text += "elapsed=%s    " % exec_time

                        except Exception as e:
                            text = "task id=%s, polling error  " % job
                            self.logging.error(str(e))
                            done = True

# show progress report, if requested
                        if show_progress is True:
#                            sys.stdout.write("\r"+text)
                            sys.stdout.write("\r"+text)
                            sys.stdout.flush()
#                            if done is True:
#                                print("        ")

# if not done, sleep to prevent overloading server with requests
                        if done is False:
                            time.sleep(5)

# successful
# NB: mediaflux seems to not return any output if run in background (eg asset.get :id xxxx &)
# this seems like a bug?
#                    self.xml_print(xml_poll)
                    elapsed = time.time() - start_time
                    self.logging.debug("completed: %s, elapsed: %r" % (service_call, elapsed))
                    return xml_poll

                else:
# CASE 2 - not run in background
                    if data_out_name is not None:
# output field specified (eg download file)
# FIXME - can only cope with 1 output
                        self.logging.debug("output filename [%s]" % data_out_name)
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
                        else:
                            self.logging.debug("missing output data in XML server response")
# successful
                    elapsed = time.time() - start_time
                    self.logging.debug("completed: %s, elapsed: %r" % (service_call, elapsed))
                    return reply

            except Exception as e:
                message = str(e)
                self.logging.debug(message)
# only flag a retry if the session was invalid and we have a token
                if "session is not valid" in message:
                    if len(self.token) > 0:
                        if post_count == 1:
                            post_retry = True

            if post_retry is True:
# use raw post here ... not a recursive aterm_run() as this may get stuck in a re-try loop
                self.logging.info("Attempting to restore session with token")
                try:
                    xml_raw = '<request><service name="system.logon"><args><token>%s</token></args></service></request>' % self.token
                    xml_retry = self._post(xml_raw.encode())
                    elem = xml_retry.find(".//session")
                    self.session = elem.text
# PYTHON3 - due to the strings vs bytes change (ie xml_text is bytes rather than string) 
                    xml_text = re.sub('session=[^>]*', 'session="%s"' % self.session, xml_text.decode()).encode()

                except Exception as e:
# no point continuing with to retry - couldn't regenerate a valid session
                    message = str(e)
                    self.logging.error(message)
                    self.session = ""
                    post_retry = False

# give up with the most recent error message
        raise Exception(message)

#------------------------------------------------------------
    def command(self, text):
        """
        Default passthrough method
        """
        self.logging.debug(text)

# special commands
        if text.startswith("delegate "):
            args = text[8:].strip()
            self.delegate(args)
        else:
# assumed mflux service calls
            reply = self.aterm_run(text)
            self.xml_print(reply)

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
        for child in list(elem):
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
            # TODO - replace with ET.tostring() ?
            for child in list(elem):
                print(self._xml_recurse(child).strip('\n'))
        else:
            print("Empty XML document")
        return

#------------------------------------------------------------
    def _xml_xpath_boolean(self, xpath, xml):
        """
        Helper for extracting boolean from expected xpath query 
        """
        elem = xml.find(xpath)
        if elem is not None:
            if elem.text == "true":
                return True
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
# completion helper ...
    def abspath(self, cwd, path):
        """
        enforce absolute remote namespace path
        """
        self.logging.debug("cwd = [%s] input = [%s]" % (cwd, path))
        if not posixpath.isabs(path):
            fullpath = posixpath.normpath(posixpath.join(cwd, path))
        else:
            fullpath = posixpath.normpath(path)

        return fullpath

#------------------------------------------------------------
    def complete_folder(self, cwd, partial_ns, start):
        """
        Command line completion for folders (aka namespaces)
        """
        self.logging.debug("cn seek: cwd=[%s] partial_ns=[%s] start=[%d]" % (cwd, partial_ns, start))
# extract any partial namespace to use as pattern match
        match = re.match(r".*/", partial_ns)
        if match:
            offset = match.end()
            pattern = partial_ns[offset:]
        else:
            offset = 0
            pattern = partial_ns

# namespace fragment prefix (if any) to include in the returned candidate
        prefix = partial_ns[start:offset]
# offset to use when extracting completion string from candidate matches
        xlat_offset = max(0, start-offset)

# special case - we "know" .. is a namespace
        if pattern == "..":
            return [partial_ns[start:]+"/"]

# construct an absolute namespace (required for any remote lookups)
#        target_ns = self.absolute_namespace(partial_ns[:offset])
        target_ns = self.abspath(cwd, partial_ns[:offset])

        self.logging.debug("cn seek: target_ns: [%s] : prefix=[%r] : pattern=[%r] : start=%r : xlat=%r" % (target_ns, prefix, pattern, start, xlat_offset))

# generate listing in target namespace for completion matches
        result = self.aterm_run('asset.namespace.list :namespace "%s"' % target_ns)

        ns_list = []
        for elem in result.iter('namespace'):
            if elem.text is not None:
# namespace matches the pattern we're looking for?
                item = None
                if len(pattern) != 0:
                    if elem.text.startswith(pattern):
                        item = posixpath.join(prefix, elem.text[xlat_offset:]+"/")
                else:
                    item = posixpath.join(prefix, elem.text[xlat_offset:]+"/")

                if item is not None:
                    ns_list.append(item)

        self.logging.debug("cn found: %r" % ns_list)

        return ns_list

# --- helper
    def escape_single_quotes(self, namespace):
        return namespace.replace("'", "\\'")

#------------------------------------------------------------
    def complete_file(self, cwd, partial_asset_path, start):
        """
        Command line completion for files (aka assets)
        """
        self.logging.debug("ca seek: cwd=[%s] partial_asset=[%s] start=[%d]" % (cwd, partial_asset_path, start))
# construct an absolute namespace (required for any remote lookups)
        candidate_ns = self.abspath(cwd, partial_asset_path)

        if self.namespace_exists(candidate_ns):
# candidate is a namespace -> it's our target for listing
            target_ns = candidate_ns
# no pattern -> add all namespaces
            pattern = None
# replacement prefix for any matches
            prefix = partial_asset_path[start:]
        else:
# candidate not a namespace -> set the parent as the namespace target
            match = re.match(r".*/", candidate_ns)
            if match:
                target_ns = match.group(0)
# extract pattern to search and prefix for any matches
                pattern = candidate_ns[match.end():]
                prefix = partial_asset_path[start:-len(pattern)]
            else:
                return None

        target_ns = self.escape_single_quotes(target_ns)
        self.logging.debug("ca seek: target_ns: [%s] : pattern = %r : prefix = %r" % (target_ns, pattern, prefix))

        if pattern is not None:
            result = self.aterm_run("asset.query :where \"namespace='%s' and name ='%s*'\" :action get-values :xpath -ename name name" % (target_ns, pattern))
        else:
            result = self.aterm_run("asset.query :where \"namespace='%s'\" :action get-values :xpath -ename name name" % target_ns)

#       ALT? eg for elem in result.findall(".//name")
        asset_list = []
        for elem in result.iter("name"):
            if elem.text is not None:
#                asset_list.append(posixpath.join(prefix, elem.text))
# NEW - check we're not suggesting a repeat of the non-editable part of the completion string
                if elem.text.startswith(partial_asset_path[:start]):
                    asset_list.append(posixpath.join(prefix, elem.text)[start:])
                else:
                    asset_list.append(posixpath.join(prefix, elem.text))

        self.logging.debug("ca found: %r" % asset_list)

        return asset_list

#------------------------------------------------------------
    def human_size(self, nbytes):
        """
        Produce a human readable version for an input number of bytes
        """
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
    def rmdir(self, namespace, prompt=None):
        """
        remove a namespace
        """
# TODO - compute count and size of assets for prompt
        if prompt is not None:
            if prompt("Delete folder %s (y/n): " % namespace) is False:
                return False
# run the removal
        self.aterm_run('asset.namespace.destroy :namespace "%s"' % namespace.replace('"', '\\\"'), background=True, show_progress=True)
        print("")
        return True

#------------------------------------------------------------
    def mkdir(self, namespace):
        """
        create a namespace
        """
        self.aterm_run('asset.namespace.create :namespace "%s"' % namespace.replace('"', '\\\"'))

#------------------------------------------------------------
    def cd(self, namespace):
        if self.namespace_exists(namespace):
            return namespace
        raise Exception("No such folder")

#------------------------------------------------------------
    def rm(self, fullpath, prompt=None):
        """
        remove a file pattern
        """
        query = self.get_query(fullpath)
        if 'and name' not in query:
            raise Exception("Use rmdir for folders")

# get the number of items to delete 
        reply = self.aterm_run('asset.query %s :action count' % query, background=True)
        elem = reply.find(".//value")
        count = int(elem.text)
        if count == 0:
            raise Exception("Nothing to delete")
# query to confirm removal
        if prompt is not None:
            if prompt("Delete %d files (y/n): " % count) is False:
                return False
        self.logging.info("Destroy confirmed.")
        self.aterm_run('asset.query %s :action pipe :service -name asset.destroy' % query, background=True, show_progress=True)
        print("")
        return True

#------------------------------------------------------------
    def info_iter(self, fullpath):
        """
        information on a named file or folder
        """
        if self.namespace_exists(fullpath):
            self.logging.info("Namespace exists")
            yield "%20s : %s" % ('namespace', fullpath) 
            xml_reply = self.aterm_run('asset.namespace.describe :namespace %s' % fullpath, background=True)
            elem = xml_reply.find(".//namespace/ctime")
            if elem is not None:
                yield "%20s : %s" % ('ctime', elem.text) 
# at project level, can report quota/usage
            elem = xml_reply.find(".//quota/allocation")
            if elem is not None:
                yield "%20s : %s" % ('quota', self.human_size(elem.text))
                elem = xml_reply.find(".//quota/used")
                yield "%20s : %s" % ('usage', self.human_size(elem.text))
                elem = xml_reply.find(".//quota/count")
                yield "%20s : %s" % ('count', elem.text) 
            else:
# all other namespaces, have to query for the info
#                query = "namespace>='%s'" % fullpath
#                xml_reply = self.aterm_run('asset.query :where "%s" :count true :action sum :xpath content/size' % query, background=True)
# july 2024 - performance improvement
                xml_reply = self.aterm_run('asset.query :namespace "%s" :count true :action sum :xpath content/size' % fullpath, background=True)
                elem = xml_reply.find(".//value")
                if elem is not None:
                    yield "%20s : %s" % ('usage', self.human_size(elem.text))
                    yield "%20s : %s" % ('count', elem.attrib['nbe']) 
        else:
            try:
# get asset information, if it exists
# TODO (maybe) - redo to allow patterns ... 
                result = self.aterm_run('asset.get :id "path=%s"' % fullpath)
                elem = result.find(".//asset")
                yield "%20s : %s" % ('asset', elem.attrib['id'])
                xpath_list = [".//asset/path", ".//asset/ctime", ".//asset/type", ".//content/csum"]
                for xpath in xpath_list:
                    elem = result.find(xpath)
                    if elem is not None:
                        yield "%20s : %s" % (elem.tag, elem.text)
# content size
                elem = result.find(".//asset/content/size")
                if elem is not None:
                    yield "%20s : %s" % ('size', self.human_size(elem.text))

# get content status 
                result = self.aterm_run('asset.content.status :id "path=%s"' % fullpath)
                elem = result.find(".//asset/state")
                if elem is not None:
                    yield "%20s : %s" % (elem.tag, elem.text)

# published (public URL)
                if fullpath.startswith('/projects/'):
                    result = self.aterm_run('asset.label.exists :id "path=%s" :label PUBLISHED' % fullpath)
                    elem = result.find(".//exists")
                    if elem is not None:
                        if 'true' in elem.text.lower():
# NB: download path will be wrong if we're looking anywhere other than /project, eg in www/ 
                            public_url = '%s://%s/download/%s' % (self.protocol, self.server, urllib.parse.quote(fullpath[10:]))
                            yield "%20s : %s" % ('published', public_url)

            except Exception as e:
                self.logging.debug(str(e))
                raise Exception("No such file or folder")

#------------------------------------------------------------
    def ls_iter(self, pattern):
        """
        generator for namespace/asset listing
        """
# yield folders first (only if pattern is a folder)
# NB: mediaflux quirk - can't pattern match against namespaces (only assets/files)
        if self.namespace_exists(pattern):
            reply = self.aterm_run('asset.namespace.list :namespace %s' % pattern)
            ns_list = reply.findall('.//namespace/namespace')
            for ns in ns_list:
                yield "[folder] %s" % ns.text

# yield all matching assets 
        query = self.get_query(pattern)
        result = self.aterm_run('asset.query %s :as iterator :action get-values :xpath -ename id id :xpath -ename name name :xpath -ename size content/size' % query)

        elem = result.find(".//iterator")
        iterator = int(elem.text)

        iterate_size = 100
        complete = "false"
        while complete != "true":
            result = self.aterm_run("asset.query.iterate :id %d :size %d" % (iterator, iterate_size))
            elem = result.find(".//iterated")
            if elem is not None:
                complete = elem.attrib['complete'].lower()
            self.logging.debug("asset query iterator chunk [%d] - complete[%s]" % (iterator, complete))
# parse the asset results
            for elem in result.findall(".//asset"):
                asset_id = '?'
                name = '?'
                size = '?'
                for child in elem:
                    if child.tag == "id":
                        asset_id = child.text
                    if child.tag == "name":
                        name = child.text
                    if child.tag == "size":
                        size = self.human_size(child.text)
                yield " %-10s | %s | %s" % (asset_id, size, name)

#------------------------------------------------------------
    def get_local_checksum(self, filepath):
        """
        Compute crc32 checksum for a local file
        """
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
    def get_query(self, fullpath_pattern, recurse=False):
        """
        Query helper function
        """
        if self.namespace_exists(fullpath_pattern):
            if recurse is True:
# NEW - reworked for better perf
                query = ":namespace '%s'" % fullpath_pattern
            else:
                query = ":where \"namespace='%s'\"" % fullpath_pattern
        else:
            pattern = posixpath.basename(fullpath_pattern)
            namespace = posixpath.dirname(fullpath_pattern)
            query = ":where \"namespace='%s' and name='%s'\"" % (namespace, pattern)

        return(query)

#------------------------------------------------------------
# TODO - more fine-grained access (eg downloadable with password)
    def publish(self, fullpath_pattern):
        """
        For all assets that match the pattern, generate publicly downloadable URLs
        """
        try:
            query = self.get_query(fullpath_pattern, recurse=True)
            reply = self.aterm_run('asset.query %s :count true :action pipe :service -name asset.label.add < :label PUBLISHED >' % query, background=True)
            elem = reply.find(".//count")
            return(int(elem.text))
        except Exception as e:
            self.logging.debug(str(e))
        return 0

#------------------------------------------------------------
    def unpublish(self, fullpath_pattern):
        """
        For all assets that match the pattern, remove and publicly downloadable URLs
        """
        try:
            query = self.get_query(fullpath_pattern, recurse=True)
            reply = self.aterm_run('asset.query %s :count true :action pipe :service -name asset.label.remove < :label PUBLISHED >' % query, background=True)
            elem = reply.find(".//count")
            return(int(elem.text))
        except Exception as e:
            self.logging.debug(str(e))
        return 0

#------------------------------------------------------------
    def get_iter(self, fullpath_pattern):
        """
        Creates an iterator for get() file candidates based on an input pattern

        Args:
            fullpath_pattern: a STRING giving the search pattern for files

        Returns:
            First - the total file count that matched the pattern
            Second - the total bytes of all the files that were matched
            Thereafter - the file names of all the matches
        """
        try:
# count download results and get total size
            query = self.get_query(fullpath_pattern, recurse=True)
# get the number of results and total size
            reply = self.aterm_run('asset.query %s :count true :action sum :xpath content/size' % query, background=True, show_progress=True)
            elem = reply.find(".//value")
# must return valid ints (NB: mflux will return empty space rather than 0 if no query match)
            total_bytes = int(elem.text)
            total_count = int(elem.attrib['nbe'])
            yield total_count
            yield total_bytes
        except Exception as e:
            self.logging.debug(str(e))
            yield 0
            yield 0
            return

# get the file list as an iterator
        result = self.aterm_run('asset.query %s :as iterator :action get-path' % query)
        elem = result.find(".//iterator")
        iterator = elem.text
# effectively the recall batch size
        iterate_size = 100
        iterate = True
        count = 0

        while iterate:
# get file list for this sub-set
            result = self.aterm_run("asset.query.iterate :id %s :size %d" % (iterator, iterate_size))
            for elem in result.findall(".//path"):
                count += 1
                yield elem.text
# terminate when we get complete=True in the results
            elem = result.find(".//iterated")
            if elem is not None:
                if 'true' in elem.attrib['complete']:
                    iterate = False

#------------------------------------------------------------
    def _wait_until_online(self, remote_filepath):
        xml_reply = self.aterm_run('asset.content.migrate :destination online :id "path=%s"' % remote_filepath, background=True)

        while self.enable_polling:
            try:
                xml_reply = self.aterm_run('asset.content.status :id "path=%s"' % remote_filepath, background=True)
                elem = xml_reply.find(".//asset/state")

                if elem is None:
                    self.logging.error("No content found for asset")
                    return False

                if "online" in elem.text:
                    return True

# external (Versity) S3 workaround - we can't see the state or make a recall (S3 proto) all we can do is proceed and let it trigger a recall
                if "reachable" in elem.text:
# try grabbing some bytes in the background ... when it returns it should be ready 
                    xml_reply = self.aterm_run('asset.content.hexdump :id "path=%s" :length 1' % remote_filepath, background=True)
                    return True

            except Exception as e:
                self.logging.error(str(e))
                return False

            time.sleep(5)

        return False

#------------------------------------------------------------
    def get(self, remote_filepath, local_filepath=None, cb_progress=None, overwrite=False):
        """
        Download a remote file to the current working directory

        Args:
            filepath: a STRING representing the full path and filename of the remote file
            local_filepath: a STRING representing the local destination for the download
            cb_progress: a FUNCTION which may be repeatedly called with a single argument for the number of bytes (non-cummulative) successfully recieved 
            overwrite: a BOOLEAN indicating the action to take if a local copy already exists

        Returns:
            0 on success or -1 if the file was skipped 

        Raises:
            An error on failure
        """

        if local_filepath is None:
            local_filepath = os.path.join(os.getcwd(), posixpath.basename(remote_filepath))
        self.logging.info("Downloading remote [%s] to local [%s]" % (remote_filepath, local_filepath))

        if os.path.isfile(local_filepath) and not overwrite:
            self.logging.debug("Local file of that name already exists, skipping.")
            cb_progress(os.path.getsize(local_filepath))
            return(-1)
        else:
# Windows path names and the posix lexer in aterm_run() are not good friends
            if "Windows" in platform.system():
                local_filepath = local_filepath.replace("\\", "\\\\")

# make any intermediate folders required ...
            local_parent = os.path.dirname(local_filepath)
            if os.path.exists(local_parent) is False:
                self.logging.debug("Creating required local folder(s): [%s]" % local_parent)
                os.makedirs(local_parent, exist_ok=True)

# download only when file is online 
            if self._wait_until_online(remote_filepath) is True:
                self.logging.debug("Downloading [%s] ... " % remote_filepath)
#                self.aterm_run('asset.get :id "path=%s" :out %s' % (remote_filepath, local_filepath))
                xml_reply = self.aterm_run('asset.get :id "path=%s"' % remote_filepath)
                elem = xml_reply.find(".//asset")
                asset_id = elem.attrib['id']
#                print("asset id = %r" % asset_id)

# NEW - progress enabled ...
                url = self.data_get + "?_skey={0}&id={1}".format(self.session, asset_id)
                request = urllib.request.Request(url)
                response = urllib.request.urlopen(request)

# buffered write to open file
                with open(local_filepath, 'wb') as output:
# NEW
                    while self.enable_polling:
                        data = response.read(self.get_buffer)
                        if data:
                            output.write(data)
                            if cb_progress is not None:
                                cb_progress(len(data))
                        else:
                            return(0)
            else:
                raise Exception("Online recall failed for: %s" % remote_filepath)

# NEW - should only occur if polling was turned off
        raise Exception("Download failed for: %s" % remote_filepath)

#------------------------------------------------------------
    def put(self, namespace, filepath, cb_progress=None, metadata=False, overwrite=True):
        """
        Creates a new asset on the Mediaflux server and uploads from a local filepath to supply its content

        Args:
            namespace: a STRING representing the remote destination in which to create the asset
             filepath: a STRING giving the absolute path and name of the local file
          cb_progress: a FUNCTION which may be repeatedly called with a single argument for the number of bytes (non-cummulative) successfully sent 
             metadata: a BOOLEAN indicating that filepath plus ".meta" should contain metadata
            overwrite: a BOOLEAN indicating action if remote copy exists

        Returns:
            0 on success or -1 if the file was skipped 

        Raises:
            An error message if unsuccessful
        """

# construct destination argument
        filename = os.path.basename(filepath)
        filename = self._xml_sanitise(filename)
        namespace = self._xml_sanitise(namespace)
        remotepath = posixpath.join(namespace, filename)

# find asset ID if exists, else create (will have empty content)
        result = self.aterm_run('asset.get :id -only-if-exists true "path=%s" :xpath -ename id id :xpath -ename crc32 content/csum :xpath -ename size content/size' % remotepath)
        xml_id = result.find(".//id")
        if xml_id is None:
            self.logging.debug("No remote file found: [%s]" % remotepath)
# NB: must create intermediate directories if they don't exist (mediaflux won't do it by default)
            reply = self.aterm_run('asset.create :namespace -create "true" %s :name %s' % (namespace, filename))
            xml_id = reply.find(".//id")
# didn't exist - so enforce writing
            overwrite=True
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
                self.logging.debug("Match; skipping [%s] -> [%s]" % (filepath, remotepath))
                return(-1)
            else:
                self.logging.debug("Mismatch; local=%r -> remote=%r" % (local_size, remote_size))

# if overwrite - do the upload
        asset_id = int(xml_id.text)
        if overwrite is True:
            self.logging.debug("Uploading asset=%d: [%s] -> [%s]" % (asset_id, filepath, remotepath))
# NB: create=true to generate intermediate directories (if needed)
            xml_string = '<request><service name="service.execute" session="%s"><args><service name="asset.set">' % self.session
            xml_string += '<id>path=%s</id><create>true</create></service></args></service></request>' % remotepath
            asset_id = self._post_multipart_buffered(xml_string, filepath, cb_progress=cb_progress)
            if metadata is True:
                self.import_metadata(asset_id, filepath + ".meta")

        return(0)

#------------------------------------------------------------
    def copy(self, from_fullpath, to_host, to_fullpath, cb_progress=None):

        self.logging.debug("copy [%s] -> s3 [%s] : [%s]" % (from_fullpath, to_host, to_fullpath))

# TODO - when arcitecta implement the new BUCKET argument in the S3 URL ... have to rework slightly ... with tests
        to_url = 's3://%s/%s' % (to_host, to_fullpath)

# ordinary copy
#        cmd = 'asset.export :id "path=%s" :include-namespaces False :url -create True %s' % (from_fullpath, to_url)
# recall first, then copy
        cmd = 'asset.preparation.request.create :id "path=%s" :migrate online :asset-service -name asset.export < :include-namespaces False :url -create True %s >' % (from_fullpath, to_url)

#        print("CMD: [%s]" % cmd)

# main call (async)
        xml = self.aterm_run(cmd)
        elem = xml.find(".//id")
        prep_id = int(elem.text)

# wait until complete ...
        self.logging.debug("Polling asset preparation request id=%d" % prep_id)
        try:
            while True:
                xml = self.aterm_run('asset.preparation.request.describe :id %d' % prep_id)
                time.sleep(5)
        except Exception as e:
# NB: it's an error (exception) when the task is done, since it no longer exists ... even though it isn't really
            pass

# update byte progress to indicate we completed the whole thing 
# FIXME - better way than just telling it the source file size???
        if cb_progress is not None:
            xml = self.aterm_run('asset.get :id "path=%s" :xpath -ename size content/size' % from_fullpath)
            elem = xml.find(".//size")
            if elem is not None:
                size = int(elem.text)
                cb_progress(size)

        return(0)

#------------------------------------------------------------
    def copy_host_setup(self, to_host, to_remote=None):
        """
        check if the to_host is a properly configured s3 endpoint
        TODO - attempt to use to_remote class to configure if not, otherwise fail
        raises exception if to_host is not a valid endpoint for the copy
        """

# MAYBE? run a to_remote.bucket_exists() check -> verify if S3 and bucket exists ...
        xml = self.aterm_run("s3.client.host.exists :host %s" % to_host)
        if self._xml_xpath_boolean(".//exists", xml) is True:
            self.logging.info("s3.client.host [%s] exists" % to_host)
            return
        else:
            if to_remote is not None:
                self.logging.info("TODO - setup s3.client.host: name=%s, url=%s, access=%s" % (to_host, to_remote.url, to_remote.access))
            else:
                self.logging.error("Failed to locate or configure s3 host: [%s]" % to_host)

        raise Exception("Failed to setup S3 host [%s]" % to_host)

#------------------------------------------------------------
# return what the remote fullpath to the file should be given 
# from_pattern (what the user specified)
# from_fullpath (a file that was determined to match from_pattern - ie generated by a list operation)
# remote_root (the root to prepend to the remote fullpath 
    def copy_fullpath_get(self, from_pattern, from_fullpath, remote_root):
#        print("[%s][%s][%s]" % (from_pattern, from_fullpath, remote_root))
        fp = PurePath(from_pattern).parent
        rp = PurePath(from_fullpath).parent
#        print("[%s][%s]" % (str(fp), str(rp)))
        lp = rp.relative_to(fp)
        output = PurePath(remote_root, lp)
#        print("[%s]" % str(output))
        return(str(output))

#------------------------------------------------------------
    def copy_iter(self, from_pattern, to_root):
        """
        Produce iterator for a from_pattern copy source and to_root as destination parent
        """
        self.logging.info("copy: [%s] -> root=[%s] " % (from_pattern, to_root))
        item_list = self.get_iter(from_pattern)
        count = int(next(item_list))
        size = int(next(item_list))
        yield count
        yield size
        for item in item_list:
            remote_fullpath = self.copy_fullpath_get(from_pattern, item, to_root)
            yield item, remote_fullpath

#------------------------------------------------------------
    def xml_to_mf(self, xml_root, result=None):
        """
        Convert XML document to mediaflux shorthand XML markup
        """
        if xml_root is not None:
            if xml_root.text is not None:
                result = " :%s %s" % (xml_root.tag, xml_root.text)
            else:
                result = " :%s <" % xml_root.tag
                for xml_child in xml_root:
                    if xml_child.text is not None:
                        result += " :%s %s" % (xml_child.tag, xml_child.text)
                    else:
                        result += self.xml_to_mf(xml_child, result)
                result += " >"
        return result

#------------------------------------------------------------
    def import_metadata(self, asset_id, filepath):
        """
        populate metadata for an asset using INI style file
        the section is the xml document namespace and options are flat element node + values
        """

        self.logging.debug("import_metadata() [asset ID=%s] : [%s]" % (asset_id, filepath))
        if os.path.isfile(filepath) is False:
            self.logging.warning("Missing metadata file: %s" % filepath)
            return

        try:
            config = configparser.ConfigParser()
            config.read(filepath)
# section -> xmlns
            xml_root = ET.Element(None)
            for section in config.sections():
                xml_child = ET.SubElement(xml_root, section)
                for option in config.options(section):
                    elem_list = option.split('/')
                    xml_item = xml_child
                    xpath = './'
# create any/all intermediate XML nodes in the xpath or merge with existing
                    for elem in elem_list:
                        xpath += "/%s" % elem
                        match = xml_root.find(xpath)
                        if match is not None:
                            xml_item = match
                            continue
                        xml_item = ET.SubElement(xml_item, elem)
# terminate at the final element to populate with the current option data
                    if xml_item is not None:
                        xml_item.text = config.get(section, option)
# DEBUG
#            self.xml_print(xml_root)

# construct the command
            xml_command = 'asset.set :id %d' % asset_id
            for xml_child in xml_root:
                if xml_child.tag == 'asset':
                    for xml_arg in xml_child:
                        xml_command += self.xml_to_mf(xml_arg)
                else:
                    xml_command += ' :meta <%s >' % self.xml_to_mf(xml_child)

# update the asset metadata
            self.aterm_run(xml_command)
# re-analyze the content - stricly only needs to be done if type/ctype/lctype was changed
# NEW - don't do this by default - it will generate stacktrace in mediaflux for DMF (offline) files
#            self.mf_client.aterm_run("asset.reanalyze :id %r" % asset_id)

        except Exception as e:
            self.logging.warning("Metadata population failed: %s" % str(e))


