#!/usr/bin/python

import os
import re
import cmd
import sys
import ssl
import time
import zlib
import shlex
import random
import string
import socket
import signal
import getpass
import inspect
import urllib2
import httplib
import datetime
import functools
import mimetypes
import posixpath
import multiprocessing
import xml.etree.ElementTree as xml_processor

# Python standard lib implementation of a mediaflux client
# Author: Sean Fleming

#------------------------------------------------------------
"""
Globals ... multiprocess IO monitoring is hard
"""
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
# TODO - parse Exception and return condensed error message instead of fail
# NB: return form should be suitable for retry of this transfer primitive
		return ("Fail", data[0], data[1])

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
# NB: return form should be suitable for retry of this transfer primitive
		return ("Fail", data[0], data[1])

	return (0, data[0], data[1])

#########################################################
class mf_client:
	"""
	Base Mediaflux authentication and communication client
	Parallel transfers should be handled with multiprocessing (urllib2 and httplib are not thread-safe)
	All unexpected failures are handled by raising exceptions
	"""

	def __init__(self, protocol, port, server, session="", timeout=120, enforce_encrypted_login=True, debug=False):
		"""
		Create a Mediaflux server connection instance. Raises an exception on failure.

		Args:
			               protocol: a STRING which should be either "http" or "https"
			                   port: a STRING which is usually "80" or "443"
			                 server: a STRING giving the FQDN of the server
			                session: a STRING supplying the session ID which, if it exists, enables re-use of an existing authenticated session 
			                timeout: an INTEGER specifying the connection timeout
			enforce_encrypted_login: a BOOLEAN that should only be False on a safe internal dev/test network
		                          debug: a BOOLEAN which controls output of troubleshooting information 

		Returns:
			A reachable mediaflux server object that has not been tested for its authentication status

		Raises:
			Error if server appears to be unreachable
		"""
# configure interfaces
		self.protocol = protocol
		self.port = int(port)
		self.server = server
		self.timeout = timeout
		self.session = session
		self.debug = debug
		self.base_url="{0}://{1}".format(protocol, server)
		self.post_url= self.base_url + "/__mflux_svc__"
		self.data_url = self.base_url + "/mflux/content.mfjp"
		self.http_lib="{0}:{1}".format(server, self.port)
		self.enforce_encrypted_login = bool(enforce_encrypted_login)
# download/upload buffers
		self.get_buffer=8192
		self.put_buffer=8192
# XML pretty print hack
		self.indent = 0
# check is server is reachable
		s = socket.socket()
		s.settimeout(self.timeout)
		s.connect((self.server, self.port))
		s.close()

# if required, attempt to display more connection info
		if self.debug:
			print "  SERVER: %s://%s:%s" % (self.protocol, self.server, self.port)
			if self.protocol == "https":
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
	def _post(self, xml_string):
		"""
		Primitive for sending an XML message to the Mediaflux server
		"""
# NB: timeout exception if server is unreachable
		request = urllib2.Request(self.post_url, data=xml_string, headers={'Content-Type': 'text/xml'})
		response = urllib2.urlopen(request, timeout=self.timeout)

		xml = response.read()
		tree = xml_processor.fromstring(xml)
		if tree.tag != "response":
			raise Exception("No response from server")

# TODO - skip this step to avoid double parse of the XML?
# this would mean every _post() call would have to do its own handling
		error = self.xml_error(tree)
		if error:
			raise Exception("Error from server: %s" % error)

		return tree

#------------------------------------------------------------
# TODO - revisit this - can use a file descriptor as input (rather than data from memory) so can cope with large files
# TODO - can you control the buffer/chunked file reading?
	def _post_multipart(self, xml, filepath):
		"""
		Primitive file upload method - NOTE - please use post_multipart_buffered instead
		Sends a multipart POST to the server; consisting of the initial XML and a single attached file
		"""
# helper
		def escape_quote(s):
			return s.replace('"', '\\"')

		boundary = ''.join(random.choice(string.digits + string.ascii_letters) for i in range(30))
		lines = []

# service call part (is the name meaningfull to mediaflux?)
       		lines.extend(( '--{0}'.format(boundary), 'Content-Disposition: form-data; name="request"', '', str(xml),))

		filename = os.path.basename(filepath)
		mimetype = mimetypes.guess_type(filepath) or 'application/octet-stream'

		f = open(filepath,'rb')
		content = f.read()

# file data part (are the name, filename values meaningful to mediaflux?)
# actual filename will be the relevant part in the xml string representing the asset.create service call
		lines.extend(( '--{0}'.format(boundary), 'Content-Disposition: form-data; name="request"; filename="{0}"'.format(escape_quote(filename)), 'Content-Type: {0}'.format(mimetype), '', content, ))
		lines.extend(( '--{0}--'.format(boundary), '',))

		body = '\r\n'.join(lines)
		headers = { 'Content-Type': 'multipart/form-data; boundary={0}'.format(boundary), 'Content-Length': str(len(body)), } 

		request = urllib2.Request(self.post_url, data=body, headers=headers)
		r = urllib2.urlopen(request, timeout=self.timeout)
		return r.read()

#------------------------------------------------------------
	def _post_multipart_buffered(self, xml, filepath):
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
       		lines.extend(( '--%s' % boundary, 'Content-Disposition: form-data; name="request"', '', str(xml),))

# CURRENT - adding Jason's suggested form field (1 data file attachment?)
# tested on dev box and does seem to 1) have no mfp created in volatile/tmp ... 2) be a lot faster
       		lines.extend(( '--%s' % boundary, 'Content-Disposition: form-data; name="nb-data-attachments"', '', "1",))
# file
		lines.extend(( '--%s' % boundary, 'Content-Disposition: form-data; name="filename"; filename="%s"' % filename, 'Content-Type: %s' % mimetype, '', '' ))
		body = '\r\n'.join(lines)

# NB - should include everything AFTER the first /r/n after the headers
		total_size = len(body) + os.path.getsize(filepath) + len(boundary) + 8

		infile = open(filepath, 'rb')

# DEBUG
#		print "body size = %r" % len(body)
#		print "file size = %r" % os.path.getsize(filepath)
#		print "term size = %r" % (len(boundary) + 6)
#		print "================="
#		print "Total size = %r" % total_size
#		print "================="

# different connection object for HTTPS vs HTTP
		if self.protocol == 'https':
			conn = httplib.HTTPSConnection(self.http_lib, timeout=self.timeout)
		else:
			conn = httplib.HTTPConnection(self.http_lib, timeout=self.timeout)

# kickoff
		self.log("DEBUG", "[pid=%d] File send starting: %s" % (pid, filepath))
		conn.putrequest('POST', "/__mflux_svc__")
# headers
		conn.putheader('User-Agent', 'Python/2.7')
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

# NOTE - used to get timeouts (on large unknown files) here
# this is less of an issue since added Arcitecta's magic nb attachements flag to the upload
# which meant data went straight to destination rather than tmp area and then moved (delay proportional to size)
# CURRENTLY - for comms with dev VM (ie same machine) get a lot of connection terminated by peer errors
# could this be the mediaflux server prematurely closing the connection due to non-existent network latency???
		message = "response did not contain an asset ID."
		for i in range(0,retry_count):
			try:
				resp = conn.getresponse()
				reply = resp.read()
				conn.close()
				tree = xml_processor.fromstring(reply)

# return asset id of uploaded filed or any (error) message
				for elem in tree.iter():
					if elem.tag == 'id':
						return int(elem.text)
					if elem.tag == 'message':
						message = elem.text
				raise Exception(message)

# re-try if we have a slow server (final ack timeout)
			except socket.timeout:
				self.log("DEBUG", "[pid=%d] No response from server [count=%d] trying again..." % (pid, i))
				time.sleep(self.timeout)

		raise Exception("[pid=%d] Giving up on final server ACK." % pid)

#------------------------------------------------------------
	def _xml_sanitise(self, text):
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
	def _xml_element_attributes_strip(self, element):
		"""
		Helper method to strip an element of any attributes
		"""
		k = element.find(' ')
		if k != -1:
			element_strip = element[:k]
		else:
			element_strip = element

		return element_strip

#------------------------------------------------------------
	def _xml_element_attributes_format(self, element):
		"""
		Helper method to format attributes '-attribute value' -> 'attribute="value"'
		"""

#		print "unformatted: [%s]" % element
		list_attributes = element.split()

# build the element string
		count=0
		for item in list_attributes:
			if count == 0:
				element_string = item
			else:
				if count % 2 == 0:
					element_string += '="%s"' % item
				else:
					element_string += " %s" % item[1:]
			count += 1

#		print "formatted: [%s]" % element_string
		return element_string

#------------------------------------------------------------
	def _xml_expand(self, xml_condensed):
		"""
		Helper method to expand a single element sequence written in Arcitecta's condensed XML format
		Expected input is of the form:
		element -optional1 attribute1 -optional2 attribute2 optional_text_data
		Which is mapped to:
		<element optional1="attribute1" optional2="attribute2">optional_text_data</element>
		"""

		if len(xml_condensed) == 0:
			return xml_condensed

#		print "\n_xml_expand(input): [%s]" % xml_condensed

		match = re.match('^\S+', xml_condensed)
		if match:
			element = match.group(0)
			start = start_data = match.end()
		else:
			raise Exception("Missing element name in: [%s]" % xml_condensed)

		qcount = dqcount = 0
		length = len(xml_condensed)
		start_attrib = start_value = 0
		xml = "<%s" % element

		try:
			for i in range(start, length):
# if any quoted string flags are active - skip any further processing
				if xml_condensed[i] == "'":
					if qcount:
						qcount=0
					else:
						qcount=1
				if xml_condensed[i] == '"':
					if dqcount:
						dqcount=0
					else:
						dqcount=1
				if qcount or dqcount:
					continue
# attribute start
				if xml_condensed[i] == '-' and xml_condensed[i-1] == ' ':
					start_attrib = i
# end of token candidate
				if xml_condensed[i] == ' ':
					if start_attrib:
						attrib = xml_condensed[start_attrib+1:i]
						xml += ' %s' % attrib
						start_value = i
						start_attrib = 0
					elif start_value:
						value = xml_condensed[start_value+1:i]
# enforce only a single surrounding pair of quotes, regardless of whether input has them or not
						xml += '="%s"' % value.strip('"')
						start_value = 0
						start_data = i

			if start_value:
				value = xml_condensed[start_value+1:]
#				print "value = [%s]" % value
				xml += '="%s"></%s>' % (value.strip('"'), element)
			else:
				text = xml_condensed[start_data+1:]
				text = text.strip('"')
				text = self._xml_sanitise(text)
#				print "text = [%s]" % text
				xml += '>%s</%s>' % (text, element)

		except Exception as e:
			self.log("DEBUG", "XML parse error: %s" % str(e))
			raise Exception("Bad command syntax.")

#		print "_xml_expand(output): [%s]\n" % xml

		return xml

#------------------------------------------------------------
	def _xml_request(self, service_call, arguments):
		""" 
		Helper method for constructing the XML request to send to the Mediaflux server

		Args:
			service_call: a STRING representing the Mediaflux service call to run on the server
			   arguments: a LIST of STRING pairs (name, value) representing the service call's arguments
			              Note that attributes should currently be embedded in the name string

		Returns:
			A STRING containing the XML, suitable for sending via post() to the Mediaflux server
		"""
# special case for logon
		if service_call == "system.logon":
			xml = '<request><service name="%s"><args>' % service_call
			tail = '</args></service></request>'
			logon = True
		else:
			xml = '<request><service name="service.execute" session="%s"><args><service name="%s">' % (self.session, service_call)
			tail = '</service></args></service></request>'
			logon = False

# add argument dictionary items
		for key, value in arguments:
# strip any attributes for terminating tag
			key_element = key.split(" ")[0]
			value = self._xml_sanitise(value)
			xml += "<{0}>{1}</{2}>".format(key, value, key_element)
# complete the xml
		xml += tail

# FIXME - very noisy - make it debug level 2?
# don't print a logon XML post -> it might contain a password
		if not logon:
			tmp = re.sub(r'session=[^>]*', 'session="..."', xml)
			self.log("DEBUG", "XML: %s" % tmp)

		return xml

#------------------------------------------------------------
	def _xml_aterm_run(self, aterm_line, post=True):
		""" 
		Method for serializing aterm's compressed XML syntax and sending to the Mediaflux server 

		Args:
		     aterm_line: raw input text that is assumed to be in aterm syntax
			   post: if False will just return the argument part of the serialized XML, if True will post and return reply

		Returns:
			A STRING containing the server reply
		"""

#		print "\ninput: [%s]" % aterm_line

# pull the service call
		match = re.match('^\S+', aterm_line)
		if match:
			service_call = match.group(0)
			argument_start = match.end()
		else:
			raise Exception("Missing service call in: [%s]" % aterm_line)

		qcount = dqcount = 0
		length = len(aterm_line)
		stack=[]
		xml = ""
		chunk = None
		start = argument_start

# process the compressed XML
		try:
			for i in range(argument_start, length):

# if any quoted string flags are active - skip any further processing
				if aterm_line[i] == "'":
					if qcount:
						qcount=0
					else:
						qcount=1
				if aterm_line[i] == '"':
					if dqcount:
						dqcount=0
					else:
						dqcount=1
				if qcount or dqcount:
					continue

# Arcitecta's shorthand root element start
				if (aterm_line[i] == ':' and aterm_line[i-1] == ' '):
					chunk = self._xml_expand(aterm_line[start+1:i-1])
					start = i

# push nested XML element reference
				if aterm_line[i] == '<':
					element = self._xml_element_attributes_format(aterm_line[start+1:i-1])
					chunk = "<%s>" % element
					stack.append(element)
#					print "Push:  [%s]" % element

# pop nested XML element reference
				if aterm_line[i] == '>':
					chunk = self._xml_expand(aterm_line[start+1:i-1])
					element = stack.pop()
					element_noattrib = self._xml_element_attributes_strip(element)
					chunk += "</%s>" % element_noattrib

# accumulate XML chunks
				if chunk is not None:
#					print "Chunk: [%s]" % chunk
					xml += chunk
					chunk = None
					start = i

# final piece (if any)
			chunk = self._xml_expand(aterm_line[start+1:])
			xml += chunk
#			print "Final: [%s]" % chunk

		except Exception as e:
			self.log("DEBUG", "XML parse error: %s" % str(e))
			raise Exception("Bad command syntax.")

#		print "output: [%s]" % xml

# intended for no-session testing (see test_mflcient)
		if post == False:
			return xml

# wrap service call & authentication XML - cross fingers, and POST
# special case for logon 
# TODO - hide/obscure the session as well ...
		if service_call == "system.logon":
			xml = '<request><service name="%s"><args>%s</args></service></request>' % (service_call, xml)
		else:
			xml = '<request><service name="service.execute" session="%s"><args><service name="%s">%s</service></args></service></request>' % (self.session, service_call, xml)
			tmp = re.sub(r'session=[^>]*', 'session="..."', xml)
			self.log("DEBUG", "XML: %s" % tmp)

		reply = self._post(xml)

		return reply

#------------------------------------------------------------
	def _xml_recurse(self, elem):
		"""
		Helper method for traversing XML and generating formatted output
		"""
		attrib_text = ""
		for key,value in elem.attrib.iteritems():
			attrib_text += "%s=%s " % (key,value)

		if len(attrib_text) > 0:
			print ' '*self.indent + '%s = %s    { %s}' % (elem.tag, elem.text, attrib_text)
		else:
			print ' '*self.indent + '%s = %s' % (elem.tag, elem.text)

		self.indent += 4
		for child in elem.getchildren():
			self._xml_recurse(child)
		self.indent -= 4

#------------------------------------------------------------
	def xml_print(self, xml_tree):
		"""
		Helper method for displaying XML nicely, as much as is possible
		"""
		self._xml_recurse(xml_tree)

#------------------------------------------------------------
	def xml_error(self, xml_tree):
		"""
		Helper method for extracting the error message (if any) from a Mediaflux XML server response
		"""
		error=False
		message=None
		for elem in xml_tree.iter():
			if elem.tag == 'error':
				error=True
			if elem.tag == 'message' and error:
				message = elem.text
		return message

#------------------------------------------------------------
	def xml_find(self, xml_tree, tag):
		"""
		XML navigation helper as I couldn't get the built in XML method root.find() to work properly
		"""
		for elem in xml_tree.iter():
			if elem.tag == tag:
				return elem
		return None

#------------------------------------------------------------
	def log(self, prefix, message):
		"""
		Timestamp based message logging.
		"""
		if "DEBUG" in prefix:
			if not self.debug:
				return
		ts = time.time()
		st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
		message = st + " >>> " + message
		print "%8s: %s" % (prefix, message)

#------------------------------------------------------------
	def logout(self):
		"""
		NOTE: system.logoff is currently bugged (mediaflux version 4.3.067) and doesn't actually destroy the session
		"""
		self.run("system.logoff")
		self.session=""

#------------------------------------------------------------
	def login(self, domain=None, user=None, password=None, token=None):
		"""
		Authenticate to the current Mediaflux server and record the session ID on success

		Input:
			domain, user, password: STRINGS specifying user login details
			token: STRING specifying a delegate credential

		Raises:
			An error if authentication fails
		"""
# security check
		if self.protocol != "https":
			if self.enforce_encrypted_login:
				raise Exception("Forbidding unencrypted password post")
			else:
				self.log("DEBUG", "Permitting unencrypted login; I hope you know what you're doing.")

# attempt token authentication first (if supplied)
		if token is not None:
			xml = self._xml_request("system.logon", [("token", token)])
		else:
			xml = self._xml_request("system.logon", [("domain", domain), ("user", user), ("password", password)])

		reply = self._post(xml)
		for elem in reply.iter():
			if elem.tag == 'session':
				self.session=elem.text
				return

		raise Exception("Login failed")

#------------------------------------------------------------
	def authenticated(self):
		"""
		Check client authentication state

		Returns:
			 A BOOLEAN value depending on the current authentication status of the Mediaflux connection
		"""
		try:
			result = self.run("actor.self.describe")
			return True
		except Exception as e:
			self.session = ""
# NB: max licence error can occur here
			self.log("DEBUG", str(e))

		return False

#------------------------------------------------------------
	def delegate(self, lifetime_days=None, token_length=16):
		"""
		Create a secure token that can be used in place of interactive authentication

		Input:
			lifetime_days: an INTEGER specifying lifetime, or None
			token_length: the length of the delegate token to create

		Returns:
			A STRING representing the token

		Raises:
			An error on failure
		"""
# query current authenticated identity
		try:
			result = self.run("actor.self.describe")
			for elem in result.iter():
				if elem.tag == 'actor':
					actor = elem.attrib.get('name', elem.text)
					i = actor.find(":")
					domain = actor[0:i]
					user = actor[i+1:]
		except Exception as e:
# NB: on test server -> max license error will fail here on 1st time setup
			self.log("ERROR", str(e))
			raise Exception("Failed to get valid identity")

# FIXME - mediaflux seems to be ignoring the max-token-length value
# expiry date (if any)
		if lifetime_days is None:
			self.log("DEBUG", "Delegating forever")
			args = [ ("role type=\"user\"", actor), ("role type=\"domain\"", domain), ("max-token-length", token_length) ]
		else:
			d = datetime.datetime.now() + datetime.timedelta(days=lifetime_days)
			expiry = d.strftime("%d-%b-%Y %H:%M:%S")
			self.log("DEBUG", "Delegating until: %s" % expiry)
			args = [ ("to", expiry), ("role type=\"user\"", actor), ("role type=\"domain\"", domain), ("max-token-length", token_length) ]

# create secure token (delegate) and assign current authenticated identity to the token
		result = self.run("secure.identity.token.create", args)
		for elem in result.iter():
			if elem.tag == 'token':
				return elem.text

		raise Exception("Failed to create secure token for current identity")

#------------------------------------------------------------
	def namespace_exists(self, namespace):
		"""
		Wrapper around the generic service call mechanism (for testing namespace existence) that parses the result XML and returns a BOOLEAN
		"""
# NB: this service call requires different escaping compared to an asset.query
		namespace = namespace.replace("'", "\'")
		xml = self._xml_request("asset.namespace.exists", [("namespace", namespace)])
		reply = self._post(xml)

		for elem in reply.iter():
			if elem.tag == "exists":
				if elem.text == "true":
					return True
		return False

#------------------------------------------------------------
	def get_url(self, asset_id):
		"""
		Retrieve a wget'able URL from the server

		Input:
			asset_id: and INTEGER specifying the remote asset

		Returns:
			A STRING representing a URL which contains an authorizing token 

		Raises:
			An error on failure
		"""

		app = "wget"
		tag = "token"
		namespace = None

# find root project namespace
		result = self.run("asset.get", [("id", "%r" % asset_id), ("xpath", "namespace") ])
		elem = self.xml_find(result, "value")
		if elem is not None:
			tmp = elem.text
			match = re.search(r"/projects/[^/]+", tmp)
			if match:
				namespace = match.group(0)
		if namespace is None:
			raise Exception("Failed to find project namespace for asset [%r]" % asset_id)

#		print "namespace: %s" % namespace

# get project token
		result = self.run("asset.namespace.application.settings.get", [("namespace", namespace), ("app", app)])
		elem = self.xml_find(result, tag)
		if elem is not None:
			token = elem.text
		else:
			raise Exception("Failed to retrieve token for project [%s]" % namespace)

# build URL
		url = self.data_url + "?_token=%s&id=%r" % (token, asset_id)

		return url

#------------------------------------------------------------
	def get_local_checksum(self, filepath): 
		current = 0
		with open(filepath, 'rb') as fd:
			while True:
				buffer = fd.read(self.put_buffer)
				if not buffer:
					break
				current = zlib.crc32(buffer, current)
		fd.close()
		return (current & 0xFFFFFFFF)

#------------------------------------------------------------
	def run(self, service_call, argument_tuple_list=[]):
		"""
		Generic mechanism for executing a service call on the current Mediaflux server

		Args:
			       service_call: a STRING representing the named service call
			argument_tuple_list: a LIST of STRING pairs (name, value) supplying the service call arguments
			                     If attributes are required, they must be embedded in the name string

		Returns:
			The XML document response from the Mediaflux server

		Raises:
			An error on failure
		"""
		xml = self._xml_request(service_call, argument_tuple_list)
		reply = self._post(xml)
		return reply

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
# CURRENT - server returns data as disposition attachment regardless of the argument disposition=attachment
#		url = self.data_url + "?_skey={0}&id={1}&disposition=attachment".format(self.session, asset_id)
		url = self.data_url + "?_skey={0}&id={1}".format(self.session, asset_id)
		req = urllib2.urlopen(url)
# distinguish between file data and mediaflux error message
# DEBUG
#		info = req.info()
#		print "get info: %s" % info
#		print "encoding: " , info.getencoding()
#		print "type: " , info.gettype()

# TODO - auto overwrite if different? (CRC)
		if os.path.isfile(filepath) and not overwrite:
			self.log("DEBUG", "Local file of that name (%s) already exists, skipping." % filepath)
# FIXME - this should lower the expected total_bytes by the size of the file ...
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
			result = self._xml_aterm_run('asset.get :id "path=%s" :xpath -ename id id :xpath -ename crc32 content/csum :xpath -ename size content/size' % remotepath)
		except Exception as e:
			self.log("DEBUG", "Not found - creating: [%s]" % remotepath)
			xml_string = '<request><service name="service.execute" session="%s" seq="0"><args><service name="asset.set">' % self.session
			xml_string += '<id>path=%s</id><create>true</create></service></args></service></request>' % remotepath
			asset_id = self._post_multipart_buffered(xml_string, filepath)
			return asset_id

# attempt checksum compare
		try:
			elem = self.xml_find(result, "id")
			asset_id = int(elem.text)
			elem = self.xml_find(result, "crc32")
			remote_crc32 = int(elem.text, 16)
			elem = self.xml_find(result, "size")
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
			raise Exception("Nothing to do")

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
			   function: the primitive transfer METHOD put() or get() to invoke in transfering a single file
			  arguments: a LIST of STRING pairs to be supplied to the transfer function primitive
			  processes: INTEGER number of processes to spawn to deal with the input list 
			total_bytes: INTEGER size of the transfer, for progress reporting

		Returns:
			manager object which can be queried for progress (see methods below) and final status
		"""
		global bytes_sent

# fail if there is already a managed transfer (there can only be one!)
		if not manage_lock.acquire(block=False):
			raise TypeError
# init monitoring
		self.start_time = time.time()

		bytes_sent.value = 0
		bytes_recv.value = 0
		self.summary = []
		self.bytes_total = total_bytes

# CURRENT - ref:http://stackoverflow.com/questions/11312525/catch-ctrlc-sigint-and-exit-multiprocesses-gracefully-in-python 
# force control-C to be ignored by process pool
		handler = signal.signal(signal.SIGINT, signal.SIG_IGN)	
# NB: urllib2 and httplib are not thread safe -> use process pool instead of threads
		self.pool = multiprocessing.Pool(processes)
# restore control-C 
		signal.signal(signal.SIGINT, handler)

		self.task = self.pool.map_async(function, arguments, callback=self.summary.extend)

		self.pool.close()

# use this if exception occurred (eg control-C) during transfer to cleanup process pool
	def cleanup(self):
		"""
		Invoke to properly terminate the process pool (eg if user cancels via control-C)
		"""
		print "\nCleaning up..."
		self.pool.terminate()
		self.pool.join()
		manage_lock.release()

	def remaining(self):
		"""
		Returns the number of transfers still remaining
		"""
		return(self.task._number_left)

	def byte_sent_rate(self):
		"""
		Returns the upload put() transfer rate
		"""
		global bytes_sent
		elapsed = time.time() - self.start_time
		rate = bytes_sent.value / elapsed
		rate /= 1024.0*1024.0
		return rate

	def byte_recv_rate(self):
		"""
		Returns the download get() transfer rate
		"""
		global bytes_recv
		elapsed = time.time() - self.start_time
		rate = bytes_recv.value / elapsed
		rate /= 1024.0*1024.0
		return rate

	def bytes_sent(self):
		"""
		Returns the total bytes sent - accumulated across all processes
		"""
		global bytes_sent
		return bytes_sent.value

	def bytes_recv(self):
		"""
		Returns the total bytes recieved - accumulated across all processes
		"""
		global bytes_recv
		return bytes_recv.value

	def is_done(self):
		"""
		BOOLEAN test for transfer completion
		"""
		if self.task.ready():
			self.pool.join()
			manage_lock.release()
			return True
		return False
