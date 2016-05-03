#!/usr/bin/python

import os
import sys
import time
import shutil
import getpass
import urllib2
import binascii
import unittest
import mfclient
import posixpath
import ConfigParser

# global mfclient instance to avoid logging in for every single test
mf_client = None

##################
# service call API
##################
class mfclient_service_calls(unittest.TestCase):

	def setUp(self):
		global mf_client
		self.mf_client = mf_client

# basic execution of a mediaflux service call with no arguments or attributes
	def test_no_arguments(self):
		result = self.mf_client.run("actor.self.describe")
		found = False
		for elem in result.iter():
			if elem.tag == "actor":
				found = True
		self.assertTrue(found, "Expected to get <actor> element back from actor.self.describe")

# mediaflux service call with argument 
	def test_with_argument(self):
		result = self.mf_client.run("asset.namespace.exists", [("namespace", "/mflux")])
		for elem in result.iter():
			if elem.tag == "exists":
				self.assertEqual(elem.text, "true", "Expected directory /mflux to exist on server")

# service call with arguments and attribute
	def test_arguments_attributes(self):
		result = self.mf_client.run("asset.query", [ ("where", "namespace>='/mflux'"), ("size", "1"), ("action", "get-values"), ("xpath ename=\"code\"", "id") ])
		found = False
		for elem in result.iter():
			if elem.tag == "code":
				found = True
		self.assertTrue(found, "Expected to get <code> element back from asset.query results")


################
# authentication 
################
class mfclient_authentication(unittest.TestCase):

	def setUp(self):
		global mf_client
		self.mf_client = mf_client

# manual login failure 
	def test_manual_login_failure(self):
		try:
			result = self.mf_client.login("ivec", "sean", "badpassword")
			raise Exception("FAIL: login command should not succeed")
		except Exception as e:
			pass

# login failure with token
	def test_token_login_failure(self):
		try:
			result = self.mf_client.login(token="abadtoken")
			raise Exception("FAIL: token login should not succeed")
		except Exception as e:
			pass


################################
# handling of special characters
################################
class mfclient_special_characters(unittest.TestCase):

	def setUp(self):
		global mf_client
		self.mf_client = mf_client

	def test_xml_sanitise_namespace(self):
		global namespace

		tmp_remote = posixpath.join(namespace, 'namespace_<"&>')

		self.mf_client.run("asset.namespace.create", [("namespace", tmp_remote)])
		self.assertTrue(self.mf_client.namespace_exists(tmp_remote), "Failed to create [%s] on server" % tmp_remote)
		self.mf_client.run("asset.namespace.destroy", [("namespace", tmp_remote)])
		self.assertFalse(self.mf_client.namespace_exists(tmp_remote), "Failed to destroy [%s] on server" % tmp_remote)

	def test_xml_sanitise_assets(self):
		global namespace

		tmp_name = 'asset_<"&>'
		tmp_remote = posixpath.join(namespace, tmp_name)

		self.mf_client.run("asset.create", [("namespace", namespace), ("name", tmp_name)])
		xml_tree = self.mf_client.run("asset.exists", [("id", "path=%s"%tmp_remote)])
		result = self.mf_client.xml_find(xml_tree, "exists")
		self.assertEqual(result.text, "true", "Failed to test existance of [%s] on server" % tmp_remote)

		self.mf_client.run("asset.destroy", [("id", "path=%s"%tmp_remote)])

		xml_tree = self.mf_client.run("asset.exists", [("id", "path=%s"%tmp_remote)])
		result = self.mf_client.xml_find(xml_tree, "exists")
		self.assertEqual(result.text, "false", "Failed to cleanup of [%s] on server" % tmp_remote)


################
# data transfers
################
class mfclient_transfers(unittest.TestCase):

	def setUp(self):
		global mf_client
		self.mf_client = mf_client

# NOTE: this test will fail if you auth as system manager - which has root permissions
	def test_put_no_permission(self):
		tmp_remote = "/www"
# local upload
		src_filepath = os.path.realpath(__file__) 
		try:
			asset_id = self.mf_client.put(tmp_remote, src_filepath)
			raise Exception("Expected no permission put() to fail!")
		except Exception as e:
			pass

	def test_put_no_overwrite(self):
		global namespace
		tmp_remote = namespace + "/tmp"
# remote setup
		self.assertFalse(self.mf_client.namespace_exists(tmp_remote), "Temporary namespace already exists on server")
		self.mf_client.run("asset.namespace.create", [("namespace", tmp_remote)])
# local upload
		src_filepath = os.path.realpath(__file__) 
		asset_id = self.mf_client.put(tmp_remote, src_filepath, overwrite=False)
# failure test
		try:
			asset_id = self.mf_client.put(tmp_remote, src_filepath, overwrite=False)
			raise Exception("FAIL: put() should not overwrite!")
		except Exception as e:
			pass
# cleanup
		self.mf_client.run("asset.destroy", [("id", asset_id)])
		self.mf_client.run("asset.namespace.destroy", [("namespace", tmp_remote)])

# TEST - upload this script (we know it exists) then download and compare
	def test_put_checksum(self):
		global namespace
		tmp_local = "/tmp"
		tmp_remote = namespace + "/tmp"
# remote setup
		self.assertFalse(self.mf_client.namespace_exists(tmp_remote), "Temporary namespace already exists on server")
		self.mf_client.run("asset.namespace.create", [("namespace", tmp_remote)])
# local setup
		self.assertTrue(os.path.isdir(tmp_local), "Need a temporary local directory for testing transfers")
		src_filepath = os.path.realpath(__file__) 
# compute local checksums
		buff = open(__file__,'rb').read()
		local_csum = (binascii.crc32(buff) & 0xFFFFFFFF)

		self.assertTrue(os.path.isfile(src_filepath), "Need a local file for testing transfers")
		dest_filepath = os.path.join(tmp_local, os.path.basename(src_filepath))
		self.assertFalse(os.path.isfile(dest_filepath), "Local temporary directory should not already contain file to download")
# upload file 
		asset_id = self.mf_client.put(tmp_remote, src_filepath)
# download file
		self.mf_client.get(int(asset_id), dest_filepath)
		self.assertTrue(os.path.isfile(dest_filepath), "get failed")
# compute/retrieve checksums
		remote_csum = self.mf_client.get_checksum(asset_id)

# assert checksums are identical
		self.assertEqual(int(local_csum), int(remote_csum, 16), "Source file crc32 (%r) does not match after transfers (%r)" % (local_csum, remote_csum))
# cleanup
		self.mf_client.run("asset.destroy", [("id", asset_id)])
		self.mf_client.run("asset.namespace.destroy", [("namespace", tmp_remote)])
		os.remove(dest_filepath)

# TEST - retrieve wget'able URL from the server for a single asset
	def test_wget_url(self):
		global namespace
		local_filepath = os.path.realpath(__file__) 
		remote_namespace = namespace + "/myfiles"
		asset_id = self.mf_client.put(remote_namespace, local_filepath)
		url = self.mf_client.get_url(asset_id)
		req = urllib2.urlopen(url)
		code = req.getcode()
		self.assertEqual(code, 200, "Did not receive OK from server")


######
# main
######
if __name__ == '__main__':

# use config if exists, else create a dummy one
	config = ConfigParser.ConfigParser()
	config_filepath = os.path.join(os.path.expanduser("~"), ".mf_config")
	config_table = config.read(config_filepath)

# IMPORTANT - name of the section to use for server connection details
	current = 'test'

# default config values
	server = 'mediaflux.org.au'
	protocol = 'https'
	port = '443'
	domain = 'home'
# NB: this should be a mediaflux namespace where you're allowed to read/write
	namespace = "/remote/namespace"
	session = None

# parse config
	if not config.has_section(current):
		print "Creating section [%s] in config file: %s" % (current, config_filepath)
		config.add_section(current)
		config.set(current, 'server', server)
		config.set(current, 'protocol', protocol)
		config.set(current, 'port', port)
		config.set(current, 'domain', domain)
		config.set(current, 'namespace', namespace)
		f = open(config_filepath, "w")
		config.write(f)
		f.close()
		print "Please edit config file, then re-run this file"
		exit(0)
	else:
# these must be present and configured
		server = config.get(current, 'server')
		protocol = config.get(current, 'protocol')
		port = config.get(current, 'port')
		domain = config.get(current, 'domain')
		namespace = config.get(current, 'namespace')
# generated again (below) if required
		try:
			session = config.get(current, 'session')
		except:
			pass

# acquire a reusable authenticated mediaflux connection
	mf_client = mfclient.mf_client(config.get(current, 'protocol'), config.get(current, 'port'), config.get(current, 'server'), session=session, enforce_encrypted_login=False)

# re-use existing session 
	if not mf_client.authenticated():
		print "Domain: %s" % domain
		user = raw_input("Username: ")
		password = getpass.getpass("Password: ")
		try:
			mf_client.login(domain=domain, user=user, password=password)
		except Exception as e:
			print str(e)
			exit(-1)
# save session
		config.set(current, 'session', mf_client.session)
		f = open(config_filepath, "w")
		config.write(f)
		f.close()

# classes to test
	test_class_list = [mfclient_service_calls, mfclient_authentication, mfclient_special_characters, mfclient_transfers]

# build suite
	suite_list = []
	for test_class in test_class_list:
		suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
	suite = unittest.TestSuite(suite_list)

# run suite
	print "\n----------------------------------------------------------------------\n"
	unittest.TextTestRunner(verbosity=2).run(suite)

