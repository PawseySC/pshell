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
import ConfigParser

# global mfclient instance to avoid logging in for every single test
mf_client = None

class mfclient_test(unittest.TestCase):

	def setUp(self):
		global mf_client

# TODO
# test handling for spaces & other funky chars etc
# proceed with tests on pre-authenticated client 
		self.mf_client = mf_client

# TEST: basic execution of a mediaflux service call with no arguments or attributes
#	@unittest.skip("skip")
	def test_service_call(self):
		print "\n--service call"
		result = self.mf_client.run("actor.self.describe")
#		self.mf_client.xml_print(result)
		found = False
		for elem in result.iter():
			if elem.tag == "actor":
				found = True
		self.assertTrue(found, "Expected to get <actor> element back from actor.self.describe")


# TEST: basic execution of a mediaflux service call with argument 
#	@unittest.skip("skip")
	def test_service_call_with_argument(self):
		print "\n--service call with arguments"
		result = self.mf_client.run("asset.namespace.exists", [("namespace", "/mflux")])
		for elem in result.iter():
			if elem.tag == "exists":
				self.assertEqual(elem.text, "true", "Expected directory /mflux to exist on server")


# TEST - service call with arguments and attribute
#	@unittest.skip("skip")
	def test_service_call_complex(self):
		print "\n--service call with arguments, attribute and special character (ampersand)"
# aterm equivalent = asset.query :where "namespace>='/'" :size 1 :action get-values :xpath "id" -ename "code"
		result = self.mf_client.run("asset.query", [ ("where", "namespace&gt;='/'"), ("size", "1"), ("action", "get-values"), ("xpath ename=\"code\"", "id") ])
#		self.mf_client.xml_print(result)
		found = False
		for elem in result.iter():
			if elem.tag == "code":
				found = True
		self.assertTrue(found, "Expected to get <code> element back from asset.query results")


# TEST - retrieve wget'able URL from the server for a single asset
#	@unittest.skip("skip")
	def test_wget_url(self):
		print "\n--upload and obtain a retrievable URL"

		namespace = "/projects/Data Team"

		local_filepath = os.path.realpath(__file__) 
		remote_namespace = namespace + "/myfiles"

		self.mf_client.log("TEST", "Uploading file [%s] to namespace [%s]" % (local_filepath, remote_namespace))
		asset_id = self.mf_client.put(remote_namespace, local_filepath)

		url = self.mf_client.get_url(asset_id)
		self.mf_client.log("TEST", "Generated URL: \"%s\"" % url)
		req = urllib2.urlopen(url)
		code = req.getcode()
		self.assertEqual(code, 200, "Did not receive OK from server")
		self.mf_client.log("TEST", "Open URL status code: %r (OK)" % code)


# TEST - upload this script (we know it exists) then download and compare
#	@unittest.skip("skip")
	def test_transfers(self):
		print "\n--upload/download with checksum test"
		tmp_local = "/tmp"
		tmp_remote = "/projects/Data Team/tmp"
# remote setup
		self.assertFalse(self.mf_client.namespace_exists(tmp_remote), "Temporary namespace already exists on server")
		self.mf_client.run("asset.namespace.create", [("namespace", tmp_remote)])
# local setup
		self.assertTrue(os.path.isdir(tmp_local), "Need a temporary local directory for testing transfers")
		src_filepath = os.path.realpath(__file__) 
		self.assertTrue(os.path.isfile(src_filepath), "Need a local file for testing transfers")
		dest_filepath = os.path.join(tmp_local, os.path.basename(src_filepath))
		self.assertFalse(os.path.isfile(dest_filepath), "Local temporary directory should not already contain file to download")
# upload file - get returned asset ID and download it
		self.mf_client.log("TEST", "Uploading file [%s] to namespace [%s]" % (src_filepath, tmp_remote))
		asset_id = self.mf_client.put(tmp_remote, src_filepath)
		self.mf_client.get(int(asset_id), dest_filepath)
		self.assertTrue(os.path.isfile(dest_filepath), "Transfer failed")
# compute/retrieve checksums
		buff = open(__file__,'rb').read()
		local_csum = (binascii.crc32(buff) & 0xFFFFFFFF)
		remote_csum = self.mf_client.get_checksum(asset_id)
		self.mf_client.log("TEST", " Local checksum: %0x" % int(local_csum))
		self.mf_client.log("TEST", "Remote checksum: %r" % remote_csum)
# assert checksums are identical
		self.assertEqual(int(local_csum), int(remote_csum, 16), "Source file crc32 (%r) does not match after transfers (%r)" % (local_csum, remote_csum))
# cleanup
		self.mf_client.run("asset.destroy", [("id", asset_id)])
		self.mf_client.run("asset.namespace.destroy", [("namespace", tmp_remote)])
		os.remove(dest_filepath)


# TEST -  managed transfers
	@unittest.skip("skip")
	def test_mp_transfers(self):
		print "\n--managed upload/download"
		tmp_local = "/tmp"
		tmp_remote = "/projects/Data Team/tmp"
		file_count = 100

# setup - NB: we want to FAIL if /tmp exists on mediaflux, so we don't hose it on the off chance it's used
		self.mf_client.run("asset.namespace.create", [("namespace", tmp_remote)])

		self.mf_client.log("TEST", "Uploading %s files to remote namespace [%s]\n" % (file_count, tmp_remote))

# create some files
		total_bytes = 0
		list_namespace_filepath = []
		for i in range(1, file_count):
			filepath = os.path.join(tmp_local, "dummy_%02d" % i)
			shutil.copyfile(os.path.realpath(__file__), filepath)
			list_namespace_filepath.append([tmp_local, filepath])
			total_bytes += os.path.getsize(filepath)

# NB: upload - this will typically happen too fast for progress monitoring
		manager = self.mf_client.put_managed(list_namespace_filepath, total_bytes, processes=4)
		try:
			while True:
				progress = 100.0 * manager.bytes_sent() / float(manager.bytes_total)

				sys.stdout.write("Upload progress: %3.0f%% at %.1f MB/s\r" % (progress, manager.byte_sent_rate()))
				sys.stdout.flush()

				if manager.is_done():
					break
				time.sleep(1)
		except KeyboardInterrupt:
			manager.cleanup()
		print "\n"

# examine the summary list
		self.mf_client.log("TEST", "Downloading %s files to local path [%s]\n" % (file_count, tmp_local))
		list_asset_filepath = []
		for asset_id, namespace, filepath in manager.summary:
# check all uploads were successful (valid integer asset ID returned)
			check_is_integer = int(asset_id)
			list_asset_filepath.append([asset_id, filepath])
			os.remove(filepath)

# NB: download - this will typically happen too fast for progress monitoring
		manager = self.mf_client.get_managed(list_asset_filepath, total_bytes, processes=4)
		try:
			while True:
				progress = 100.0 * manager.bytes_recv() / float(manager.bytes_total)

				sys.stdout.write("Download progress: %3.0f%% at %.1f MB/s\r" % (progress, manager.byte_recv_rate()))
				sys.stdout.flush()

				if manager.is_done():
					break
				time.sleep(1)
		except KeyboardInterrupt:
			manager.cleanup()
		print "\n"

# cleanup - local
		download_size = 0
		for status, asset_id, filepath in manager.summary:
# this checks all downloads were successful (status integer ... will be a string error on failure)
			check_is_integer = int(status)
			download_size += os.path.getsize(filepath)
			os.remove(filepath)
# cleanup - remote
		self.mf_client.run("asset.namespace.destroy", [("namespace", tmp_remote)])
# final check
		self.assertEqual(download_size, total_bytes)


if __name__ == '__main__':
# server config (option heading) to use
#	current = 'dev'
	current = 'test'
#	current = 'pawsey'

# use config if exists, else create a dummy one
	config = ConfigParser.ConfigParser()
	config_home = os.path.expanduser("~")
	config_filepath = os.path.join(config_home, ".mf_config")
	config_table = config.read(config_filepath)

	if not config.has_section(current):
		print "Creating config file: %s" % config_filepath
		config.add_section(current)
		if 'pawsey' in current:
			config.set(current, 'server', 'data.pawsey.org.au')
			config.set(current, 'protocol', 'https')
			config.set(current, 'port', '443')
		elif 'test' in current:
			config.set(current, 'server', '146.118.74.12')
			config.set(current, 'protocol', 'http')
			config.set(current, 'port', '80')
		else:
			print "No information on server: %s" % current
			exit(-1)
# common
		config.set(current, 'domain', 'ivec')
		config.set(current, 'session', '')
		f = open(config_filepath, "w")
		config.write(f)
		f.close()

# acquire a reusable authenticated mediaflux connection
	mf_client = mfclient.mf_client(config.get(current, 'protocol'), config.get(current, 'port'), config.get(current, 'server'), debug=False, enforce_encrypted_login=False)

# re-use existing delegate 
	if config.has_option(current, 'token'):

		mf_client.log("TEST", "Re-using delegate")
		token = config.get(current, 'token')
		try:
			mf_client.login(token=config.get(current, 'token'))
		except:
			print "Error, bad token: %r" % token
			exit(-1)
	else:
		mf_client.log("TEST", "First time setup")
		print "Domain: %s" % config.get(current, 'domain')
		user = raw_input("Username: ")
		password = getpass.getpass("Password: ")
		mf_client.login(domain=config.get(current, 'domain'), user=user, password=password)
# create delegate and save
		mf_client.log("TEST", "Creating re-usable delegate")
		token = mf_client.delegate()
		config.set(current, 'token', token)
		f = open(config_filepath, "w")
		config.write(f)
		f.close()

# run tests
	unittest.main()
