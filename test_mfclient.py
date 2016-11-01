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

# namespace (folder) tests
	def test_sanitise_namespace(self):
		global namespace

		c = "_^#%-&{}<>[]()*? $!`\":;,.@+`|=~1234567890\\"
		tmp_name = 'namespace' + c
		tmp_remote = posixpath.join(namespace, tmp_name)
		self.mf_client.run("asset.namespace.create", [("namespace", tmp_remote)])
# assert verified creation
		self.assertTrue(self.mf_client.namespace_exists(tmp_remote), "Failed to create [%s] on server" % tmp_remote)
		self.mf_client.run("asset.namespace.destroy", [("namespace", tmp_remote)])
# assert verified destroy
		self.assertFalse(self.mf_client.namespace_exists(tmp_remote), "Failed to destroy [%s] on server" % tmp_remote)

# asset (filename) tests
	def test_sanitise_assets(self):
		global namespace

		c = "_^#%-&{}<>[]()*? $!`\":;,.@+`|=~1234567890\\"
		tmp_name = 'asset' + c
		tmp_remote = posixpath.join(namespace, tmp_name)
		self.mf_client.run("asset.create", [("namespace", namespace), ("name", tmp_name)])
		xml_tree = self.mf_client.run("asset.exists", [("id", "path=%s"%tmp_remote)])
		result = self.mf_client.xml_find(xml_tree, "exists")
# assert verified creation
		self.assertEqual(result.text, "true", "Failed to test existance of [%s] on server" % tmp_remote)
		self.mf_client.run("asset.destroy", [("id", "path=%s"%tmp_remote)])
		xml_tree = self.mf_client.run("asset.exists", [("id", "path=%s"%tmp_remote)])
		result = self.mf_client.xml_find(xml_tree, "exists")
# assert verified destroy
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

############
# fixed bugs
############
class mfclient_fixes(unittest.TestCase):

	def setUp(self):
		global mf_client
		self.mf_client = mf_client

	def test_mimetype_guess(self):
		global namespace
		local_filepath = os.path.realpath(__file__) 
# make a copy of this script and give it an extension that will result in more than one tuple returned for mimetype guess
		local_gzip = os.path.join(os.path.dirname(local_filepath), "test.jpg.gz")
		shutil.copyfile(local_filepath, local_gzip)
# trigger exception if mimetype guess fails
		asset_id = self.mf_client.put(namespace, local_gzip)
# cleanup
		self.mf_client.run("asset.destroy", [("id", asset_id)])
		os.remove(local_gzip)

# make a copy of this script and give it an extension that will result in None for mimetype guess
		local_none = os.path.join(os.path.dirname(local_filepath), "test.x1")
		shutil.copyfile(local_filepath, local_none)
# trigger exception if mimetype guess fails
		asset_id = self.mf_client.put(namespace, local_none)
# cleanup
		self.mf_client.run("asset.destroy", [("id", asset_id)])
		os.remove(local_none)


################################################
# serverless aterm style XML serialisation tests
################################################
class mfclient_aterm_syntax(unittest.TestCase):

	def setUp(self):
		global mf_client
		self.mf_client = mf_client

	def test_aterm_asset_get(self):
		line = 'asset.get :id 123 :format extended'
		reply = self.mf_client._xml_aterm_run(line, post=False)
		self.assertEqual(reply, '<id>123</id><format>extended</format>')

	def test_aterm_actor_grant(self):
		line = 'actor.grant :perm < :access access :resource -type service asset.* > :name request-review :type role'
		reply = self.mf_client._xml_aterm_run(line, post=False)
		self.assertEqual(reply, '<perm><access>access</access><resource type="service">asset.*</resource></perm><name>request-review</name><type>role</type>')

	def test_aterm_asset_namespace_acl_grant(self):
		line = 'asset.namespace.acl.grant :namespace /projects :acl < :actor -type user "system:posix" :access < :namespace access :asset access > >'
		reply = self.mf_client._xml_aterm_run(line, post=False)
		self.assertEqual(reply, '<namespace>/projects</namespace><acl><actor type="user">system:posix</actor><access><namespace>access</namespace><asset>access</asset></access></acl>')

	def test_aterm_asset_query(self):
		line = 'asset.query :where "namespace>=/www" :action pipe :service -name asset.label.add < :label "PUBLISHED" >'
		reply = self.mf_client._xml_aterm_run(line, post=False)
		self.assertEqual(reply, '<where>namespace&gt;=/www</where><action>pipe</action><service name="asset.label.add"><label>PUBLISHED</label></service>')

	def test_aterm_service_add(self):
# it's assuming the last attribute value is the element value
		line = 'system.service.add :name custom.service :replace-if-exists true :access ACCESS :definition < :element -name arg1 -type string :element -name arg2 -type string -min-occurs 0 -default \" \" :element -name arg3 -type boolean -min-occurs 0 -default false > :execute \"return [xvalue result [asset.script.execute :id 1 :arg -name namespace [xvalue namespace $args] :arg -name page [xvalue page $args] :arg -name recurse [xvalue recurse $args]]]\"'
		reply = self.mf_client._xml_aterm_run(line, post=False)
		self.assertEqual(reply, '<name>custom.service</name><replace-if-exists>true</replace-if-exists><access>ACCESS</access><definition><element name="arg1" type="string"></element><element name="arg2" type="string" min-occurs="0" default=" "></element><element name="arg3" type="boolean" min-occurs="0" default="false"></element></definition><execute>return [xvalue result [asset.script.execute :id 1 :arg -name namespace [xvalue namespace $args] :arg -name page [xvalue page $args] :arg -name recurse [xvalue recurse $args]]]</execute>')

	def test_aterm_semicolon_value(self):
		line = 'actor.grant :name public:public :type user :role -type role read-only'
		reply = self.mf_client._xml_aterm_run(line, post=False)
		self.assertEqual(reply, '<name>public:public</name><type>user</type><role type="role">read-only</role>')


######
# main
######
if __name__ == '__main__':

# use config if exists, else create a dummy one
	config = ConfigParser.ConfigParser()
	config_filepath = os.path.join(os.path.expanduser("~"), ".mf_config")
	config_table = config.read(config_filepath)

# NB: name of the section to use for server connection details
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

	print "\n----------------------------------------------------------------------"
	print "Testing against: protocol=%r server=%r port=%r" % (mf_client.protocol, mf_client.server, mf_client.port)
	print "----------------------------------------------------------------------\n"

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
#	test_class_list = [mfclient_service_calls, mfclient_authentication, mfclient_special_characters, mfclient_transfers, mfclient_fixes, mfclient_aterm_syntax]
#	test_class_list = [mfclient_fixes]
#	test_class_list = [mfclient_aterm_syntax]
#	test_class_list = [mfclient_special_characters]
# for when Jeffrey removes the backing store on test... (transfer tests will all fail)
	test_class_list = [mfclient_service_calls, mfclient_authentication, mfclient_special_characters, mfclient_aterm_syntax]


# build suite
	suite_list = []
	for test_class in test_class_list:
		suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
	suite = unittest.TestSuite(suite_list)

# run suite
	unittest.TextTestRunner(verbosity=2).run(suite)

