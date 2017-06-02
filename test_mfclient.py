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

################
# special functionality 
################
class mfclient_calls(unittest.TestCase):

    def setUp(self):
        global mf_client
        self.mf_client = mf_client

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

    def test_namespace_exist(self):
        self.assertEqual(self.mf_client.namespace_exists("/projects/Data Team"), True)
        self.assertEqual(self.mf_client.namespace_exists("/idon'texist"), False)

################################################
# serverless aterm style XML serialisation tests
################################################
# a lot of these don't require a MF login/server -> the way of the future for tests??? 
class mfclient_aterm_syntax(unittest.TestCase):

    def setUp(self):
        global mf_client
        self.mf_client = mf_client

    def test_aterm_asset_get(self):
        line = 'asset.get :id 123 :format extended'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<id>123</id><format>extended</format>')

    def test_aterm_actor_grant(self):
        line = 'actor.grant :perm < :access access :resource -type service asset.* > :name request-review :type role'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<perm><access>access</access><resource type="service">asset.*</resource></perm><name>request-review</name><type>role</type>')

    def test_aterm_acl_grant(self):
        line = 'asset.namespace.acl.grant :namespace /www :acl < :actor -type user "public:public" :access < :namespace access :asset access > >'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<namespace>/www</namespace><acl><actor type="user">public:public</actor><access><namespace>access</namespace><asset>access</asset></access></acl>')

    def test_aterm_asset_query(self):
        line = 'asset.query :where "namespace>=/www" :action pipe :service -name asset.label.add < :label "PUBLISHED" >'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<where>namespace&gt;=/www</where><action>pipe</action><service name="asset.label.add"><label>PUBLISHED</label></service>')

    def test_aterm_service_add(self):
        line = 'system.service.add :name custom.service :replace-if-exists true :access ACCESS :definition < :element -name arg1 -type string :element -name arg2 -type string -min-occurs 0 -default " " :element -name arg3 -type boolean -min-occurs 0 -default false > :execute \"return [xvalue result [asset.script.execute :id 1 :arg -name namespace [xvalue namespace $args] :arg -name page [xvalue page $args] :arg -name recurse [xvalue recurse $args]]]\"'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<name>custom.service</name><replace-if-exists>true</replace-if-exists><access>ACCESS</access><definition><element name="arg1" type="string"></element><element name="arg2" type="string" min-occurs="0" default=" "></element><element name="arg3" type="boolean" min-occurs="0" default="false"></element></definition><execute>return [xvalue result [asset.script.execute :id 1 :arg -name namespace [xvalue namespace $args] :arg -name page [xvalue page $args] :arg -name recurse [xvalue recurse $args]]]</execute>')

    def test_aterm_semicolon_value(self):
        line = 'actor.grant :name public:public :type user :role -type role read-only'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<name>public:public</name><type>user</type><role type="role">read-only</role>')

    def test_aterm_whitespace_text(self):
        line = 'asset.namespace.rename :name test3 :namespace /projects/Data Team/sean/test2'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, "<name>test3</name><namespace>/projects/Data Team/sean/test2</namespace>")

    def test_aterm_quoted_query(self):
        line = "asset.query :where \"namespace='/www' and name='system-alert'\" :action get-name"
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, "<where>namespace='/www' and name='system-alert'</where><action>get-name</action>")

    def test_aterm_sanitise_asset(self):
        c = "_^#%-&{}<>[]()*? $!'\":;,.@+`|=~1234567890\\"
        tmp_name = 'asset' + c

        reply = self.mf_client.aterm_run('asset.create :namespace "%s" :name "%s"' % (namespace, tmp_name), post=False)
        self.assertEqual(reply, "<namespace>/projects/Data Team</namespace><name>asset_^#%-&amp;{}&lt;&gt;[]()*? $!'&quot;:;,.@+`|=~1234567890\</name>")

    def test_aterm_sanitise_namespace(self):
        c = "_^#%-&{}<>[]()*? $!'\":;,.@+`|=~1234567890\\"
        tmp_name = 'namespace' + c
        tmp_remote = posixpath.join(namespace, tmp_name)

        reply = self.mf_client.aterm_run("asset.namespace.create :namespace \"%s\"" % tmp_remote, post=False)
        self.assertEqual(reply, "<namespace>/projects/Data Team/namespace_^#%-&amp;{}&lt;&gt;[]()*? $!'&quot;:;,.@+`|=~1234567890\</namespace>")

        reply = self.mf_client.aterm_run('asset.namespace.create :namespace "%s"' % tmp_remote, post=False)
        self.assertEqual(reply, "<namespace>/projects/Data Team/namespace_^#%-&amp;{}&lt;&gt;[]()*? $!'&quot;:;,.@+`|=~1234567890\</namespace>")

    def test_aterm_www_list_sanitise(self):
        reply = self.mf_client.aterm_run('www.list :namespace "/projects/Data Team/sean\'s dir" :page 1 :size 30', post=False)
        self.assertEqual(reply, "<namespace>/projects/Data Team/sean's dir</namespace><page>1</page><size>30</size>")


# CURRENT - this won't work (line continuations) ... worth fixing?
#    def test_maybe(self):
#        line = 'system.service.add :name "project.describe" :replace-if-exists true :description "Custom project description for the web portal." :access ACCESS :object-meta-access ACCESS \
#                    :definition < :element -name name -type string > \
#                    :execute "return [xvalue result [asset.script.execute :id %d :arg -name name [xvalue name $args]]]"'
#        self.mf_client.aterm_run(line, post=False)


# convenience wrapper for squishing bugs
class mfclient_bugs(unittest.TestCase):
    def setUp(self):
        global mf_client
        self.mf_client = mf_client

    def test_multi(self):
        line = 'asset.get :id 123 :format extended'
        reply = self.mf_client.aterm_run(line, post=False)
        print ""
        print reply


######
# main
######
if __name__ == '__main__':

# get the test server to connect to
# TODO - would be nice to isolate from this requirement in theory but, in practise, difficult
    config = ConfigParser.ConfigParser()
    config_filepath = os.path.join(os.path.expanduser("~"), ".mf_config")
    config_table = config.read(config_filepath)
    current = 'test'

# parse config
    if config.has_section(current):
        server = config.get(current, 'server')
        protocol = config.get(current, 'protocol')
        port = config.get(current, 'port')
        domain = config.get(current, 'domain')
        namespace = config.get(current, 'namespace')
        try:
            session = config.get(current, 'session')
        except:
            pass

# acquire a mediaflux connection
    try:
        mf_client = mfclient.mf_client(config.get(current, 'protocol'), config.get(current, 'port'), config.get(current, 'server'), session=session, enforce_encrypted_login=False)
        print "\n----------------------------------------------------------------------"
        print "Testing against: protocol=%r server=%r port=%r" % (mf_client.protocol, mf_client.server, mf_client.port)
        print "----------------------------------------------------------------------\n"
    except Exception as e:
        print str(e)
        exit(-1)

# re-use existing session or log in again
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
#    test_class_list = [mfclient_authentication]
#    test_class_list = [mfclient_bugs]
#    test_class_list = [mfclient_authentication, mfclient_aterm_syntax, mfclient_calls]

    test_class_list = [mfclient_aterm_syntax, mfclient_calls]

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

