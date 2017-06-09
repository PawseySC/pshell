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

################################################
# serverless aterm style XML serialisation tests
################################################
class mfclient_syntax(unittest.TestCase):

    def setUp(self):
        global mf_client
        self.mf_client = mf_client

    def test_asset_get(self):
        line = 'asset.get :id 123 :format extended'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<id>123</id><format>extended</format>')

    def test_actor_grant(self):
        line = 'actor.grant :perm < :access access :resource -type service asset.* > :name request-review :type role'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<perm><access>access</access><resource type="service">asset.*</resource></perm><name>request-review</name><type>role</type>')

    def test_acl_grant(self):
        line = 'asset.namespace.acl.grant :namespace /www :acl < :actor -type user "public:public" :access < :namespace access :asset access > >'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<namespace>/www</namespace><acl><actor type="user">public:public</actor><access><namespace>access</namespace><asset>access</asset></access></acl>')

    def test_asset_query(self):
        line = 'asset.query :where "namespace>=/www" :action pipe :service -name asset.label.add < :label "PUBLISHED" >'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<where>namespace&gt;=/www</where><action>pipe</action><service name="asset.label.add"><label>PUBLISHED</label></service>')

    def test_empty_property_value(self):
        reply = self.mf_client.aterm_run(r'dummy.call :element -property " " :element2 text', post=False)
        self.assertEqual(reply, '<element property=" "></element><element2>text</element2>')

# FIXME - in practise, might have to escape the [] chars as they are special in TCL (but this should be done internal to the string itself)
    def test_service_add(self):
        line = 'system.service.add :name custom.service :replace-if-exists true :access ACCESS :definition < :element -name arg1 -type string :element -name arg2 -type string -min-occurs 0 -default " " :element -name arg3 -type boolean -min-occurs 0 -default false > :execute \"return [xvalue result [asset.script.execute :id 1 :arg -name namespace [xvalue namespace $args] :arg -name page [xvalue page $args] :arg -name recurse [xvalue recurse $args]]]\"'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<name>custom.service</name><replace-if-exists>true</replace-if-exists><access>ACCESS</access><definition><element name="arg1" type="string"></element><element default=" " min-occurs="0" name="arg2" type="string"></element><element default="false" min-occurs="0" name="arg3" type="boolean"></element></definition><execute>return [xvalue result [asset.script.execute :id 1 :arg -name namespace [xvalue namespace $args] :arg -name page [xvalue page $args] :arg -name recurse [xvalue recurse $args]]]</execute>')

    def test_semicolon_value(self):
        line = 'actor.grant :name public:public :type user :role -type role read-only'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, '<name>public:public</name><type>user</type><role type="role">read-only</role>')

    def test_whitespace_text(self):
        line = 'asset.namespace.rename :name test3 :namespace /projects/Data Team/sean/test2 :id 123'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, "<name>test3</name><namespace>/projects/Data Team/sean/test2</namespace><id>123</id>")

    def test_quoted_query(self):
        line = "asset.query :where \"namespace='/www' and name='system-alert'\" :action get-name"
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(reply, "<where>namespace='/www' and name='system-alert'</where><action>get-name</action>")

    def test_sanitise_asset(self):
# as out input string is using double quotes - have to test escaped double quotes separately (see next test)
        tmp_name = r"asset_^#%-&{}<>[]()*? $!':;,.@+`|=~1234567890\\"
        reply = self.mf_client.aterm_run('asset.create :namespace /projects/Data Team :name "%s"' % tmp_name, post=False)
        self.assertEqual(reply, "<namespace>/projects/Data Team</namespace><name>asset_^#%-&amp;{}&lt;&gt;[]()*? $!':;,.@+`|=~1234567890\\</name>")

    def test_sanitise_asset_quotes(self):
        reply = self.mf_client.aterm_run(r'asset.create :namespace /projects/Data Team :name "sean\"file"', post=False)
        self.assertEqual(reply, '<namespace>/projects/Data Team</namespace><name>sean"file</name>')

    def test_sanitise_namespace(self):
        tmp_remote = r"/projects/Data Team/namespace_^#%-&{}<>[]()*? $!':;,.@+`|=~1234567890\\"
        reply = self.mf_client.aterm_run('asset.namespace.create :namespace "%s" :quota < :allocation "10 TB" >' % tmp_remote, post=False)
        self.assertEqual(reply, "<namespace>/projects/Data Team/namespace_^#%-&amp;{}&lt;&gt;[]()*? $!':;,.@+`|=~1234567890\\</namespace><quota><allocation>10 TB</allocation></quota>")

    def test_sanitise_namespace_quotes(self):
        reply = self.mf_client.aterm_run(r'asset.namespace.create :namespace "namespace_\"" :quota < :allocation "10 TB" >', post=False)
        self.assertEqual(reply, '<namespace>namespace_"</namespace><quota><allocation>10 TB</allocation></quota>')

    def test_www_list_sanitise(self):
        reply = self.mf_client.aterm_run('www.list :namespace "/projects/Data Team/sean\'s dir" :page 1 :size 30', post=False)
        self.assertEqual(reply, "<namespace>/projects/Data Team/sean's dir</namespace><page>1</page><size>30</size>")


########################################
# convenience wrapper for squishing bugs
########################################
class mfclient_bugs(unittest.TestCase):
    def setUp(self):
        global mf_client
        self.mf_client = mf_client

    def test_squish(self):
        self.mf_client.debug = True
        self.mf_client.debug_level = 1

        reply = self.mf_client.aterm_run(r'asset.namespace.create :namespace "namespace_\"" :quota < :allocation "10 TB" >', post=False)
        self.assertEqual(reply, '<namespace>namespace_"</namespace><quota><allocation>10 TB</allocation></quota>')

        print reply


######
# main
######
if __name__ == '__main__':

# acquire a dummy (non-connected/authenticated) client instance
    try:
        mf_client = mfclient.mf_client("http", "80", "localhost", dummy=True)
        print "\n----------------------------------------------------------------------"
        print "Running offline tests for: mfclient module"
        print "----------------------------------------------------------------------\n"
    except Exception as e:
        print str(e)
        exit(-1)


# classes to test
    test_class_list = [mfclient_syntax]

#    test_class_list = [mfclient_bugs]


# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

