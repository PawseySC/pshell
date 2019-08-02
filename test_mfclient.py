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

# global mfclient instance to avoid setup for every test class
mf_client = None

################################################
# serverless aterm style XML serialisation tests
################################################
class mfclient_syntax(unittest.TestCase):

    def setUp(self):
        global mf_client
        self.mf_client = mf_client


# --- helper: remove common XML wrapper
    def _peel(self, text):
        prefix = '<request><service name="service.execute" session=""><args>'
        postfix = '</args></service></request>'
        if text.startswith(prefix):
            text = text[len(prefix):]
            text = text[:len(text)-len(postfix)]
        return text


    def test_asset_get(self):
        line = 'asset.get :id 123 :format extended'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.get"><id>123</id><format>extended</format></service>')

    def test_actor_grant(self):
        line = 'actor.grant :perm < :access access :resource -type service asset.* > :name request-review :type role'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(self._peel(reply), '<service name="actor.grant"><perm><access>access</access><resource type="service">asset.*</resource></perm><name>request-review</name><type>role</type></service>')

    def test_acl_grant(self):
        line = 'asset.namespace.acl.grant :namespace /www :acl < :actor -type user "public:public" :access < :namespace access :asset access > >'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.namespace.acl.grant"><namespace>/www</namespace><acl><actor type="user">public:public</actor><access><namespace>access</namespace><asset>access</asset></access></acl></service>')

    def test_asset_query(self):
        line = 'asset.query :where "namespace>=/www" :action pipe :service -name asset.label.add < :label "PUBLISHED" >'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.query"><where>namespace&gt;=/www</where><action>pipe</action><service name="asset.label.add"><label>PUBLISHED</label></service></service>')

    def test_empty_property_value(self):
        reply = self.mf_client.aterm_run(r'dummy.call :element -property " " :element2 text', post=False)
        self.assertEqual(self._peel(reply), '<service name="dummy.call"><element property=" " /><element2>text</element2></service>')

# FIXME - in practise, might have to escape the [] chars as they are special in TCL (but this should be done internal to the string itself)
    def test_service_add(self):
        line = 'system.service.add :name custom.service :replace-if-exists true :access ACCESS :definition < :element -name arg1 -type string :element -name arg2 -type string -min-occurs 0 -default " " :element -name arg3 -type boolean -min-occurs 0 -default false > :execute \"return [xvalue result [asset.script.execute :id 1 :arg -name namespace [xvalue namespace $args] :arg -name page [xvalue page $args] :arg -name recurse [xvalue recurse $args]]]\"'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(self._peel(reply), '<service name="system.service.add"><name>custom.service</name><replace-if-exists>true</replace-if-exists><access>ACCESS</access><definition><element name="arg1" type="string" /><element default=" " min-occurs="0" name="arg2" type="string" /><element default="false" min-occurs="0" name="arg3" type="boolean" /></definition><execute>return [xvalue result [asset.script.execute :id 1 :arg -name namespace [xvalue namespace $args] :arg -name page [xvalue page $args] :arg -name recurse [xvalue recurse $args]]]</execute></service>')

    def test_semicolon_value(self):
        line = 'actor.grant :name public:public :type user :role -type role read-only'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(self._peel(reply), '<service name="actor.grant"><name>public:public</name><type>user</type><role type="role">read-only</role></service>')

    def test_whitespace_text(self):
        line = 'asset.namespace.rename :name test3 :namespace /projects/Data Team/sean/test2 :id 123'
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.namespace.rename"><name>test3</name><namespace>/projects/Data Team/sean/test2</namespace><id>123</id></service>')

    def test_quoted_query(self):
        line = "asset.query :where \"namespace='/www' and name='system-alert'\" :action get-name"
        reply = self.mf_client.aterm_run(line, post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.query"><where>namespace=\'/www\' and name=\'system-alert\'</where><action>get-name</action></service>')

    def test_sanitise_asset(self):
# as out input string is using double quotes - have to test escaped double quotes separately (see next test)
        tmp_name = r"asset_^#%-&{}<>[]()*? $!':;,.@+`|=~1234567890\\"
        reply = self.mf_client.aterm_run('asset.create :namespace /projects/Data Team :name "%s"' % tmp_name, post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.create"><namespace>/projects/Data Team</namespace><name>asset_^#%-&amp;{}&lt;&gt;[]()*? $!\':;,.@+`|=~1234567890\\</name></service>')

    def test_sanitise_asset_quotes(self):
        reply = self.mf_client.aterm_run(r'asset.create :namespace /projects/Data Team :name "sean\"file"', post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.create"><namespace>/projects/Data Team</namespace><name>sean"file</name></service>')

    def test_sanitise_namespace(self):
        tmp_remote = r"/projects/Data Team/namespace_^#%-&{}<>[]()*? $!':;,.@+`|=~13457890\\"
        reply = self.mf_client.aterm_run('asset.namespace.create :namespace "%s" :quota < :allocation "10 TB" >' % tmp_remote, post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.namespace.create"><namespace>/projects/Data Team/namespace_^#%-&amp;{}&lt;&gt;[]()*? $!\':;,.@+`|=~13457890\\</namespace><quota><allocation>10 TB</allocation></quota></service>')

    def test_sanitise_namespace_quotes(self):
        reply = self.mf_client.aterm_run(r'asset.namespace.create :namespace "namespace_\"" :quota < :allocation "10 TB" >', post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.namespace.create"><namespace>namespace_"</namespace><quota><allocation>10 TB</allocation></quota></service>')

    def test_sanitise_www_list(self):
        reply = self.mf_client.aterm_run('www.list :namespace "/projects/Data Team/sean\'s dir" :page 1 :size 30', post=False)
        self.assertEqual(self._peel(reply), '<service name="www.list"><namespace>/projects/Data Team/sean\'s dir</namespace><page>1</page><size>30</size></service>')

    def test_xmlns_parsing(self):
        reply = self.mf_client.aterm_run(r'asset.set :id 123 :meta < :pawsey:custom < :pawsey-key "pawsey value" >', post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.set"><id>123</id><meta><pawsey:custom xmlns:pawsey="pawsey"><pawsey-key>pawsey value</pawsey-key></pawsey:custom></meta></service>')

    def test_negative_not_attribute(self):
        reply = self.mf_client.aterm_run('asset.set :id 123 :geoshape < :point < :latitude -31.95 :longitude 115.86 :elevation 10.0 > >', post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.set"><id>123</id><geoshape><point><latitude>-31.95</latitude><longitude>115.86</longitude><elevation>10.0</elevation></point></geoshape></service>')

    def test_asset_get_out(self):
        reply = self.mf_client.aterm_run('asset.get :id 123 :format extended :out /Users/sean/test123', post=False)
        self.assertEqual(self._peel(reply), '<service name="asset.get" outputs="1"><id>123</id><format>extended</format></service><outputs-via>session</outputs-via>')

    def test_service_execute(self):
        reply = self.mf_client.aterm_run('service.execute :service -name "asset.create" < :namespace "folder" :name "file" > :input-ticket 1', post=False)
        self.assertEqual(reply, '<request><service name="service.execute" session=""><args><service name="asset.create"><namespace>folder</namespace><name>file</name></service><input-ticket>1</input-ticket></args></service></request>')

# regression test for special characters in password
    def test_sanitise_login_password(self):
        password = 'a?|:;(){}[  ]#@$%&* b.,c~123!> </\\\\'
        line = "system.logon :domain ivec :user sean :password %s" % password
        reply = self.mf_client.aterm_run(line, post=False)
#        print reply
        self.assertEqual(reply, '<request><service name="system.logon"><args><domain>ivec</domain><user>sean</user><password>%s</password></args></service></request>' % self.mf_client._xml_sanitise(password))


########################################
# convenience wrapper for squishing bugs
########################################
class mfclient_bugs(unittest.TestCase):
    def setUp(self):
        global mf_client
        self.mf_client = mf_client

# FIXME: this is not a proper test as OUT filename is not returned in the reply by aterm_run()
    def test_windows_path_handling(self):
        self.mf_client.debug=2
# test escaping of windows path separator to avoid the posix lexer in aterm_run() screwing it up
        line = r"asset.get :id 123 :out c:\Users\sean\test.zip"
        line = line.replace("\\", "\\\\")
        reply = self.mf_client.aterm_run(line, post=False)
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
#    test_class_list = [mfclient_syntax]
    test_class_list = [mfclient_bugs]


# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

