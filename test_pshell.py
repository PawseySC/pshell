#!/usr/bin/python

import os
import sys
import unittest
import subprocess
import xml.etree.ElementTree as ET
from subprocess import Popen, PIPE, STDOUT

#######################
# pshell commands tests
#######################
class pshell_syntax(unittest.TestCase):
    def setUp(self):
        global session, server, script, verbosity
        self.session = session
        self.server = server
        self.script = script
        self.verbosity = verbosity

# --
    def test_cd(self):
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "cd /www"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "Remote: /www" in line:
                flag = True
        self.assertTrue(flag)
# --
    def test_ls(self):
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "ls /www"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "index.html" in line:
                flag = True
        self.assertTrue(flag)
# -- 
    def test_mkdir_rmdir(self):
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "mkdir /test && cd /test && pwd"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            self.assertNotIn("Folder already exists", line)
            if "Remote: /test" in line:
                flag = True
        self.assertTrue(flag)
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "rmdir /test"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            self.assertNotIn("Could not find remote folder", line)
        flag=False
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "cd /test"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            if "Could not find remote folder" in line:
                flag=True
        self.assertTrue(flag)
# -- 
    def test_put(self):
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "put test_pshell.py"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "Completed" in line:
                flag = True
        self.assertTrue(flag)
        flag = False
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "ls"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            if "test_pshell.py" in line:
                flag = True
        self.assertTrue(flag)
        Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "rm test_pshell.py"], stdout=PIPE, stderr=STDOUT)
# -- 
    def test_get(self):
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "lls"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            self.assertNotIn("index.html", line)
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "get /www/index.html"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "Completed at" in line:
                flag = True
        self.assertTrue(flag)
        os.remove("index.html")
# -- 
    def test_rm(self):
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "asset.create :namespace / :name test.txt"], stdout=PIPE, stderr=STDOUT)
        flag=False
        for line in p.stdout:
            if "id=" in line:
                flag=True
        self.assertTrue(flag)
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "rm /test.txt"], stdout=PIPE, stderr=STDOUT)
# -- 
    def test_file(self):
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "file /www/index.html"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "size" in line:
                flag = True
        self.assertTrue(flag)
# --
    def test_lls(self):
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "lls"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "test_pshell.py" in line:
                flag = True
        self.assertTrue(flag)


########################################
# convenience wrapper for squishing bugs
########################################
class pshell_bugs(unittest.TestCase):
    def setUp(self):
        global session, server, script, verbosity

        self.session = session
        self.server = server
        self.script = script
        self.verbosity = verbosity

    def test_template(self):
        p = Popen(["python", self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "ls /www"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "index.html" in line:
                flag = True
        self.assertTrue(flag)



######
######
# main
######
if __name__ == '__main__':

    global session, server, script, verbosity

    print("\n----------------------------------------------------------------------")
    print("Running tests for: pshell")
    print("----------------------------------------------------------------------\n")

# establish a session for live tests (intended for fresh install running in a local container)
    session = None
    server = "http://0.0.0.0:80"
# NB: don't use pshell as we can't run bundle_pshell in the container
    script = "pshell.py"
    verbosity = "0"

# class suite to test
    test_class_list = [pshell_syntax]
#    test_class_list = [pshell_bugs]


# setup the session
    p = Popen(["python", script, "-v", verbosity, "-u", server, "system.logon :domain system :user manager :password change_me"], stdout=PIPE)
    for line in p.stdout:
        if "session=" in line:
            result = line.split()
            session = result[0][9:-1]
    if session is None:
        print("Failed to establish mediaflux session with: %s" % server)
        exit(-1)

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

