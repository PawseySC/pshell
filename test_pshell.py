#!/usr/bin/env python3

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
        self.python = "python3"

# --
    def test_cd(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "cd /www"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "Remote: /www" in line.decode():
                flag = True
        self.assertTrue(flag)
# --
    def test_ls(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "ls /www"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "index.html" in line.decode():
                flag = True
        self.assertTrue(flag)
# -- 
    def test_mkdir_rmdir(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "mkdir /test && cd /test && pwd"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            self.assertNotIn("Folder already exists", line.decode())
            if "Remote: /test" in line.decode():
                flag = True
        self.assertTrue(flag)
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "rmdir /test"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            self.assertNotIn("Could not find remote folder", line.decode())
        flag=False
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "cd /test"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            if "Could not find remote folder" in line.decode():
                flag=True
        self.assertTrue(flag)
# -- 
    def test_put(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "put test_pshell.py"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            print(line)
            if "Completed" in line.decode():
                flag = True
        self.assertTrue(flag)
        flag = False
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "ls"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            if "test_pshell.py" in line.decode():
                flag = True
        self.assertTrue(flag)
        Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "rm test_pshell.py"], stdout=PIPE, stderr=STDOUT)
# -- 
    def test_get(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "lls"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            self.assertNotIn("index.html", line.decode())
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "get /www/index.html"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "Completed at" in line.decode():
                flag = True
        self.assertTrue(flag)
        os.remove("index.html")
# -- 
    def test_rm(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "asset.create :namespace / :name test.txt"], stdout=PIPE, stderr=STDOUT)
        flag=False
        for line in p.stdout:
            if "id=" in line.decode():
                flag=True

        self.assertTrue(flag)
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "rm /test.txt"], stdout=PIPE, stderr=STDOUT)
# -- 
    def test_file(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "file /www/index.html"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "size" in line.decode():
                flag = True
        self.assertTrue(flag)
# --
    def test_lls(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "lls"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "test_pshell.py" in line.decode():
                flag = True
        self.assertTrue(flag)


########################################
# wrapper for squishing bugs
########################################
class pshell_bugs(unittest.TestCase):
    def setUp(self):
        global session, server, script, verbosity
        self.session = session
        self.server = server
        self.script = script
        self.verbosity = verbosity
        self.python = "python3"

    def test_template(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, "ls /www"], stdout=PIPE, stderr=STDOUT)
        flag = False
        for line in p.stdout:
            if "index.html" in line:
                flag = True
        self.assertTrue(flag)

########################################
# wrapper for new features 
########################################
class pshell_features(unittest.TestCase):
    def setUp(self):
        global session, server, script, verbosity
        self.session = session
        self.server = server
        self.script = script
        self.verbosity = "1"
        self.python = "python3"

    def test_get_iter(self):

        command = 'get scripts/*.tcl'
        p = Popen([self.python, self.script, "-v", self.verbosity, "-u", self.server, "-s", self.session, command], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            print(line)



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
    test_class_list = [pshell_features]
#    test_class_list = [pshell_syntax]
#    test_class_list = [pshell_bugs]


# setup the session
    p = Popen(["python3", script, "-v", verbosity, "-u", server, "system.logon :domain system :user manager :password change_me"], stdout=PIPE)
    for line in p.stdout:
        if "session=" in line.decode():
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

