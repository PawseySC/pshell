#!/usr/bin/env python3

import os
import sys
import unittest
import subprocess
import xml.etree.ElementTree as ET
from subprocess import Popen, PIPE, STDOUT

#######################
# pshell local commands 
#######################
class pshell_local(unittest.TestCase):
    def setUp(self):
        global script, verbosity, config
        self.script = script
        self.verbosity = verbosity
        self.python = "python3"
# CURRENT - override if we want to point at a server in config
        self.config = config

# --
    def test_lcd(self):
        flag=False
        p = Popen([self.python, self.script, "-v", self.verbosity, "lpwd"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            if 'Local' in line.decode():
                flag = True
        self.assertTrue(flag)

# --
    def test_lls(self):
        flag = False
        p = Popen([self.python, self.script, "-v", self.verbosity, "lls"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            if "test_pshell.py" in line.decode():
                flag = True
        self.assertTrue(flag)


########################################
# wrapper for squishing bugs
########################################
class pshell_bugs(unittest.TestCase):
    def setUp(self):
        global script, verbosity, config
        self.script = script
        self.verbosity = verbosity
        self.python = "python3"
# CURRENT - override if we want to point at a server in config
        self.config = config


########################################
# wrapper for new features 
########################################
class pshell_features(unittest.TestCase):
    def setUp(self):
        global script, verbosity, config
        self.script = script
        self.verbosity = "1"
        self.python = "python3"
# CURRENT - override if we want to point at a server in config
        self.config = config


    def test_ls(self):
        if self.config is not None:
            p = Popen([self.python, self.script, "-v", self.verbosity, "-c", self.config, "ls"], stdout=PIPE, stderr=STDOUT)
            flag = False
            for line in p.stdout:
                print(line)
        else:
            print("No remote - skipping")



######
######
# main
######
if __name__ == '__main__':

    global session, server, script, verbosity

    print("\n----------------------------------------------------------------------")
    print("Running tests for: pshell")
    print("----------------------------------------------------------------------\n")

# NB: don't use pshell as we can't run bundle_pshell in the container
    script = "pshell.py"
    verbosity = "0"

# local (offline) testing only
#    config = None
#   config = 0.0.0.0

# remote testing (eg features)
    config = "data.pawsey.org.au"

# class suite to test
#    test_class_list = [pshell_features]
    test_class_list = [pshell_local]
#    test_class_list = [pshell_bugs]

# setup the session
# TODO - just use config ...
    if config is not None:
        print("TODO - run pshell and login if required ...")

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

