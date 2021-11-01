#!/usr/bin/env python3

import os
import sys
import unittest
import subprocess
from subprocess import Popen, PIPE, STDOUT

#------------------------------------------------------------
class pshell_standard(unittest.TestCase):
    def setUp(self):
        global config, python, script, verbosity
        self.config = config
        self.python = python
        self.script = script
        self.verbosity = verbosity

# ---
    def test_exit_codes(self):
        p = Popen([self.python, self.script, "-v", self.verbosity, "help"], stdout=PIPE, stderr=STDOUT)
        data = p.communicate()
        code = p.returncode
        self.assertEqual(code, 0)
        p = Popen([self.python, self.script, "-v", self.verbosity, "bad_command_or_error_of_some_kind"], stdout=PIPE, stderr=STDOUT)
        data = p.communicate()
        code = p.returncode
        self.assertNotEqual(code, 0)

# ---
    def test_remote_add_mflux(self):
        flag=False
        line = "mfclient mflux http://0.0.0.0:80"
        p = Popen([self.python, self.script, "-v", self.verbosity, "remote add %s && remote" % line], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            text = line.decode()
            if "mfclient" in text:
                if "mflux" in text:
                    if "0.0.0.0" in text:
                        flag=True
        self.assertTrue(flag)

# ---
    def test_remote_add_s3(self):
        flag=False
        line = "s3client s3 http://0.0.0.0"
        p = Popen([self.python, self.script, "-v", self.verbosity, "remote add %s && remote" % line], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            text = line.decode()
            if "s3client" in text:
                if "s3" in text:
                    if "0.0.0.0" in text:
                        flag=True
        self.assertTrue(flag)

# ---
    def test_default_portal_config(self):
        flag=False
        p = Popen([self.python, self.script, "-v", self.verbosity, "remote"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            text = line.decode()
            if "portal" in text:
                if "mflux" in text:
                    if "data.pawsey.org.au" in text:
                        flag=True
        self.assertTrue(flag)

# ---
    def test_default_public_config(self):
        flag=False
        p = Popen([self.python, self.script, "-v", self.verbosity, "remote"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            text = line.decode()
            if "public" in text:
                if "mflux" in text:
                    if "data.pawsey.org.au" in text:
                        flag=True
        self.assertTrue(flag)

# ---
    def test_lpwd(self):
        flag=False
        pwd = os.getcwd()
        p = Popen([self.python, self.script, "-v", self.verbosity, "lpwd"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            if pwd in line.decode():
                flag=True

# ---
    def test_lcd(self):
        flag=False
        p = Popen([self.python, self.script, "-v", self.verbosity, "lcd ."], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            if 'Local' in line.decode():
                flag = True
        self.assertTrue(flag)

# ---
    def test_lls(self):
        flag = False
        p = Popen([self.python, self.script, "-v", self.verbosity, "lls"], stdout=PIPE, stderr=STDOUT)
        for line in p.stdout:
            if "test_pshell.py" in line.decode():
                flag = True
        self.assertTrue(flag)

#------------------------------------------------------------

if __name__ == '__main__':

    global python, config, script, verbosity

    print("\n----------------------------------------------------------------------")
    print("Running tests for: pshell")
    print("----------------------------------------------------------------------\n")

    python = "python3"
    config = "test"
    script = "pshell.py"
    verbosity = "0"

# class suite to test
    test_class_list = [pshell_standard]

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

