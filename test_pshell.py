#!/usr/bin/python

import os
import sys
import unittest
import subprocess

################################################
# serverless pshell tests
################################################
class pshell_syntax(unittest.TestCase):

    def setUp(self):
        print "setup"

    def test_cd(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", "cd /projects\"'"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            print line


    def test_put(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", "get *"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            print line

 


# convenience wrapper for squishing bugs
class pshell_bugs(unittest.TestCase):
    def setUp(self):
        print "setup"

    def test_squish1(self):
        print "TODO"





######
# main
######
if __name__ == '__main__':



# classes to test
    test_class_list = [pshell_syntax]

#    test_class_list = [mfclient_bugs]


# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

