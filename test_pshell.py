#!/usr/bin/python

import os
import sys
import unittest
import subprocess

#########################
# serverless pshell tests
#########################
class pshell_syntax(unittest.TestCase):

    def test_cd(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", "cd /projects\"'"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="asset.namespace.exists" session=""><args><namespace>/projects"\'</namespace></args></service></request>')

    def test_mkdir(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", "mkdir /dir1/../dir2/namespace\"'"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="asset.namespace.create" session=""><args><namespace>/dir2/namespace"\'</namespace></args></service></request>')

    def test_file(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", r'file "/dir1/../dir2/test_!@#\""'], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="asset.get" session=""><args><id>path=/dir2/test_!@#"</id></args></service></request>')
 

########################################
# convenience wrapper for squishing bugs
########################################
class pshell_bugs(unittest.TestCase):
    def setUp(self):
        print "setup"

    def test_squish(self):
        print "TODO"


######
# main
######
if __name__ == '__main__':

    print "\n----------------------------------------------------------------------"
    print "Running offline tests for: pshell"
    print "----------------------------------------------------------------------\n"

# classes to test
    test_class_list = [pshell_syntax]

#    test_class_list = [pshell_bugs]


# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

