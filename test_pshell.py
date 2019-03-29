#!/usr/bin/python

import os
import sys
import unittest
import subprocess
import xml.etree.ElementTree as ET

#########################
# serverless pshell tests
#########################
class pshell_syntax(unittest.TestCase):

#    pshell_exe = "pshell.py"
    pshell_exe = "pshell"

    def test_cd(self):
        proc = subprocess.Popen([self.pshell_exe, "-c", "dummy", "cd /projects\"'"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.namespace.exists"><namespace>/projects"\'</namespace></service></args></service></request>')

    def test_rm(self):
        proc = subprocess.Popen([self.pshell_exe, "-c", "dummy", "rm *\'*"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.query"><where>namespace=\'/projects\' and name=\'*\\\'*\'</where><action>count</action></service></args></service></request>')

    def test_file(self):
        proc = subprocess.Popen([self.pshell_exe, "-c", "dummy", r'file "/dir1/../dir2/test_!@#\""'], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.get"><id>path=/dir2/test_!@#"</id></service></args></service></request>')

    def test_mkdir(self):
        proc = subprocess.Popen([self.pshell_exe, "-c", "dummy", "mkdir /dir1/../dir2/namespace\"'"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.namespace.create"><namespace>/dir2/namespace"\'</namespace></service></args></service></request>')

    def test_rmdir(self):
        proc = subprocess.Popen([self.pshell_exe, "-c", "dummy", "rmdir sean's dir"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.namespace.exists"><namespace>/projects/sean\'s dir</namespace></service></args></service></request>')

# expected response from server is "invalid session" ... anything else is a server connection problem
    def test_server_responding(self):
        proc = subprocess.Popen(["pshell", "whoami"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "Failed to establish network connection" in line:
                raise Exception("Server failed to respond.")

########################################
# convenience wrapper for squishing bugs
########################################
class pshell_bugs(unittest.TestCase):
    def setUp(self):
        print "setup"

# this works ... but is slow ... takes 7+ seconds ...
#    def test_public_www(self):
#        proc = subprocess.Popen(["pshell", "get /www/index.html"], stdout=subprocess.PIPE)
#        for line in proc.stdout:
#            print line.strip()

# can't do - as we forbid login in a pshell scripted sense ...
#    def test_connection(self):
#        proc = subprocess.Popen(["pshell", "login"], stdout=subprocess.PIPE)
#        for line in proc.stdout:
#            print line.strip()

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

