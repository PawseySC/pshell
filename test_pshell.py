#!/usr/bin/python

import os
import sys
import unittest
import subprocess
import xml.etree.ElementTree as ET
import pshell

#########################
# serverless pshell tests
#########################
class pshell_syntax(unittest.TestCase):

    def test_cd(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", "cd /projects\"'"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.namespace.exists"><namespace>/projects"\'</namespace></service></args></service></request>')

    def test_rm(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", "rm *\'*"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.query"><where>namespace=\'/projects\' and name=\'*\\\'*\'</where><action>count</action></service></args></service></request>')

    def test_file(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", r'file "/dir1/../dir2/test_!@#\""'], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.get"><id>path=/dir2/test_!@#"</id></service></args></service></request>')

    def test_mkdir(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", "mkdir /dir1/../dir2/namespace\"'"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.namespace.create"><namespace>/dir2/namespace"\'</namespace></service></args></service></request>')

    def test_rmdir(self):
        proc = subprocess.Popen(["pshell.py", "-c", "dummy", "rmdir sean's dir"], stdout=subprocess.PIPE)
        for line in proc.stdout:
            if "request" in line:
                self.assertEqual(line.strip(), '<request><service name="service.execute" session=""><args><service name="asset.namespace.exists"><namespace>/projects/sean\'s dir</namespace></service></args></service></request>')

# NEW - test conversion of XML to Arcitecta's shorthand format - used for metadata import
    def test_import_metadata(self):
        pp = pshell.parser()
        xml_root = ET.fromstring('<ctype>"image/tiff"</ctype>')
        result = pp.xml_to_mf(xml_root, result=None)
        self.assertEqual(result, ' :ctype "image/tiff"')
        xml_root = ET.fromstring('<geoshape><point><elevation>5.0</elevation></point></geoshape>')
        result = pp.xml_to_mf(xml_root, result=None)
        self.assertEqual(result, ' :geoshape < :point < :elevation 5.0 > >')


########################################
# convenience wrapper for squishing bugs
########################################
class pshell_bugs(unittest.TestCase):
    def setUp(self):
        print "setup"

# TODO - more testing of pshell methods
    def test_squish(self):
        print "NEW" 
        pp = pshell.parser()
        xml_root = ET.fromstring('<geoshape><point><elevation>5.0</elevation></point></geoshape>')
        result = pp.xml_to_mf(xml_root, result=None)
        self.assertEqual(result, ' :geoshape < :point < :elevation 5.0 > >')


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

