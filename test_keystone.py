#!/usr/bin/env python3

import keystone
import unittest

# global to avoid setup for every test class
ks = None

#------------------------------------------------------------
class keystone_standard(unittest.TestCase):
    def setUp(self):
        global ks
        self.keystone = ks

# --- port change to 8080 ...
    def test_discover_s3(self):


# TODO 

        self.keystone.url = "https://somewhere.org:500"
        s3 = self.keystone.discover_s3_endpoint()

        print(s3['url'])

        self.keystone.url = "https://somewhere.org"
        s3 = self.keystone.discover_s3_endpoint()

        print(s3['url'])

#------------------------------------------------------------

if __name__ == '__main__':

    try:
        ks = keystone.keystone("url")
        print("\n----------------------------------------------------------------------")
        print("Running tests for: keystone module")
        print("----------------------------------------------------------------------\n")
    except Exception as e:
        print(str(e))
        exit(-1)

# classes to test
    test_class_list = [keystone_standard]

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

