#!/usr/bin/env python3

import s3client
import unittest

# global to avoid setup for every test class
s3_client = None

#------------------------------------------------------------
class s3client_standard(unittest.TestCase):

    def setUp(self):
        global s3_client
        self.s3_client = s3_client

# general

    def test_split_empty_string(self):
        reply = self.s3_client.path_split("")
        self.assertEqual(reply[0], None)
        self.assertEqual(reply[1], "")

# abspath behaviour

    def test_split_missing_bucket(self):
        reply = self.s3_client.path_split("/")
        self.assertEqual(reply[0], None)
        self.assertEqual(reply[1], "")

    def test_split_nokey(self):
        reply = self.s3_client.path_split("/bucket")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "")

    def test_split_prefix(self):
        reply = self.s3_client.path_split("/bucket/")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "/")

    def test_split_object(self):
        reply = self.s3_client.path_split("/bucket/object1")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "object1")

    def test_split_object_prefix(self):
        reply = self.s3_client.path_split("/bucket/prefix/")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "prefix/")

    def test_split_path_resolve_object(self):
        reply = self.s3_client.path_split("/bucket/prefix1/../prefix2")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "prefix2")

    def test_split_path_resolve_object_prefix(self):
        reply = self.s3_client.path_split("/bucket/prefix1/../prefix2/")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "prefix2/")

# relpath behaviour (NB: expect abspath, but handle by converting)

    def test_split_relpath_nokey(self):
        reply = self.s3_client.path_split("bucket")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "")

    def test_split_relpath_prefix(self):
        reply = self.s3_client.path_split("bucket/")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "/")

    def test_split_relpath_object(self):
        reply = self.s3_client.path_split("bucket/object1")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "object1")

    def test_split_relpath_object_prefix(self):
        reply = self.s3_client.path_split("bucket/object1/")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "object1/")

#------------------------------------------------------------
class s3client_bugs(unittest.TestCase):

    def setUp(self):
        global s3_client
        self.s3_client = s3_client

#------------------------------------------------------------
if __name__ == '__main__':

# acquire a dummy client instance
    try:
        s3_client = s3client.s3_client()

        print("\n----------------------------------------------------------------------")
        print("Running tests for: s3client module")
        print("----------------------------------------------------------------------\n")
    except Exception as e:
        print(str(e))
        exit(-1)

# classes to test
    test_class_list = [s3client_standard]

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

