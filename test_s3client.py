#!/usr/bin/env python3

import s3client
import unittest

# global to avoid setup for every test class
s3_client = None

# --- helper

################################################
# serverless aterm style XML serialisation tests
################################################
class s3client_syntax(unittest.TestCase):

    def setUp(self):
        global s3_client
        self.s3_client = s3_client

    def test_split(self):
        reply = self.s3_client.path_split("/acacia/bucket/object1")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "object1/")

    def test_cd(self):
        reply = self.s3_client.cd("relative")
        self.assertEqual(reply, "/relative")

        reply = self.s3_client.cd("/mount")
        self.assertEqual(reply, "/mount")

        reply = self.s3_client.cd("/mount/bucket")
        self.assertEqual(reply, "/mount/bucket")

        reply = self.s3_client.cd("/mount/bucket/object/far/away/")
        self.assertEqual(reply, "/mount/bucket")



########################################
# convenience wrapper for squishing bugs
########################################
class s3client_bugs(unittest.TestCase):

    def setUp(self):
        global s3_client
        self.s3_client = s3_client

######
# main
######
if __name__ == '__main__':

# acquire a dummy client instance
    try:

        s3_client = s3client.s3_client()

        print("\n----------------------------------------------------------------------")
        print("Running offline tests for: s3client module")
        print("----------------------------------------------------------------------\n")
    except Exception as e:
        print(str(e))
        exit(-1)


# classes to test
    test_class_list = [s3client_syntax]


# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

