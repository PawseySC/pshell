#!/usr/bin/env python3

import parser
import mfclient
import s3client
import unittest

# global to avoid setup for every test class
myparser = None

#------------------------------------------------------------
class parser_standard(unittest.TestCase):
    def setUp(self):
        self.mf_client = mfclient.mf_client()
        self.s3_client = mfclient.s3_client()
        global parser


        self.parser = myparser
        self.parser.remotes_add('/path1', self.mf_client)
        self.parser.remotes_add('/path2', self.s3_client)

# TODO - test more awkward mounting patterns ie /path and /path/subremote

# TODO - test to enforce no mounting on the same path


# ---
    def test_remotes_get(self):
        try:
            result = self.parser.remotes_get("/path1")
            result = self.parser.remotes_get("/path1/some/folder")
            result = self.parser.remotes_get("/path2")
            result = self.parser.remotes_get("/path2/some/folder")
            success = True
        except:
            success = False
        self.assertTrue(success)

# ---
    def test_remotes_get_fail(self):
        try:
            self.parser.remotes_get("/nothing")
            success = True
        except Exception as e:
            success = False
            pass
        self.assertFalse(success)

# ---
    def test_remotes_complete(self):
        result = self.parser.remotes_complete("/pa", 0)
        if 'th1' in result and 'th2' in result:
            success = True
        else:
            success = False
        self.assertTrue(success)

#------------------------------------------------------------

if __name__ == '__main__':

    try:
        myparser = parser.parser()
        print("\n----------------------------------------------------------------------")
        print("Running tests for: parser module")
        print("----------------------------------------------------------------------\n")
    except Exception as e:
        print(str(e))
        exit(-1)


# classes to test
    test_class_list = [parser_standard]

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

