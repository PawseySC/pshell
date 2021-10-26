#!/usr/bin/env python3

import parser
import remote
import unittest

# global to avoid setup for every test class
myparser = None

#------------------------------------------------------------
class parser_standard(unittest.TestCase):
    def setUp(self):
        global parser

        self.parser = myparser
        self.parser.remotes_add('mfclient', {'type':'mflux', 'protocol':'http', 'server':'localhost', 'port':80})
        self.parser.remotes_add('s3client', {'type':'s3', 'url':'http://localhost'})

# ---
    def test_remote_complete(self):
        result = self.parser.complete_remote("mf", "mf", 0, 2)
        if 'mfclient' in result:
            success = True
        else:
            success = False
        self.assertTrue(success)

# ---
    def test_remote_set(self):
        self.parser.do_remote('mfclient')
        self.assertEqual(self.parser.remotes_current, 'mfclient')
        self.parser.do_remote('s3client')
        self.assertEqual(self.parser.remotes_current, 's3client')


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

