#!/usr/bin/env python3

import logging
import s3client
import unittest

# global to avoid setup for every test class
s3_client = None

#------------------------------------------------------------
class s3client_standard(unittest.TestCase):

    def setUp(self):
        global s3_client
        self.s3_client = s3_client

# path to bucket,prefix,key conversion tests
    def test_convert_missing_bucket(self):
        reply = self.s3_client.path_convert("/")
        self.assertEqual(reply[0], None)
        self.assertEqual(reply[1], "")
        self.assertEqual(reply[2], "")
    def test_covert_noprefix_key(self):
        reply = self.s3_client.path_convert("/bucket/key")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "")
        self.assertEqual(reply[2], "key")
    def test_convert(self):
        reply = self.s3_client.path_convert("/bucket")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "")
        self.assertEqual(reply[2], "")
    def test_covert_nokey_prefix(self):
        reply = self.s3_client.path_convert("/bucket/prefix/")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "prefix/")
        self.assertEqual(reply[2], "")
    def test_covert_nokey_long_prefix(self):
        reply = self.s3_client.path_convert("/bucket/prefix1/prefix2/prefix3/")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "prefix1/prefix2/prefix3/")
        self.assertEqual(reply[2], "")
    def test_convert_all(self):
        reply = self.s3_client.path_convert("/bucket/prefix1/prefix2/prefix3/key*")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "prefix1/prefix2/prefix3/")
        self.assertEqual(reply[2], "key*")
    def test_convert_normpath_all(self):
        reply = self.s3_client.path_convert("/bucket/prefix1/prefix2/../prefix3/key")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "prefix1/prefix3/")
        self.assertEqual(reply[2], "key")
    def test_convert_relpath_normpath_all(self):
        reply = self.s3_client.path_convert("bucket/prefix1/prefix2/../prefix3/key")
        self.assertEqual(reply[0], "bucket")
        self.assertEqual(reply[1], "prefix1/prefix3/")
        self.assertEqual(reply[2], "key")
    def test_convert_bucket_normpath(self):
        reply = self.s3_client.path_convert("/bucket/../new")
        self.assertEqual(reply[0], "new")
        self.assertEqual(reply[1], "")
        self.assertEqual(reply[2], "")
    def test_completion_match_no_candidate(self):
        reply = self.s3_client.completion_match("/bucket/child1/", "partial", 0, "match/")
        self.assertEqual(reply, None)
    def test_completion_match_nongreedy(self):
        reply = self.s3_client.completion_match("/", "bucket/child1/", 0, "child1/child2/")
        self.assertEqual(reply, "bucket/child1/child2/")
    def test_completion_match_empty_partial_prefix(self):
        reply = self.s3_client.completion_match("/bucket/child1/", "", 0, "child2/")
        self.assertEqual(reply, "child2/")
    def test_completion_match_empty_partial_object(self):
        reply = self.s3_client.completion_match("/bucket/child1/", "", 0, "object1")
        self.assertEqual(reply, "object1")
    def test_completion_match_normpath(self):
        reply = self.s3_client.completion_match("/bucket1/", "../chil", 0, "child1/child2/")
        self.assertEqual(reply, "../child1/child2/")
    def test_completion_match_normpath_no_candidate(self):
        reply = self.s3_client.completion_match("/bucket1/", "../bucket2/child1/partial", 0, "child1/child2/")
        self.assertEqual(reply, None)
    def test_completion_match_normpath_greedy_prefix(self):
        reply = self.s3_client.completion_match("/bucket1/", "../bucket2/child1/chil", 0, "child1/child2/")
        self.assertEqual(reply, "../bucket2/child1/child2/")
    def test_completion_normpath_nongreedy(self):
        reply = self.s3_client.completion_match("/bucket/child1/", "../", 0, "child2/")
        self.assertEqual(reply, "../child2/")
    def test_completion_normpath_nongreedy_offset(self):
        reply = self.s3_client.completion_match("/bucket/child1/", "../", 3, "child2/")
        self.assertEqual(reply, "child2/")
    def test_completion_normpath_partial(self):
        reply = self.s3_client.completion_match("/bucket/child1/", "../c", 0, "child2/")
        self.assertEqual(reply, "../child2/")
    def test_completion_normpath_partial_offset(self):
        reply = self.s3_client.completion_match("/bucket/child1/", "../c", 3, "child2/")
        self.assertEqual(reply, "child2/")
    def test_completion_nomatch(self):
        reply = self.s3_client.completion_match("/bucket/", "zg", 0, "prefix/")
        self.assertEqual(reply, None)
    def test_completion_nomatch_char_overlap(self):
        reply = self.s3_client.completion_match("/bucket/", "sp", 0, "prefix/")
        self.assertEqual(reply, None)
    def test_completion_nomatch_string_overlap(self):
        reply = self.s3_client.completion_match("/bucket/", "popref", 0, "prefix/")
        self.assertEqual(reply, None)

# TODO - not used yet
    def test_completion_bucket(self):
        reply = self.s3_client.completion_match("/", "buc", 0, "bucket1")
        self.assertEqual(reply, "bucket1")

#------------------------------------------------------------
class s3client_new(unittest.TestCase):

    def setUp(self):
        global s3_client
        self.s3_client = s3_client

    def test_policy_get(self):
        reply = self.s3_client.policy_bucket_get("bucket", "+r", "user1")
        self.assertEqual(reply, '{"Id": "Custom-Policy", "Statement": [{"Effect": "Allow", "Principal": {"AWS": ["arn:aws:iam:::user/user1"]}, "Action": ["s3:ListBucket", "s3:GetObject"], "Resource": ["arn:aws:s3:::bucket/*"]}]}')

        reply = self.s3_client.policy_bucket_get("bucket", "+r", "user1, user2")
        self.assertEqual(reply, '{"Id": "Custom-Policy", "Statement": [{"Effect": "Allow", "Principal": {"AWS": ["arn:aws:iam:::user/user1", "arn:aws:iam:::user/user2"]}, "Action": ["s3:ListBucket", "s3:GetObject"], "Resource": ["arn:aws:s3:::bucket/*"]}]}')


        reply = self.s3_client.policy_bucket_get("bucket", "+w", "user1")
        reply = self.s3_client.policy_bucket_get("bucket", "+w", "user1, user2")

# TODO - asserts ...


#------------------------------------------------------------
if __name__ == '__main__':

# acquire a dummy client instance
    try:
        s3_client = s3client.s3_client()
#        s3_client = s3client.s3_client(log_level=logging.DEBUG)

        print("\n----------------------------------------------------------------------")
        print("Running tests for: s3client module")
        print("----------------------------------------------------------------------\n")
    except Exception as e:
        print(str(e))
        exit(-1)


# classes to test
#    test_class_list = [s3client_standard]
    test_class_list = [s3client_new]

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

