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
        # FIXME - not used ...
    def test_completion_bucket(self):
        reply = self.s3_client.completion_match("/", "buc", 0, "bucket1")
        self.assertEqual(reply, "bucket1")

# NEW - S3 policy 
    def test_policy_read_allow(self):
        policy = s3client.s3_policy("bucket")
        policy.iam_owner = 'user4'
        s = policy.statement_new(resources="arn:aws:s3:::bucket", perm="+r", users="user1,user2, user3")
# FIXME - Principle assertion
#        self.assertCountEqual(s['Principle']['AWS'], ['arn:aws:iam:::user/user1','arn:aws:iam:::user/user2','arn:aws:iam:::user/user3', 'user4'])
        self.assertEqual(s['Effect'], "Allow")
        self.assertEqual(s['Resource'], "arn:aws:s3:::bucket")
        self.assertCountEqual(s['Action'], ['s3:ListBucket', 's3:GetObject'])

    def test_policy_read_deny(self):
        policy = s3client.s3_policy("bucket")
        s = policy.statement_new(perm="-r", users="user1")
        self.assertEqual(s['Effect'], "Deny")
        self.assertCountEqual(s['Action'], ['s3:ListBucket', 's3:GetObject'])
# FIXME - Principle assertion
#        self.assertCountEqual(s['Principle']['AWS'], ['arn:aws:iam:::user/user1'])

    def test_policy_write_allow(self):
        policy = s3client.s3_policy("bucket")
        s = policy.statement_new(perm="+w", users="user1, user2")
# FIXME - Principle assertion
        self.assertEqual(s['Effect'], "Allow")
        self.assertCountEqual(s['Action'], ['s3:PutObject', 's3:DeleteObject'])

    def test_policy_write_deny(self):
        policy = s3client.s3_policy("bucket")
        s = policy.statement_new(perm="-w", users="user1, user2")
# FIXME - Principle assertion
        self.assertEqual(s['Effect'], "Deny")
        self.assertCountEqual(s['Action'], ['s3:PutObject', 's3:DeleteObject'])

#------------------------------------------------------------
class s3client_new(unittest.TestCase):
    def test_something(self):
        print("TODO")

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
    test_class_list = [s3client_standard]
#    test_class_list = [s3client_new]

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)

