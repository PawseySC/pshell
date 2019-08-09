#!/usr/bin/python

import os
import fuse
import errno
import unittest
from pmount import mfwrite

mfobj = None

# -- buffer class tests
class pmount_buffer(unittest.TestCase):
# --
    def setUp(self):
        global mfobj
        self.mfobj = mfobj
# --
    def test_buffer_sequential(self):
        ba1 = bytearray([0,1,2])
        ba2 = bytearray([6,7,8])
        ba3 = bytearray([0,1,2,6,7,8])
        self.mfobj.inject(ba1, 0)
        self.mfobj.inject(ba2, 3)
        self.assertEqual(ba3, self.mfobj.buffer)
# --
    def test_buffer_restart(self):
        ba1 = bytearray([0,1,2])
        ba2 = bytearray([6,7,8])
        self.mfobj.inject(ba1, 0)
        self.mfobj.inject(ba2, 0)
        self.assertEqual(ba2, self.mfobj.buffer)
# --
    def test_buffer_nonseq(self):
        ba1 = bytearray([0,1,2])
        ba2 = bytearray([6,7,8])
        ba3 = bytearray([0,1,2,0,0,0,0,0,0,6,7,8])
        self.mfobj.inject(ba1, 0)
        self.mfobj.inject(ba2, 9)
        self.assertEqual(ba3, self.mfobj.buffer)
# --
    def test_buffer_illseq(self):
        ba1 = bytearray([0,1,2])
        ba2 = bytearray([6,7,8])
        ba3 = bytearray([3,4,5])
        self.mfobj.inject(ba1, 0)
        self.mfobj.inject(ba2, 6)
# TODO - if this passes we support fully random writes (difficult to implement in mediaflux)
        try:
            self.mfobj.inject(ba3, 3)
        except:
            print "unordered write() not implemented"

# TODO - quota and truncate tests ...
#        for b in self.mfobj.buffer:
#            print "[%r]" % b


# -- TODO: FUSE operations tests



# -- test runner
if __name__ == '__main__':

# setup
    mfobj = mfwrite(store="iron", quota="1000000", tmpfile="/rdsi/tmp123", fullpath="/projects/name/file123")

# classes to test
    test_class_list = [pmount_buffer]
#    test_class_list = [pmount_fuse]

# build suite
    suite_list = []
    for test_class in test_class_list:
        suite_list.append(unittest.TestLoader().loadTestsFromTestCase(test_class))
    suite = unittest.TestSuite(suite_list)

# run suite
    unittest.TextTestRunner(verbosity=2).run(suite)


