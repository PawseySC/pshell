#!/usr/bin/python

"""
This module is a Python 3.x implementation of a client interface to remote storage servers 
Author: Sean Fleming
"""

class client:
    """
    Base class
    """

    def __init__(self):
        self.status = "not connected"
        self.type = "generic"
        self.cwd = None

    def endpoint(self):
        raise Exception("Not implemented")

    def connect(self):
        raise Exception("Not implemented")

    def status(self):
        return(self.status)

    def login(self):
        raise Exception("Not implemented")

    def logout(self):
        raise Exception("Not implemented")

    def delegate(self, subcommand):
        raise Exception("Not implemented")

    def info(self, remote_filepath):
        raise Exception("Not implemented")

    def cd(self, remote_fullpath):
        self.cwd = remote_fullpath

    def ls_iter(self, pattern):
        yield "empty"

    def copy(self, src, dest, dest_type):
        raise Exception("Not implemented")

    def get(self, remote_filepath, local_filepath=None):
        raise Exception("Not implemented")

    def put(self, remote_fullpath, local_filepath):
        raise Exception("Not implemented")

    def mkdir(self, fullpath):
        raise Exception("Not implemented")

    def rmdir(self, fullpath, prompt=None):
        raise Exception("Not implemented")

    def rm(self, filepath, prompt=None):
        raise Exception("Not implemented")

    def command(self, line):
        raise Exception("Unknown command or not implemented")
