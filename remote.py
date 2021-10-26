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
        self.status = "Connected to the void"
        self.type = None
        self.cwd = None

# --- primary methods

    def endpoint(self):
        return { 'type':self.type }

    def connect(self):
        return True

    def status(self):
        return(self.status)

    def cd(self, remote_fullpath):
        self.cwd = remote_fullpath

    def ls_iter(self, pattern):
        yield "Nothing"

    def get_iter(self, pattern):
        yield 1
        yield 0
        yield "Nothing"

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

# --- secondary methods

    def login(self):
        raise Exception("Not implemented")

    def logout(self):
        raise Exception("Not implemented")

    def delegate(self, subcommand):
        raise Exception("Not implemented")

    def publish(self, filepath):
        raise Exception("Not implemented")

    def unpublish(self, filepath):
        raise Exception("Not implemented")

    def info(self, remote_filepath):
        raise Exception("Not implemented")

    def complete_file(self, cwd, partial, start):
        raise Exception("Not implemented")

    def complete_folder(self, cwd, partial, start):
        raise Exception("Not implemented")

    def copy(self, src, dest, dest_type):
        raise Exception("Not implemented")

    def command(self, line):
        raise Exception("Unknown remote command")
