# README #

mfclient.py is a basic Mediaflux client module for Python 2.7

owner: Sean Fleming (sean.fleming@pawsey.org.au)


### How do I get set up? ###

Simply import the mfclient module into your Python code.


### How do I run the tests? ###

Run test_mfclient.py

### Is there a reference document? ###

To generate an API reference document, run:

    python3 -m pydoc mfclient

### Command line client ###

pshell.py - command line client for mediaflux that uses mfclient for server communication.

It features:

* SFTP-like syntax
* Tab completion
* Basic support for mediaflux aterm commands


### FUSE filesystem client ###

pmount.py - FUSE filesystem implementation for mounting a mediaflux namespace as a local folder.

