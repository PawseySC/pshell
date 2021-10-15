# README #

mfclient.py is a basic Mediaflux client module for Python 3.x
s3client.py is a basic S3 client module for Python 3.x

owner: Sean Fleming (sean.fleming@pawsey.org.au)


### How do I get set up? ###

Simply import the mfclient module into your Python code.


### How do I run the tests? ###

Run test_mfclient.py

### Is there a reference document? ###

To generate an API reference document, run:

    python3 -m pydoc mfclient

### Command line client ###

pshell.py - command line client for mediaflux that uses mfclient and/or s3client for server communication.

It features:

* SFTP-like syntax
* Tab completion
* Basic support for passthru commands


### FUSE filesystem client ###

pmount.py - FUSE filesystem implementation for mounting a mediaflux namespace as a local folder.

