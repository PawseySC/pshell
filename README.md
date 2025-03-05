# README #

pshell is a command line file transfer tool for:
- Mediaflux
- S3

owner: Sean Fleming (sean.fleming@pawsey.org.au)

### How do I get set up? ###

Install python >= 3.6 on the platform of your choice.

Invoke pshell.py or the prepackaged binary distribution.

Note that you will need to install (eg via pip3) boto3 if you want to use the S3 capabilities.

### How do I run the tests? ###

Run test_all

### Command line client ###

pshell.py - command line client for mediaflux that uses mfclient and/or s3client for server communication.

Features:
- SFTP-like syntax
- Tab completion
- Basic support for passthru commands
