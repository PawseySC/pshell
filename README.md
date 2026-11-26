# README #

pshell is a command line file transfer tool for:
- Mediaflux
- S3

owner: Sean Fleming (sean.fleming@pawsey.org.au)

### How do I get set up? ###

Install python >= 3.6 on the platform of your choice.

Invoke pshell.py or the prepackaged binary distribution.

Release builds bundle boto3 and its dependencies for S3 support. Source runs
require a compatible boto3 installation.

### How do I run the tests? ###

Run test_all

### Command line client ###

pshell.py - command line client for mediaflux that uses mfclient and/or s3client for server communication.

Features:
- SFTP-like syntax
- Tab completion
- Basic support for passthru commands

## License

pshell is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
