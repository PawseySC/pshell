#!/usr/bin/env python3
"""
Standard lib python3 command line client for mediaflux and s3
Author: Sean Fleming
"""

import sys
# magnus python 3.4 seems broken, module python/3.6.3 works fine
VERSION_MIN = (3, 6)
if sys.version_info < VERSION_MIN:
    sys.exit("ERROR: Python >= %d.%d is required, your version = %d.%d\n" % (VERSION_MIN[0], VERSION_MIN[1], sys.version_info[0], sys.version_info[1]))
import os
import re
import json
import urllib
import logging
import argparse
import platform
import itertools
import configparser
import concurrent.futures
from remote import client
import mfclient
import keystone
import s3client
import parser
# no readline on windows
try:
    import readline
except:
    pass

# auto
build= "20211022131216"

#------------------------------------------------------------
def main():
    global build

# server config (section heading) to use
    p = argparse.ArgumentParser(description="pshell help")
    p.add_argument("-c", dest='current', default='pawsey', help="the config name in $HOME/.mf_config to connect to")
    p.add_argument("-i", dest='script', help="input script file containing pshell commands")
    p.add_argument("-o", dest='output', default=None, help="output any failed commands to a script")
    p.add_argument("-v", dest='verbose', default=None, help="set verbosity level (0,1,2)")
    p.add_argument("-u", dest='url', default=None, help="Remote endpoint URL")
    p.add_argument("-t", dest='type', default=None, help="Remote endpoint type (eg mflux, s3)")
    p.add_argument("-d", dest='domain', default=None, help="login authentication domain")
    p.add_argument("-s", dest='session', default=None, help="session")
    p.add_argument("-m", dest='mount', default='/', help="mount point for remote")
    p.add_argument("-k", dest='keystone', default=None, help="A URL to the REST interface for OpenStack Keystone")
    p.add_argument("command", nargs="?", default="", help="a single command to execute")
    args = p.parse_args()

# configure logging
    logging_level = logging.ERROR
    if args.verbose is not None:
        if args.verbose == "2":
            logging_level = logging.DEBUG
        elif args.verbose == "1":
            logging_level = logging.INFO
#    print("log level = %d" % logging_level)
    logging.basicConfig(format='%(levelname)9s %(asctime)-15s >>> %(module)s.%(funcName)s(): %(message)s', level=logging_level)

# basic info
    logging.info("PSHELL=%s" % build)
    logging.info("PLATFORM=%s" % platform.system())
    version = sys.version
    i = version.find("\n")
    logging.info("PYTHON=%s" % version[:i])

# attempt to locate a valid config file
    config_filepath = os.path.expanduser("~/.pshell_config")
    try:
        open(config_filepath, 'a').close()
    except:
        config_filepath = os.path.join(os.getcwd(), ".pshell_config")

    config = configparser.ConfigParser()
    logging.debug("Reading config file: [%s]" % config_filepath)
    config.read(config_filepath)

# create an endpoint 
    try:
        endpoint = None 
        endpoints = None 
        if args.url is None:
# existing config and no input URL
            if config.has_section(args.current) is True:
                logging.info("No input URL, reading endpoints from existing config [%s]" % args.current)
                endpoints = json.loads(config.get(args.current, 'endpoints'))
            else:
# 1st time default
                logging.info("Initialising Pawsey config")
                endpoint = {'name':args.current, 'type':'mflux', 'url':'https://data.pawsey.org.au:443', 'domain':'ivec' }
                args.mount = '/projects'
                args.keystone = 'https://nimbus.pawsey.org.au:5000'
# 1st time default or URL override
        if endpoints is None:
            if endpoint is None:
                logging.info("Creating endpoint from url: [%s]" % args.url)
                endpoint = {'name':'custom', 'type':args.type, 'url':args.url, 'domain':args.domain }

# setup for adding as remotes
            endpoints = { args.mount:endpoint }
            config[args.current] = {'endpoints':json.dumps(endpoints)}

    except Exception as e:
        logging.debug(str(e))

# CLI overrides
    if args.session is not None:
        endpoint['session'] = args.session
    if args.domain is not None:
        endpoint['domain'] = args.domain

# extract terminal size for auto pagination
    try:
        import fcntl, termios, struct
        size = struct.unpack('hh', fcntl.ioctl(0, termios.TIOCGWINSZ, '1234'))
    except:
# FIXME - make this work with windows
        size = (80, 20)

# configure parsing loop
    my_parser = parser.parser()
    my_parser.config = config
    my_parser.config_name = args.current
    my_parser.config_filepath = config_filepath
# generic thread pool for background processes
    my_parser.thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=my_parser.thread_max)

# add discovery url
    if args.keystone is not None:
        my_parser.config.set(args.current, 'keystone', args.keystone)
    if my_parser.config.has_option(args.current, 'keystone'):
        my_parser.keystone = keystone.keystone(my_parser.config.get(args.current, 'keystone'))

# add endpoints
    try:
        for mount in endpoints:
            endpoint = endpoints[mount]
            logging.info("Connecting [%s] endpoint to [%s]" % (endpoint['type'], mount))
# create remote client and associate mount
            myclient = my_parser.remotes_new(endpoint)
            my_parser.remotes_add(mount, myclient)

# added all remotes without error - save to config
        my_parser.remotes_config_save()

    except Exception as e:
        logging.info(str(e))

# just in case the terminal height calculation returns a very low value
    my_parser.terminal_height = max(size[0], my_parser.terminal_height)

# restart script
    if args.output is not None:
        my_parser.script_output = args.output

# TAB completion
# strange hackery required to get tab completion working under OS-X and also still be able to use the b key
# REF - http://stackoverflow.com/questions/7124035/in-python-shell-b-letter-does-not-work-what-the
    try:
        if 'libedit' in readline.__doc__:
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")
    except:
        logging.info("No readline module; tab completion unavailable")

# build non interactive input iterator
    input_list = []
    my_parser.interactive = True
    if args.script:
        input_list = itertools.chain(input_list, open(args.script))
        my_parser.interactive = False
# FIXME - stricly, need regex to avoid split on quote protected &&
    if len(args.command) != 0:
        input_list = itertools.chain(input_list, args.command.split("&&"))
        my_parser.interactive = False

# interactive or input iterator (scripted)
    if my_parser.interactive:
        print(" === pshell: type 'help' for a list of commands ===")
        my_parser.loop_interactively()
    else:
        for item in input_list:
            line = item.strip()
            try:
                print("%s:%s> %s" % (args.current, my_parser.cwd, line))
                my_parser.onecmd(line)
            except KeyboardInterrupt:
                print(" Interrupted by user")
                exit(-1)
            except SyntaxError:
                print(" Syntax error: for more information on commands type 'help'")
                exit(-1)
            except Exception as e:
                print(str(e))
                exit(-1)


if __name__ == '__main__':
    main()
