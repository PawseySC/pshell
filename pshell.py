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

#------------------------------------------------------------
def main():
    global build

# server config (section heading) to use
    p = argparse.ArgumentParser(description="pshell help")
    p.add_argument("-c", dest='current', default='data.pawsey.org.au', help="the config name in $HOME/.mf_config to connect to")
    p.add_argument("-i", dest='script', help="input script file containing pshell commands")
    p.add_argument("-o", dest='output', default=None, help="output any failed commands to a script")
    p.add_argument("-v", dest='verbose', default=None, help="set verbosity level (0,1,2)")
    p.add_argument("-u", dest='url', default=None, help="Remote endpoint")
    p.add_argument("-d", dest='domain', default=None, help="login authentication domain")
    p.add_argument("-s", dest='session', default=None, help="session")
    p.add_argument("-m", dest='mount', default='/', help="mount point for remote")
    p.add_argument("--keystone", dest='keystone', default=None, help="A URL to the REST interface for Keystone (Openstack)")
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

# attempt to locate a valid config file
    config_filepath = os.path.expanduser("~/.pshell_config")
    try:
        open(config_filepath, 'a').close()
    except:
        config_filepath = os.path.join(os.getcwd(), ".pshell_config")

    config = configparser.ConfigParser()
    logging.debug("Reading config file: [%s]" % config_filepath)
    config.read(config_filepath)

# attempt to use the current section in the config for connection info
    try:
        endpoints = None 

        if args.url is None:
# existing config and no input URL
            if config.has_section(args.current) is True:
                logging.info("No input URL, reading endpoints from existing config [%s]" % args.current)
                endpoints = json.loads(config.get(args.current, 'endpoints'))
            else:
# 1st time default
                logging.info("Initialising default config")
                args.url = 'https://data.pawsey.org.au:443'
                args.domain = 'ivec'
                args.mount = '/projects'

        if endpoints is None:
# if URL supplied or 1st time setup
            logging.info("Creating endpoint from url: [%s]" % args.url)

# WTF - urlparse not extracting the port
            aaa = urllib.parse.urlparse(args.url)
# HACK - workaround for urlparse not extracting port, despite the doco indicating it should
            p = '(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*'
            m = re.search(p,args.url)
            port = m.group('port')
            args.current = aaa.hostname

# FIXME - historically, have to assume it's mflux but now could be s3 
# FIXME - could adopt a scheme where we use "mflux://data.pawsey.org.au:443" and "s3://etc" ... and assume the proto from the port
            endpoint = {'name':args.current, 'type':'mfclient', 'protocol':aaa.scheme, 'server':aaa.hostname, 'port':port, 'domain':args.domain }
# FIXME - session="" needs to be strongly enforced or can get some wierd bugs
            endpoint['session'] = ""
            endpoint['token'] = ""

# no such config section - add and save
            logging.info("Saving section: [%s] in config" % args.current)

            endpoints = { args.mount:endpoint }

            config[args.current] = {'endpoints':json.dumps(endpoints)}

            with open(config_filepath, 'w') as f:
                config.write(f)

    except Exception as e:
        logging.debug(str(e))
        logging.info("No remote endpoints configured.")


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
            logging.info("Connecting [%s] endpoint on [%s]" % (endpoint['type'], mount))
            if endpoint['type'] == 'mfclient':
                myclient = mfclient.mf_client(protocol=endpoint['protocol'], server=endpoint['server'], port=endpoint['port'], domain=endpoint['domain'])
# CLI overrides
                if args.session is not None:
                    endpoint['session'] = args.session
                if args.domain is not None:
                    endpoint['domain'] = args.domain
# TODO - remotes to accept endpoint as initialiser
                if 'session' in endpoint:
                    myclient.session = endpoint['session']
                if 'token' in endpoint:
                    myclient.token = endpoint['token']
                if 'domain' in endpoint:
                    myclient.domain = endpoint['domain']
            elif endpoint['type'] == 's3':
                myclient = s3client.s3_client(host=endpoint['host'], access=endpoint['access'], secret=endpoint['secret'])
            else:
                myclient = client()

# associate client with mount
            my_parser.remotes_add(mount, myclient)

# added all remotes without error - save to config
        my_parser.remotes_config_save()

    except Exception as e:
        logging.error(str(e))

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
        logging.warning("No readline module; tab completion unavailable")

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
# On Windows calling this function is necessary.
#    multiprocessing.freeze_support()
    main()
