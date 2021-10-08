#!/usr/bin/python

"""
This module is a Python 3.x (standard lib only) implementation of a simple keystone client
Author: Sean Fleming
"""

import json
import urllib
import logging

#########################################################
class keystone:
    """
    Basic Keystone authentication and communication client
    """

    def __init__(self, url):
        """
        Create a Keystone server connection instance.

        Args:
                url: a STRING which is the full address to the keystone REST API

        Returns:
                A keystone object

        Raises:
                Error if service appears to be unreachable - TODO
        """

        self.url = url
        self.token = None
        self.user = None
        self.project_dict = None
        self.credential_list = None
        self.logger = logging.getLogger('keystone')
# CURRENT
#        self.logger.setLevel(logging.DEBUG)

#------------------------------------------------------------
# connect to keystone and acquire user details via mflux sso
    def connect(self, mfclient, refresh=False):
        self.logger.info("url=%r" % self.url)
# TODO - more sophisticated checking here? eg token expired ...
        if self.token == None or refresh == True:
            self.sso_mfclient(mfclient)
# always done
        self.get_projects()
        self.get_credentials()

#------------------------------------------------------------
    def discover_s3(self, s3client):

        s3endpoint = self.s3_candidate_find()

# TODO - could potentially be a list of accessible endpoints
        if s3endpoint:
# TODO - can we discover the s3 url?
#            s3client.connect('https://nimbus.pawsey.org.au:8080', s3endpoint[1], s3endpoint[2], s3endpoint[0])
            s3client.host = 'https://nimbus.pawsey.org.au:8080'
            s3client.access = s3endpoint[1]
            s3client.secret = s3endpoint[2]
            s3client.prefix = s3endpoint[0]
        else:
# nothing found - clear the deck (ie ec2 credentials deleted)
#            s3client.connect(None, None, None, None)
            s3client.host = None
            s3client.access = None
            s3client.secret = None
            s3client.prefix = None

#------------------------------------------------------------
    def get_auth_token(self, user, password):
        data = '{ "auth": { "identity": { "methods": ["password"], "password": { "user": { "name": "%s", "domain": { "name": "pawsey" }, "password": "%s" } } } } }' % (user, password)
        length = len(data)
        headers = {"Content-type": "application/json", "Content-length": "%d" % length }
        request = urllib.request.Request(self.url + "/v3/auth/tokens", data=data.encode(), headers=headers)
        response = urllib.request.urlopen(request)
        reply = response.read()
        stuff = json.loads(reply)
        self.user = stuff['token']['user']['id']
        self.token = response.headers.get('x-subject-token')
        self.logger.debug("acquired token for user [%s]" % self.user)

#------------------------------------------------------------
    def sso_mfclient(self, mfclient):
        xml_reply = mfclient.aterm_run('user.self.describe')
        elem = xml_reply.find(".//user")
        user = elem.attrib['user']
        self.logger.debug("accessing mediaflux wallet for user [%s]" % user)
# attempt use secure wallet for SSO
        wallet_recreate = False
        xml_reply = mfclient.aterm_run("secure.wallet.can.be.used")
        elem = xml_reply.find(".//can")
        if "true" in elem.text:
            self.logger.info("wallet is accessible")
            try:
                xml_reply = mfclient.aterm_run("secure.wallet.get :key ldap")
                elem = xml_reply.find(".//value")
                # main call
                self.get_auth_token(user, elem.text)
                return
            except Exception as e:
                self.logger.debug(str(e))
        else:
            wallet_recreate = True

# failed due to no key or no useable wallet
        print("Keystone authentication for user [%s] required.")
        password = getpass.getpass("Password: ")
        if wallet_recreate is True:
            mfclient.aterm_run("secure.wallet.recreate :password %s" % password)
# TODO - encrypt so it's not plain text 
        mfclient.aterm_run("secure.wallet.set :key ldap :value %s" % password)
        self.get_auth_token(user, password)

#------------------------------------------------------------
    def get_credentials(self):
# get user ec2 credentials 
        ec2_url = "/v3/users/%s/credentials/OS-EC2" % self.user
        headers = {"X-Auth-Token": self.token, "Content-type": "application/json"}
        request = urllib.request.Request(self.url + ec2_url, headers=headers)
        response = urllib.request.urlopen(request)
        reply = response.read()
        self.credential_list = json.loads(reply)['credentials']
        self.logger.debug("success")

#------------------------------------------------------------
    def get_projects(self):
# get user project membership
        projects_url = "/v3/users/%s/projects" % self.user
        headers = {"X-Auth-Token": self.token, "Content-type": "application/json"}
        request = urllib.request.Request(self.url + projects_url, headers=headers)
        response = urllib.request.urlopen(request)
        reply = response.read()
        project_list = json.loads(reply)
        self.project_dict = {}
        for entry in project_list['projects']:
            project_id = entry['id']
            project_name = entry['name']
            project_enabled = entry['enabled']
            self.project_dict[project_name] = project_id
        self.logger.debug("success")

#------------------------------------------------------------
    def s3_candidate_find(self):
# TODO - could we query the magenta url as well???
        for project_name in self.project_dict():
            for credential in self.credential_list:
                if credential['tenant_id'] == self.project_dict[project_name]:
                    return (project_name, credential['access'], credential['secret'])

#------------------------------------------------------------
    def credentials_print(self, project):
        for project_name in self.project_dict:
            print("project = %s" % project_name)
            for credential in self.credential_list:
                if credential['tenant_id'] == self.project_dict[project_name]:
                    print("    access = %s : secret = %s" % (credential['access'], credential['secret']))

#------------------------------------------------------------
    def credentials_create(self, project):

        if project in self.project_dict.keys():
            project_id = self.project_dict[project]
        else:
            project_id = project

        self.logger.info("Creating ec2 credential for project_id [%s]" % project_id)

#         curl -g -i -X POST https://nimbus.pawsey.org.au:5000/v3/users/0da49abca73d9eaf24fab0a6cc14c6b953494b8008e3f436a4e2223db9c18115/credentials/OS-EC2 -H "Accept: application/json" -H "Content-Type: application/json" -H "User-Agent: python-keystoneclient" -H "X-Auth-Token: {SHA256}1a415063e2de80ec509085a7102068cebb70443a9e7b53f9503882b18c03ad2a" -d '{"tenant_id": "e26b4c0824854f09b13bb7ac6eb6a909"}'
#RESP BODY: {"credential": {"user_id": "0da49abca73d9eaf24fab0a6cc14c6b953494b8008e3f436a4e2223db9c18115", "tenant_id": "e26b4c0824854f09b13bb7ac6eb6a909", "access": "ff64589a348c4fa893e93caa6c19cfbb", "secret": "677cdcfe90a446b48ba69d449d20db12", "trust_id": null, "links": {"self": "https://nimbus.pawsey.org.au:5000/v3/users/0da49abca73d9eaf24fab0a6cc14c6b953494b8008e3f436a4e2223db9c18115/credentials/OS-EC2/ff64589a348c4fa893e93caa6c19cfbb"}}}
# NB: project name has to be *exactly* right ... even a space in front of the project id string will bollocks it
#        print("create credential: [%s]" % project)

# TODO - this the correct/safe way to construct json payload?
        data = json.dumps({ "tenant_id": project_id })
        headers = {"Accept": "application/json", "Content-type": "application/json", "X-Auth-Token": self.token }
        url = "%s/v3/users/%s/credentials/OS-EC2" % (self.url, self.user)
        request = urllib.request.Request(url, data=data.encode(), headers=headers, method="POST")
        response = urllib.request.urlopen(request)
        reply = response.read()
        credential = json.loads(reply)
        print("Created access: %s" % credential['credential']['access'])
        self.get_credentials()

#------------------------------------------------------------
    def credentials_delete(self, access):
 
#REQ: curl -g -i -X DELETE https://nimbus.pawsey.org.au:5000/v3/users/0da49abca73d9eaf24fab0a6cc14c6b953494b8008e3f436a4e2223db9c18115/credentials/OS-EC2/ff64589a348c4fa893e93caa6c19cfbb -H "Accept: application/json" -H "User-Agent: python-keystoneclient" -H "X-Auth-Token: {SHA256}236b14f7b2eabb1b7c411f8a558396ea2fc8e218d73ebb2a1056905f099c5ce2"
        self.logger.info("Destroying ec2 credential for access [%s]" % access)
        url = "%s/v3/users/%s/credentials/OS-EC2/%s" % (self.url, self.user, access)
        headers = {"Accept": "application/json", "X-Auth-Token": self.token }
        request = urllib.request.Request(url, headers=headers, method="DELETE")
        response = urllib.request.urlopen(request)
# expected response.status = 204 (empty content)
        if response.status == 204:
            print("Success")
            self.get_credentials()
        else:
            print("Error")

