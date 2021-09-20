#!/usr/bin/python

"""
This module is a Python 3.x (standard lib only) implementation of a simple keystone client
Author: Sean Fleming
"""

import json
import urllib

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

#------------------------------------------------------------
# connect to keystone and acquire user details via mflux sso
    def connect(self, mfclient, refresh=False):
        if self.token == None or refresh == True:
            print("keystone.connect()")
            self.sso_mfclient(mfclient)
            self.get_projects()
            self.get_credentials()

#------------------------------------------------------------
    def whoami(self):
        print("=== %s ===" % self.url)
        for name in self.project_dict.keys():
            print("    %s" % name)

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
#        print("Authenticated as user [%s]" % self.user)

#------------------------------------------------------------
    def sso_mfclient(self, mfclient):
        print("mflux sso...")
        xml_reply = mfclient.aterm_run('user.self.describe')
        elem = xml_reply.find(".//user")
        user = elem.attrib['user']
        xml_reply = mfclient.aterm_run("secure.wallet.get :key ldap")
        elem = xml_reply.find(".//value")
        self.get_auth_token(user, elem.text)

#------------------------------------------------------------
    def get_credentials(self):
        print("caching credentials...")
# get user ec2 credentials 
        ec2_url = "/v3/users/%s/credentials/OS-EC2" % self.user
        headers = {"X-Auth-Token": self.token, "Content-type": "application/json"}
        request = urllib.request.Request(self.url + ec2_url, headers=headers)
        response = urllib.request.urlopen(request)
        reply = response.read()
        self.credential_list = json.loads(reply)['credentials']

#------------------------------------------------------------
    def get_projects(self):
        print("caching projects...")
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

#------------------------------------------------------------
    def s3_candidate_find(self):
# TODO - could we query the magenta url as well???
        for project_name in self.project_dict.keys():
            for credential in self.credential_list:
                if credential['tenant_id'] == self.project_dict[project_name]:
                    return (project_name, credential['access'], credential['secret'])

#------------------------------------------------------------
    def credentials_print(self, project):
        for project_name in self.project_dict.keys():
            print("project = %s" % project_name)
            for credential in self.credential_list:
                if credential['tenant_id'] == self.project_dict[project_name]:
                    print("    access = %s : secret = %s" % (credential['access'], credential['secret']))

#------------------------------------------------------------
    def credentials_create(self, project):

        print("Creating ec2 credential for project = %s" % project)
        if project in self.project_dict.keys():
            project_id = self.project_dict[project]
        else:
            project_id = project

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

