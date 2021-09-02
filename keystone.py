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
        print("init: %s" % self.url)
        # TODO -> get /v3 ?

#------------------------------------------------------------
# necessary, but not sufficient
    def is_authenticated(self):
        if self.token is None:
            return(False)

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
            print("Authenticated as user [%s]" % self.user)

#------------------------------------------------------------
    def get_credentials(self):
# get user ec2 credentials 
            ec2_url = "/v3/users/%s/credentials/OS-EC2" % self.user
            headers = {"X-Auth-Token": self.token, "Content-type": "application/json"}
            request = urllib.request.Request(self.url + ec2_url, headers=headers)
            response = urllib.request.urlopen(request)
            reply = response.read()
            credential_list = json.loads(reply)['credentials']
            for item in credential_list:
                print("[%s] = %s : %s" % (item['tenant_id'], item['access'], item['secret']))

#------------------------------------------------------------
    def get_projects(self):
# get user project membership
            projects_url = "/v3/users/%s/projects" % self.user
            headers = {"X-Auth-Token": self.token, "Content-type": "application/json"}
            request = urllib.request.Request(self.url + projects_url, headers=headers)
            response = urllib.request.urlopen(request)
            reply = response.read()
            project_list = json.loads(reply)
            for entry in project_list['projects']:
                project_id = entry['id']
                project_name = entry['name']
                project_enabled = entry['enabled']
                print("%s : %s" % (project_name, project_id))

# TODO - how to best link project (tenant_id) with credentials?
#                ec2 = False
#                for dict_item in credential_list:
#                    print(dict_item['tenant_id'])
# not sure what the hell this is doing but it's wrong
#                    if project_id == dict_item['tentant_id']:
#                    if project_id in dict_item.values():
#                        ec2 = True

