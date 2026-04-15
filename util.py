"""
Shared helpers for pshell.
"""

import os
import json
import logging
import configparser


def normalize_endpoint(endpoint):
    """
    For a single endpoint dict, if type is mflux, ensure protocol is https and
    encrypt is True. Returns True if endpoint was modified.
    """
    if not isinstance(endpoint, dict) or endpoint.get('type') != 'mflux':
        return False
    changed = False
    if endpoint.get('protocol') != 'https':
        endpoint['protocol'] = 'https'
        changed = True
    if endpoint.get('encrypt') is not True:
        endpoint['encrypt'] = True
        changed = True
    return changed


def normalize_config_file(config_filepath):
    """
    Make sure the mediaflux endpoints in the config file use 'https' and 'encrypt' is True.
    """
    if not os.path.isfile(config_filepath):
        return
    try:
        if os.path.getsize(config_filepath) == 0:
            return
    except OSError:
        return
    cfg = configparser.ConfigParser(interpolation=None)
    try:
        read_ok = cfg.read(config_filepath)
        if not read_ok:
            return
    except (configparser.Error, OSError):
        return
    if not cfg.has_section('pawsey') or not cfg.has_option('pawsey', 'endpoints'):
        return
    try:
        endpoints = json.loads(cfg.get('pawsey', 'endpoints'))
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(endpoints, dict):
        return
    changed = False
    for endpoint_name in ('portal', 'public'):
        endpoint = endpoints.get(endpoint_name)
        if normalize_endpoint(endpoint):
            changed = True
    if not changed:
        return
    cfg.set('pawsey', 'endpoints', json.dumps(endpoints))
    tmp_path = config_filepath + '.tmp'
    try:
        with open(tmp_path, 'w') as f:
            cfg.write(f)
        os.replace(tmp_path, config_filepath)
    except OSError as e:
        try:
            if os.path.isfile(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass
        logging.warning("Could not normalize pawsey endpoints in [%s]: %s" % (config_filepath, e))
