#!/usr/bin/env python3

import os
import mfclient

if __name__ == "__main__":
    try:
        mf_client = mfclient.mf_client('https', '443', 'data.pawsey.org.au')
# NB: aterm/pshell token re-generation
# secure.identity.token.create :role -type role "system-monitor" :role -type domain "ivec" :max-token-length 32
        mf_client.login(token=os.environ['NAGIOS_MFLUX_TOKEN'])
    except Exception as e:
        print(str(e))
        exit(-1)

# get server status
    reply = mf_client.aterm_run('server.status')
    elem = reply.find('.//uptime')
    uptime = float(elem.text)
    uptime_units = elem.attrib['units']
    if "days" in uptime_units:
        uptime *= 86400
    if "hours" in uptime_units:
        uptime *= 3600
    if "minutes" in uptime_units:
        uptime *= 60
# free memory
    elem = reply.find('.//memory/free')
    free = float(elem.text)
    free_units = elem.attrib['units']
    if "TB" in free_units:
        free *= 1000000
    if "GB" in free_units:
        free *= 1000
    if "KB" in free_units:
        free /= 1000
# running threads
    elem = reply.find('.//threads/total')
    tasks = int(elem.text)

# get network connections
    reply = mf_client.aterm_run('network.describe')
    elem_list = reply.findall('.//connections/active')
    con = 0
    maxcon = 0
    for elem in elem_list:
        con += int(elem.text)
        maxcon += int(elem.attrib['max'])
    warncon = maxcon/2

# nagios output
    print("MEDIAFLUX STATUS | uptime=%ds thread_count=%d;250;500;0;1000 free_memory=%dMB;1024;512;0;232448 network=%d;%d;%d;0;%d" % (uptime, tasks, free, con, warncon, maxcon, maxcon))

