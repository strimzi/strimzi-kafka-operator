#! /usr/bin/env python

import sys, os

if len(sys.argv) > 1:
    filename = sys.argv[1]
else:
    filename = "/tmp/zookeeper.properties"

# opening file in append mode in order to add servers connection configuration
f = open(filename, "a");

# TODO : too much tied to 3 zookeeper servers in the cluster
for serverid in range(1,4):
    
    if str(serverid) == os.environ['ZOOKEEPER_ID']:
        # for the current zookeeper server, the configuration needs to have 0.0.0.0 address
        line = "server.{0}=0.0.0.0:2888:3888\n".format(serverid)
    else:
        # otherwise the zookeeper specific service address
        envname = "ZOOKEEPER{0}_SERVICE_HOST".format(serverid)
        line = "server.{0}={1}:2888:3888\n".format(serverid, os.environ[envname])
        
    f.write(line)

f.close()

