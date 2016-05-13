#! /usr/bin/env python

import sys, os

filename = sys.argv[1]

f = open(filename, "a");

for serverid in range(1,4):
    if str(serverid) == os.environ['ZOOKEEPER_ID']:
        line = "server.{0}=0.0.0.0:2888:3888\n".format(serverid)
    else:
        envname = "ZOOKEEPER{0}_SERVICE_HOST".format(serverid)
        line = "server.{0}={1}:2888:3888\n".format(serverid, os.environ[envname])
    f.write(line)

f.close()