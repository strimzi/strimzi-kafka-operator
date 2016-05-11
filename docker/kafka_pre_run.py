#! /usr/bin/env python

import sys, os, fcntl

if len(sys.argv) > 1:
    basedir = sys.argv[1]
else:
    basedir = "/tmp/kafka/"

if len(sys.argv) > 2:
    basename = sys.argv[2]
else:
    basename = "kafka-logs"

brokerid = 1
done = False
name = ""

while not done:
    name = "%s-%i" % (basename, brokerid)
    datadir = basedir + name
    lockfile = datadir + "/.lock"
    if os.path.isdir(datadir) and os.path.isfile(lockfile):
        try:
            # check for the .lock file
            f = open(lockfile, "w+")
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.lockf(f, fcntl.LOCK_UN)
            done = True
        except Exception as e:
            brokerid = brokerid + 1
    else:
        done = True
        
print brokerid