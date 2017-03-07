#! /usr/bin/env python

import sys, os, fcntl, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_get_id")

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
    logger.info("try with %s", lockfile)
    if os.path.isdir(datadir) and os.path.isfile(lockfile):
        try:
            logger.info("exists try lock on %s", lockfile)
            # check for the .lock file
            f = open(lockfile, "w+")
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.lockf(f, fcntl.LOCK_UN)
            done = True
        except Exception as e:
            logger.error("error " + str(e))
            brokerid = brokerid + 1
    else:
        done = True
        
logger.info("brokerid %i", brokerid)
logger.info("folder %s", datadir)
        
print brokerid

