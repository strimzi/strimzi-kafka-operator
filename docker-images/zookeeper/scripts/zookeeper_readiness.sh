#!/bin/bash
 ZOOKEEPER_ID=$(hostname | awk -F'-' '{print $NF+1}')
CLIENT_PORT=$(expr 21810 + $ZOOKEEPER_ID - 1)
 echo stat | nc 127.0.0.1 $CLIENT_PORT | grep -Eq '^Mode: (leader|follower|standalone)$'
exit $?
