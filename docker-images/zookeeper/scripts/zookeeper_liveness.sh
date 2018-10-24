#!/bin/bash

ZOOKEEPER_ID=$(hostname | awk -F'-' '{print $NF+1}')
CLIENT_PORT=$(expr 21810 + $ZOOKEEPER_ID - 1)

OK=$(echo ruok | nc 127.0.0.1 $CLIENT_PORT)
if [ "$OK" == "imok" ]; then
    exit 0
else
    exit 1
fi