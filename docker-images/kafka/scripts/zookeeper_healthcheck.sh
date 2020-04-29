#!/usr/bin/env bash

OK=$(echo ruok | nc 127.0.0.1 2181)
if [ "$OK" == "imok" ]; then
    exit 0
else
    exit 1
fi