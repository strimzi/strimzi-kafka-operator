#!/usr/bin/env bash
set -e

OK=$(echo ruok | nc 127.0.0.1 12181)
if [ "$OK" == "imok" ]; then
    exit 0
else
    exit 1
fi