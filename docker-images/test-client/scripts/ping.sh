#!/usr/bin/env bash
set -x
set -e
./producer.sh
./consumer.sh
