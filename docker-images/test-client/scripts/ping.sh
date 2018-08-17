#!/bin/bash
set -x
set -e
./producer.sh
./consumer.sh
