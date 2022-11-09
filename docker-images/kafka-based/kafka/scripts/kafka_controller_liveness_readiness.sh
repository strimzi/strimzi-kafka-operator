#!/usr/bin/env bash
set -e

# Test listening on control plane listener 9090
# We mark the controller ready once it is listening, even if it isn't part of a quorum yet
netstat -lnt | grep -Eq 'tcp6?[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^ ]+:9090.*LISTEN[[:space:]]*'
