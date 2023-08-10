#!/usr/bin/env bash
set -e

file=/tmp/strimzi.properties
test -f $file
roles=$(awk -F "process.roles=" '{print $2}' "$file")
if [[ "$roles" =~ "controller" ]] && [[ ! "$roles" =~ "broker" ]]; then
  # For controller only mode, check if it is listening on port 9090 (configured in controller.listener.names).
  netstat -lnt | grep -E 'tcp?[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^ ]+:9090+[[:space:]]+[^ ]+[[:space:]]+LISTEN*'
else
  # For combined or broker only mode, check readiness via HTTP endpoint exposed by Kafka Agent.
  # The endpoint returns 204 when broker state is 3 (RUNNING).
  curl http://localhost:8080/v1/ready/ --fail
fi
