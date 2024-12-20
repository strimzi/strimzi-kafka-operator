#!/usr/bin/env bash
set -e

file=/tmp/strimzi.properties
test -f $file

# During migration, the process.roles field can be still not set on broker only nodes
# so, because grep would fail, the "|| true" operation allows to return empty roles result
roles=$(grep -Po '(?<=^process.roles=).+' "$file" || true)
if [[ "$roles" =~ "controller" ]] && [[ ! "$roles" =~ "broker" ]]; then
  # For controller only mode, check if it is listening on port 9090 (configured in controller.listener.names).
  netstat -lnt | grep -Eq 'tcp6?[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^ ]+:9090.*LISTEN[[:space:]]*'
else
  # For combined or broker only mode, check readiness via HTTP endpoint exposed by Kafka Agent.
  # The endpoint returns 204 when broker state is 3 (RUNNING).
  curl http://localhost:8080/v1/ready/ --fail
fi
