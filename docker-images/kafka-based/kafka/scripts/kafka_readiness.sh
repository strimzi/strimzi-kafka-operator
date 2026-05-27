#!/usr/bin/env bash
set -e

file=/tmp/strimzi.properties
test -f $file

# During migration, the process.roles field can be still not set on broker only nodes
# so, because grep would fail, the "|| true" operation allows to return empty roles result
roles=$(grep -Po '(?<=^process.roles=).+' "$file" || true)
if [[ "$roles" =~ "controller" ]] && [[ ! "$roles" =~ "broker" ]]; then
  # For controller only mode:
  #   1. Listener on port 9090 (CONTROLPLANE) must be bound.
  #   2. The local KRaft node must be attached to the metadata quorum as leader/follower —
  #      verified via the Kafka Agent's /v1/controller-ready endpoint, which reads
  #      'kafka.server:type=raft-metrics, current-state' via JMX. This catches cases where the
  #      port is open but the controller never finished registering / is wedged.
  #
  # If the endpoint returns 404 the raft-metrics MBean isn't registered (older Kafka, or very
  # early startup). In that case we fall back to the port-listening check alone, preserving the
  # pre-existing behavior.
  netstat -lnt | grep -Eq 'tcp6?[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^ ]+:9090.*LISTEN[[:space:]]*'

  controller_status=$(curl -sS -o /dev/null -w "%{http_code}" --max-time 5 http://localhost:8080/v1/controller-ready/ || echo "000")
  case "$controller_status" in
    204|404) ;;
    *) exit 1 ;;
  esac
else
  # For combined or broker only mode, check readiness via HTTP endpoint exposed by Kafka Agent.
  # The endpoint returns 204 when broker state is 3 (RUNNING).
  curl http://localhost:8080/v1/ready/ --fail
fi
