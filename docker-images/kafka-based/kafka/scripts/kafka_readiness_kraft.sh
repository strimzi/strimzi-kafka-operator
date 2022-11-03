#!/usr/bin/env bash
set -e

# Test broker readiness
test -f /var/opt/kafka/kafka-ready

# Test controller readiness
. ./kafka_controller_liveness_readiness.sh
