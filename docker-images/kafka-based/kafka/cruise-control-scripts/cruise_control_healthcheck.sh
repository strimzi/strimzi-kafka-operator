#!/usr/bin/env bash
set -e

ARGS=()

if [ "$STRIMZI_CC_API_SSL_ENABLED" = true ] ; then
  ARGS+=(--cacert /etc/cruise-control/cc-certs/cruise-control.crt)
  SCHEME="https"
else
  SCHEME="http"
fi

if [ "$STRIMZI_CC_API_AUTH_ENABLED" = true ] ; then
  ARGS+=(--user "${API_HEALTHCHECK_USERNAME}:$(cat /opt/cruise-control/api-auth-config/healthcheck.password)")
fi

curl "${ARGS[@]}" "${SCHEME}://localhost:${API_PORT}${API_HEALTHCHECK_PATH}"