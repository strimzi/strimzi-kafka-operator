#!/usr/bin/env bash
set -e

SCHEME="http"
HOST=$(hostname | rev | cut -d "-" -f3- | rev):$API_PORT
ARGS="--resolve ${HOST}:0.0.0.0"

if [ "$API_AUTHENTICATION_ENABLED" = true ] ; then
  SCHEME="https"
  ARGS="${ARGS}
  --cert-type P12 \
  --cert /etc/tls-sidecar/cc-certs/cruise-control.p12 \
  --pass $(cat /etc/tls-sidecar/cc-certs/cruise-control.password) \
  --cacert /etc/tls-sidecar/cc-certs/cruise-control.crt"
fi

if [ "$API_AUTHORIZATION_ENABLED" = true ] ; then
  ARGS="${ARGS} --user ${API_USER}:$(cat /opt/cruise-control/api-auth-config/cruise-control.apiUserPassword)"
fi

curl ${ARGS} "${SCHEME}://${HOST}${API_HEALTHCHECK_PATH}"

