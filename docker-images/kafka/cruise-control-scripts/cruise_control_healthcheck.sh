#!/usr/bin/env bash
set -e

ARGS=()

if [ "$STRIMZI_CC_API_AUTHENTICATION_ENABLED" = true ] ; then
  # Disable certificate verification
  ARGS+=("-k")
  SCHEME="https"
else
  SCHEME="http"
fi

if [ "$STRIMZI_CC_API_AUTHORIZATION_ENABLED" = true ] ; then
  ARGS+=(--user "${API_USER}:$(cat /opt/cruise-control/api-auth-config/cruise-control.apiUserPassword)")
fi

curl "${ARGS[@]}" "${SCHEME}://127.0.0.1:${API_PORT}${API_HEALTHCHECK_PATH}"