#!/usr/bin/env bash

set -eu

source ./.checksums

RETURN_CODE=0

# Helm Charts
CHECKSUM="$(find ./helm-charts/ -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum)"
echo "checksum of ./helm-charts/ is CHECKSUM=${CHECKSUM}"

if [ "$CHECKSUM" != "$HELM_CHART_CHECKSUM" ]; then
  echo "ERROR checksum of ./helm-charts does not match expected"
  echo "expected ${HELM_CHART_CHECKSUM}"
  echo "found ${CHECKSUM}"
  echo "if your changes are not related to a release please check your changes into"
  echo "./packaging/helm-charts"
  echo "instead of ./helm-charts"
  echo ""
  echo "if this is part of a release instead update the checksum i.e."
  echo "HELM_CHART_CHECKSUM=\"${HELM_CHART_CHECKSUM}\""
  echo "->"
  echo "HELM_CHART_CHECKSUM=\"${CHECKSUM}\""
  RETURN_CODE=$((RETURN_CODE+1))
fi


# install
CHECKSUM="$(find ./install/ -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum)"
echo "checksum of ./install/ is CHECKSUM=${CHECKSUM}"

if [ "$CHECKSUM" != "$INSTALL_CHECKSUM" ]; then
  echo "ERROR checksum of ./install does not match expected"
  echo "expected ${INSTALL_CHECKSUM}"
  echo "found ${CHECKSUM}"
  echo "if your changes are not related to a release please check your changes into"
  echo "./packaging/install"
  echo "instead of ./install"
  echo ""
  echo "if this is part of a release instead update the checksum i.e."
  echo "INSTALL_CHECKSUM=\"${INSTALL_CHECKSUM}\""
  echo "->"
  echo "INSTALL_CHECKSUM=\"${CHECKSUM}\""
  RETURN_CODE=$((RETURN_CODE+1))
fi

# examples
CHECKSUM="$(find ./examples/ -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum)"
echo "checksum of ./examples/ is CHECKSUM=${CHECKSUM}"

if [ "$CHECKSUM" != "$EXAMPLES_CHECKSUM" ]; then
  echo "ERROR checksum of ./install does not match expected"
  echo "expected ${EXAMPLES_CHECKSUM}"
  echo "found ${CHECKSUM}"
  echo "if your changes are not related to a release please check your changes into"
  echo "./packaging/examples"
  echo "instead of ./examples"
  echo ""
  echo "if this is part of a release instead update the checksum i.e."
  echo "EXAMPLES_CHECKSUM=\"${EXAMPLES_CHECKSUM}\""
  echo "->"
  echo "EXAMPLES_CHECKSUM=\"${CHECKSUM}\""
  RETURN_CODE=$((RETURN_CODE+1))
fi

exit $RETURN_CODE