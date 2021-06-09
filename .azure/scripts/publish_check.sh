#!/usr/bin/env bash

set -eu

### IMPORTANT ###
# if the below line has changed, this means the ./helm-charts directory has changed
#   the checksum and ./helm-charts directory should only be modified on official releases as part of a release
# if this checksum has changed as part of any non-release specific changes, please apply your changes to the
#   development version of the helm charts in ./packaging/helm-charts
### IMPORTANT ###
HELM_CHART_CHECKSUM="5c7c8dd53dede7f412f02760d66c3396e3c4ed5c  -"

CHECKSUM="$(find ./helm-charts/ -type f -print0 | sort -z | xargs -0 sha1sum | sha1sum)"

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
  echo "HELM_CHART_CHECKSUM=${HELM_CHART_CHECKSUM}"
  echo "->"
  echo "HELM_CHART_CHECKSUM=${CHECKSUM}"
  exit 1
fi