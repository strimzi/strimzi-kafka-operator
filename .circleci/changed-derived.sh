#!/usr/bin/env bash
set -e

CHANGED_DERIVED=$(git diff --name-status -- install/ helm-charts/ documentation/book/appendix_crds.adoc cluster-operator/src/main/resources/cluster-roles)
if [ -n "$CHANGED_DERIVED" ] ; then
  echo "ERROR: Uncommitted changes in derived resources:"
  echo "$CHANGED_DERIVED"
  echo "Run the following to add up-to-date resources:"
  echo "  mvn clean verify -DskipTests -DskipITs \\"
  echo "    && git add install/ helm-charts/ documentation/book/appendix_crds.adoc cluster-operator/src/main/resources/cluster-roles"
  echo "    && git commit -m 'Update derived resources'"
  exit 1
fi