#!/usr/bin/env bash
set -e

echo "Build reason: ${BUILD_REASON}"
echo "Source branch: ${BRANCH}"

CHANGED_DERIVED=$(git diff --name-status -- packaging/install/ packaging/helm-charts/ documentation/modules/appendix_crds.adoc cluster-operator/src/main/resources/cluster-roles)
GENERATED_FILES=$(git ls-files --other --exclude-standard -- packaging/install/ packaging/helm-charts/ cluster-operator/src/main/resources/cluster-roles api/src/test/resources/io/strimzi/api/kafka/model)
if [ -n "$CHANGED_DERIVED" ] || [ -n "$GENERATED_FILES" ] ; then
    if [ -n "$CHANGED_DERIVED" ] ; then
        echo "ERROR: Uncommitted changes in derived resources:"
        echo "$CHANGED_DERIVED"
    fi
  
    if [ -n "$GENERATED_FILES" ] ; then
        echo "ERROR: Uncommitted changes in generated resources:"
        echo "$GENERATED_FILES"
    fi
  
    echo "Run the following to add up-to-date resources:"
    echo "  mvn clean verify -DskipTests -DskipITs \\"
    echo "    && make crd_install \\"
    echo "    && make dashboard_install \\"
    echo "    && make helm_install \\"
    echo "    && git add packaging/install/ packaging/helm-charts/ documentation/modules/appendix_crds.adoc cluster-operator/src/main/resources/cluster-roles \\"
    echo "    && git commit -s -m 'Update derived resources'"
    exit 1
fi
