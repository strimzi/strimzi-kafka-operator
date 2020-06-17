#!/usr/bin/env bash
set -e

# The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
if [ ${JAVA_MAJOR_VERSION} -eq 11 ] ; then
  # some parts of the workflow should be done only one on the main build which is currently Java 11
  export MAIN_BUILD="TRUE"
fi

export PULL_REQUEST=${PULL_REQUEST:-true}
export BRANCH=${BRANCH:-master}
export TAG=${TAG:-latest}
export DOCKER_ORG=${DOCKER_ORG:-strimzici}
export DOCKER_REGISTRY=${DOCKER_REGISTRY:-docker.io}
export DOCKER_TAG=$COMMIT

make shellcheck
make docu_check
make spotbugs

make crd_install
make helm_install
make docker_build

if [ ! -e documentation/modules/appendix_crds.adoc ] ; then
  echo "ERROR: documentation/modules/appendix_crds.adoc does not exist!"
  exit 1
fi

CHANGED_DERIVED=$(git diff --name-status -- install/ helm-charts/ documentation/modules/appendix_crds.adoc cluster-operator/src/main/resources/cluster-roles)
GENERATED_FILES=$(git ls-files --other --exclude-standard -- install/ helm-charts/ cluster-operator/src/main/resources/cluster-roles)
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
  echo "    && make helm_install \\"
  echo "    && git add install/ helm-charts/ documentation/modules/appendix_crds.adoc cluster-operator/src/main/resources/cluster-roles \\"
  echo "    && git commit -s -m 'Update derived resources'"
  exit 1
fi

# Push to the real docker org
if [ "$PULL_REQUEST" != "false" ] ; then
    make docu_html
    make docu_htmlnoheader
    echo "Building Pull Request - nothing to push"
elif [ "${TRAVIS_REPO_SLUG}" != "strimzi/strimzi-kafka-operator" ]; then
    make docu_html
    make docu_htmlnoheader
    echo "Building in a fork and not in a Strimzi repository. Will not attempt to push anything."
elif [ "$TAG" = "latest" ] && [ "$BRANCH" != "master" ]; then
    make docu_html
    make docu_htmlnoheader
    echo "Not in master branch and not in release tag - nothing to push"
else
    if [ "${MAIN_BUILD}" = "TRUE" ] ; then
        echo "Login into Docker Hub ..."
        docker login -u $DOCKER_USER -p $DOCKER_PASS

        export DOCKER_ORG=strimzi
        export DOCKER_TAG=$TAG
        echo "Pushing to docker org $DOCKER_ORG"
        make docker_push
        if [ "$BRANCH" = "master" ]; then
            make docu_pushtowebsite
        fi
        make pushtonexus
    fi
fi
