#!/usr/bin/env bash
set -e

echo "Build reason: ${BUILD_REASON}"
echo "Source branch: ${BRANCH}"

# The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
if [ ${JAVA_MAJOR_VERSION} -eq 11 ] ; then
    # some parts of the workflow should be done only one on the main build which is currently Java 11
    export MAIN_BUILD="TRUE"
fi

export DOCKER_ORG=${DOCKER_ORG:-strimzici}
export DOCKER_REGISTRY=${DOCKER_REGISTRY:-quay.io}
export DOCKER_TAG=$COMMIT

make docu_check
make spotbugs
make shellcheck

make crd_install
make helm_install
make docker_build
make docu_html
make docu_htmlnoheader

# Run tests for strimzi-test-container
# We need to tag built images even in fork repos to make images available for test container locally
DOCKER_REGISTRY=quay.io DOCKER_ORG=strimzi DOCKER_TAG=latest make docker_tag
mvn test -f test-container/pom.xml

if [ ! -e documentation/modules/appendix_crds.adoc ] ; then
    echo "ERROR: documentation/modules/appendix_crds.adoc does not exist!"
    exit 1
fi

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
    echo "    && make helm_install \\"
    echo "    && git add install/ helm-charts/ documentation/modules/appendix_crds.adoc cluster-operator/src/main/resources/cluster-roles \\"
    echo "    && git commit -s -m 'Update derived resources'"
    exit 1
fi

# Push artifatcs (Docker containers, JARs, docs)
if [ "$BUILD_REASON" == "PullRequest" ] ; then
    echo "Building Pull Request - nothing to push"
elif [[ "$BRANCH" != "refs/tags/"* ]] && [ "$BRANCH" != "refs/heads/main" ]; then
    echo "Not in main branch and not in release tag - nothing to push"
else
    if [ "${MAIN_BUILD}" == "TRUE" ] ; then
        echo "Main build on main branch or release tag - going to push to Docker Hub, Nexus and website"
        
        echo "Login into Docker Hub ..."
        docker login -u $DOCKER_USER -p $DOCKER_PASS $DOCKER_REGISTRY

        export DOCKER_ORG=strimzi
        
        if [ "$BRANCH" == "refs/heads/main" ]; then
            export DOCKER_TAG="latest"
        else
            export DOCKER_TAG="${BRANCH#refs/tags/}"
        fi
 
        echo "Pushing to docker org $DOCKER_ORG"
        make docker_push

        if [ "$BRANCH" == "refs/heads/main" ]; then
            make docu_pushtowebsite
        fi

        make pushtonexus
    else
        echo "Not in main build - nothing to do"
    fi
fi
