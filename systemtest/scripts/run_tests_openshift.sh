#!/usr/bin/env bash
CURDIR=`readlink -f \`dirname $0\``
source ${CURDIR}/test_func.sh

TESTCASE=${1:-.*ST}
JUNIT_TAGS=${2:-acceptance}
TEST_PROFILE=${3:-systemtests}
DEFAULT_OPENSHIFT_PROJECT="myproject"

if [ -n "$TESTCASE" ]; then
    EXTRA_ARGS="-Dit.test=$TESTCASE"
fi

echo "Running tests with profile: ${TEST_PROFILE}, tests: ${TESTCASE}"

failure=0

export KUBERNETES_API_TOKEN=$(oc whoami -t)
export KUBERNETES_API_URL=$(oc whoami --show-server=true)
export KUBERNETES_NAMESPACE=${DEFAULT_OPENSHIFT_PROJECT}

#run tests
run_test ${TESTCASE} ${JUNIT_TAGS} ${TEST_PROFILE} || failure=$(($failure + 1))

if [[ ${failure} -gt 0 ]]; then
    echo "Systemtests failed"
    exit 1
fi
echo "End of run_test_components.sh"
