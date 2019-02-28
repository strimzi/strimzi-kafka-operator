#!/usr/bin/env bash
CURDIR=`readlink -f \`dirname $0\``
source ${CURDIR}/test_func.sh

TESTCASE=$1
JUNIT_TAGS=$2
TEST_PROFILE=${3:-systemtests}
DEFAULT_OPENSHIFT_PROJECT="myproject"

if [ -n "$TESTCASE" ]; then
    EXTRA_ARGS="-Dit.test=$TESTCASE"
fi

info "Running tests with profile: ${TEST_PROFILE}, tests: ${TESTCASE}"

failure=0

#start system resources logging
${CURDIR}/system-stats.sh > ${ARTIFACTS_DIR}/system-resources.log &
STATS_PID=$!
info "process for checking system resources is running with PID: ${STATS_PID}"

export KUBERNETES_API_TOKEN=$(oc whoami -t)
export KUBERNETES_API_URL=$(oc whoami --show-server=true)
export KUBERNETES_NAMESPACE=${DEFAULT_OPENSHIFT_PROJECT}

#run tests
run_test ${TESTCASE} ${JUNIT_TAGS} ${TEST_PROFILE} || failure=$(($failure + 1))

if [[ ${failure} -gt 0 ]]; then
    echo "Systemtests failed"
    exit 1
fi
info "End of run_test_components.sh"


#stop system resources logging
info "process for checking system resources with PID: ${STATS_PID} will be killed"
kill -9 ${STATS_PID}

mvn -e -q verify -pl systemtest -P systemtests -Djava.net.preferIPv4Stack=true -DtrimStackTrace=false -DjunitTags=acceptance $EXTRA_ARGS
