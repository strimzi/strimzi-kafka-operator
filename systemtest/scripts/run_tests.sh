#!/usr/bin/env bash

TESTCASE=${1:-.*ST}
JUNIT_TAGS=${2:-acceptance}
TEST_PROFILE=${3:-systemtests}

function run_test() {
    TESTCASE=$1
    JUNIT_TAGS=$2
    PROFILE=${3:-systemtests}

    if [[ -n "${TESTCASE}" ]]; then
        EXTRA_TEST_ARGS="-Dit.test=${TESTCASE}"
    fi

    if [[ -n "${JUNIT_TAGS}" ]]; then
        EXTRA_TEST_ARGS="${EXTRA_TEST_ARGS} -DjunitTags=${JUNIT_TAGS}"
    fi

    echo "Extra args for tests: ${EXTRA_TEST_ARGS}"

    mvn -B verify -pl systemtest -P${PROFILE} -Djava.net.preferIPv4Stack=true -DfailIfNoTests=false -Djansi.force=true -Dstyle.color=always ${EXTRA_TEST_ARGS}
}

echo "Running tests with profile: ${TEST_PROFILE}, tests: ${TESTCASE}"

failure=0

#run tests
run_test ${TESTCASE} ${JUNIT_TAGS} ${TEST_PROFILE} || failure=$(($failure + 1))

if [[ ${failure} -gt 0 ]]; then
    echo "Systemtests failed"
    exit 1
fi
echo "End of run_tests.sh"
