#!/usr/bin/env bash

TESTCASE=${1:-.*ST}
TEST_PROFILE=${3:-acceptance}

function run_test() {
    TESTCASE=$1
    PROFILE=${2:-systemtests}

    if [[ -n "${TESTCASE}" ]]; then
        EXTRA_TEST_ARGS="-Dit.test=${TESTCASE}"
    fi

    echo "Extra args for tests: ${EXTRA_TEST_ARGS}"

    mvn -B verify -pl systemtest -P${PROFILE} -Djava.net.preferIPv4Stack=true -DfailIfNoTests=false -Djansi.force=true -Dstyle.color=always ${EXTRA_TEST_ARGS}
}

echo "Running tests with profile: ${TEST_PROFILE}, tests: ${TESTCASE}"

failure=0

#run tests
run_test ${TESTCASE} ${TEST_PROFILE} || failure=$(($failure + 1))

if [[ ${failure} -gt 0 ]]; then
    echo "Systemtests failed"
    exit 1
fi
echo "End of run_tests.sh"
