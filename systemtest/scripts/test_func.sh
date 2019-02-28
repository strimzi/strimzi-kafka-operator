#!/usr/bin/env bash

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

    mvn -B verify -pl systemtests -P${PROFILE} -Djava.net.preferIPv4Stack=true -DfailIfNoTests=false -Djansi.force=true -Dstyle.color=always ${EXTRA_TEST_ARGS}
}
