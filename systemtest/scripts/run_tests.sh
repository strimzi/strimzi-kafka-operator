#!/usr/bin/env bash
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
TESTCASE=${1:-.*ST}
TEST_PROFILE=${2:-smoke}

function run_test() {
    TESTCASE=$1
    PROFILE=${2:-systemtests}

    if [[ -n "${TESTCASE}" ]]; then
        # Concatenate user specified EXTRA_TEST_ARGS if supplied
        EXTRA_TEST_ARGS="-Dit.test=${TESTCASE} ${EXTRA_TEST_ARGS}"
    fi

    echo "Extra args for tests: ${EXTRA_TEST_ARGS}"

    pushd "${SCRIPT_DIR}"/../.. && \
    mvn -B verify \
      -pl systemtest \
      -am \
      -P"${PROFILE}" \
      -Djava.net.preferIPv4Stack=true \
      -DfailIfNoTests=false \
      -Djansi.force=true \
      -Dstyle.color=always \
      -DtrimStackTrace=false \
      ${EXTRA_TEST_ARGS}
    popd || exit
}

echo "Running tests with profile: ${TEST_PROFILE}, tests: ${TESTCASE}"

failure=0

#run tests
run_test "${TESTCASE}" "${TEST_PROFILE}" || failure=$(($failure + 1))

if [[ ${failure} -gt 0 ]]; then
    echo "Systemtests failed"
    exit 1
fi
echo "End of run_tests.sh"
