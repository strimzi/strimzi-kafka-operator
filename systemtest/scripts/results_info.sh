#!/usr/bin/env bash

RESULTS_PATH=${1}
TEST_CASE=${2}
TEST_PROFILE=${3}

JSON_FILE_RESULTS=results.json

function get_test_count () {
    _TEST_TYPE=${1}
    _VALUES=$(find "${RESULTS_PATH}" -name "TEST*.xml" -type f -print0 | xargs -0 sed -n "s#.*${_TEST_TYPE}=\"\([0-9]*\)\" .*#\1#p")
    _TEST_COUNTS_ARR=$(echo "${_VALUES}" | tr " " "\n")
    _TEST_COUNT=0

    for item in ${_TEST_COUNTS_ARR}
    do
        _TEST_COUNT=$((_TEST_COUNT + item))
    done

    echo ${_TEST_COUNT}
}

TEST_COUNT=$(get_test_count "tests")
TEST_ERRORS_COUNT=$(get_test_count "errors")
TEST_SKIPPED_COUNT=$(get_test_count "skipped")

SUMMARY="Test Profile: ${TEST_PROFILE}\n**Test Case:** ${TEST_CASE}\n**TOTAL:** ${TEST_COUNT}\n**PASS:** $((TEST_COUNT - TEST_ERRORS_COUNT - TEST_SKIPPED_COUNT))\n**FAIL:** ${TEST_ERRORS_COUNT}\n**SKIPPED:** ${TEST_SKIPPED_COUNT}"

FAILURES=$(find "${RESULTS_PATH}" -name 'TEST*.xml' -type f -print0 | xargs -0 grep "<testcase.*time=\"[0-9]*,\{0,1\}[0-9]\{1,3\}\..*[^\/]>$" | cut -d '"'  -f 2,4)

echo "Creating body ..."

FAILED_TESTS=$(echo "${FAILURES}" | sed 's@ST @ST\\n - @g' | sed 's@"@ in @g')
SUMMARY=${SUMMARY//\"// in }

if [ -n "${FAILED_TESTS}" ]
then
  FAILED_TEST_BODY="### Test Failures\n- ${FAILED_TESTS}"
fi

if [ "${TEST_COUNT}" == 0 ]
then
  BODY="{\"body\":\"**Build Failed**\"}"
else
  BODY="{\"body\":\"### Test Summary\n${SUMMARY}\n${FAILED_TEST_BODY}\"}"
fi

echo "${BODY}" > ${JSON_FILE_RESULTS}

# Cat created file
cat ${JSON_FILE_RESULTS}
