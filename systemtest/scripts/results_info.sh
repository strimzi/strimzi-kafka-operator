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

SUMMARY="**TEST_PROFILE**: ${TEST_PROFILE}\n**TEST_CASE:** ${TEST_CASE}\n**TOTAL:** ${TEST_COUNT}\n**PASS:** $((TEST_COUNT - TEST_ERRORS_COUNT - TEST_SKIPPED_COUNT))\n**FAIL:** ${TEST_ERRORS_COUNT}\n**SKIPPED:** ${TEST_SKIPPED_COUNT}"

FAILED_TESTS=$(find "${RESULTS_PATH}" -name 'TEST*.xml' -type f -print0 | xargs -0 sed -n "s#\(<testcase.*[^\/]>\)#\1#p" | awk -F '"' '{print "- " $2 " in "  $4}')

echo "Creating body ..."

FAILED_TESTS=${FAILED_TESTS//ST//ST\n}

if [ -n "${FAILED_TESTS}" ]
then
  FAILED_TEST_BODY="### :heavy_exclamation_mark: Test Failures :heavy_exclamation_mark:\n${FAILED_TESTS}"
fi

if [ "${TEST_COUNT}" == 0 ]
then
  BODY="{\"body\":\":heavy_exclamation_mark: **Build Failed** :heavy_exclamation_mark:\"}"
else
  if [ "${TEST_ERRORS_COUNT}" == 0 ]
  then
    BODY="{\"body\":\"### :heavy_check_mark: Test Summary :heavy_check_mark:\n${SUMMARY}\n${FAILED_TEST_BODY}\"}"
  else
    BODY="{\"body\":\"### :x: Test Summary :x:\n${SUMMARY}\n${FAILED_TEST_BODY}\"}"
  fi
fi

echo "${BODY}" > ${JSON_FILE_RESULTS}

# Cat created file
cat ${JSON_FILE_RESULTS}
