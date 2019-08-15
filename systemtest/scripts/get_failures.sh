#!/usr/bin/env bash

JSON_FILE=failures.json

FAILURES=$(find ~/Downloads/failsafe-reports/ -name 'TEST*.xml' -type f -print0 | xargs -0 grep "<testcase.*time=\"[0-9]*,\{0,1\}[0-9]\{1,3\}\..*[^\/]>$" | cut -d '"'  -f 2,4)
echo ${FAILURES} > ${JSON_FILE}
cat ${JSON_FILE}

echo "Creating body ..."

TMP=$(cat ${JSON_FILE} | sed 's@ST @ST\\n - @g' | sed 's@"@ in @g')

BODY="{\"body\":\"**Test Failures**\n- ${TMP}\"}"
echo ${BODY} > ${JSON_FILE}
cat ${JSON_FILE}