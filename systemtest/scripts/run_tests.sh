#!/usr/bin/env bash
TESTCASE=$1

if [[ -n "$TESTCASE" ]]; then
    EXTRA_ARGS="-Dit.test=$TESTCASE"
fi

mvn -e -q verify -pl systemtest -P systemtests -Djava.net.preferIPv4Stack=true -DtrimStackTrace=false -DjunitTags=acceptance $EXTRA_ARGS
