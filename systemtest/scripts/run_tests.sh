#!/bin/sh
TESTCASE=$1

if [ -n "$TESTCASE" ]; then
    EXTRA_ARGS="-Dit.test=$TESTCASE"
fi

mvn -q verify -pl systemtest -Djava.net.preferIPv4Stack=true -DtrimStackTrace=false -Dtags=acceptance $EXTRA_ARGS
