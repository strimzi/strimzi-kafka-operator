#!/usr/bin/env bash

WORKPLACE=$1

echo "[INFO] Preparing system tests for METHOD-WIDE parallelism"

find $WORKPLACE"/systemtest/src/test/java/io/strimzi/systemtest" -type f -name "*.java" -print0 | xargs -0 sed -i "s/^@ParallelSuite\$//g"
find $WORKPLACE"/systemtest/src/test/java/io/strimzi/systemtest" -type f -name "*.java" -print0 | xargs -0 sed -i "s/^import io.strimzi.test.annotations.ParallelSuite;\$//g"

if [[ $? == 0 ]]
then
    echo "[INFO] METHOD-WIDE parallelism successfully configured"
else
    echo "[ERROR] Some error occured during congiguring parallism!"
    exit 1
fi