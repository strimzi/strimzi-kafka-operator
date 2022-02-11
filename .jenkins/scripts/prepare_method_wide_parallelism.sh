#!/usr/bin/env bash

echo "[INFO] Prepare system tests for METHOD-WIDE parallelism"
find systemtest/src/test/java/io/strimzi/systemtest/ -type f -name "*.java" -print0 | xargs -0 sed -i '' -e 's/^@ParallelSuite\$//g'
find systemtest/src/test/java/io/strimzi/systemtest/ -type f -name "*.java" -print0 | xargs -0 sed -i '' -e 's/^import io.strimzi.test.annotations.ParallelSuite;\$//g'
