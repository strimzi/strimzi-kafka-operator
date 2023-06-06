#!/bin/sh -eux


TEST_LOG_DIR="${TMT_PLAN_DATA}/../discover/default-0/tests/systemtest/target/logs"
XUNIT_LOG_DIR="${TMT_PLAN_DATA}/../discover/default-0/tests/systemtest/target/failsafe-reports"

TARGET_DIR="${TMT_PLAN_DATA}"
LOGS_DIR="${TARGET_DIR}/logs"
XUNIT_DIR="${TARGET_DIR}/xunit"

mkdir -p "${LOGS_DIR}"
mkdir -p "${XUNIT_DIR}"

cp -R "${TEST_LOG_DIR}" "${LOGS_DIR}" || true
cp -R "${XUNIT_LOG_DIR}" "${XUNIT_DIR}" || true
