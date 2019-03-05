#!/usr/bin/env bash
CURDIR=`readlink -f \`dirname $0\``
source ${CURDIR}/test_func.sh

TESTCASE=${1:-.*ST}
JUNIT_TAGS=${2:-acceptance}
TEST_PROFILE=${3:-systemtests}
DEFAULT_KUBERNETES_NAMESPACE="myproject"

if [[ -n "$TESTCASE" ]]; then
    EXTRA_ARGS="-Dit.test=$TESTCASE"
fi

echo "Running tests with profile: ${TEST_PROFILE}, tests: ${TESTCASE}"

failure=0

API_URL=$(kubectl config view --minify | grep server | cut -f 2- -d ":" | tr -d " ")
API_TOKEN=$(kubectl describe secret $(kubectl get serviceaccount default -o jsonpath='{.secrets[0].name}') | grep -E '^token' | cut -f2 -d':' | tr -d " ")

export KUBERNETES_API_URL=${KUBERNETES_API_URL:-${API_URL}}
export KUBERNETES_API_TOKEN=${KUBERNETES_API_TOKEN:-${API_TOKEN}}
export KUBERNETES_NAMESPACE=${DEFAULT_KUBERNETES_NAMESPACE}

#run tests
run_test ${TESTCASE} ${JUNIT_TAGS} ${TEST_PROFILE} || failure=$(($failure + 1))

if [[ ${failure} -gt 0 ]]; then
    echo "Systemtests failed"
    exit 1
fi
echo "End of run_test_kubernetes.sh"
