#!/usr/bin/env bash
set -e

# Extracts strimzi-kafka-oauth dependency version from third-party-libraries pom.xml
function get_strimzi_oauth_version {
    grep -m 1 strimzi-oauth.version docker-images/artifacts/kafka-thirdparty-libs/*/pom.xml | head -n 1 | sed -e 's/.*>\(.*\)<.*/\1/'
}