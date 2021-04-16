#!/usr/bin/env bash
set -e

POM_FILE="$(dirname $(realpath $0))/../pom.xml"

# Extracts strimzi-kafka-oauth dependency version from pom.xml:
# "strimzi_oauth_version": String containing the version
function get_strimzi_oauth_version {
    strimzi_oauth_version=$(grep strimzi-oauth.version < $POM_FILE | grep -m 1 strimzi-oauth.version | sed -e 's/.*>\(.*\)<.*/\1/')
}