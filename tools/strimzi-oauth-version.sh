#!/usr/bin/env bash
set -e

POM_FILE=pom.xml

# Extracts strimzi-kafka-oauth dependency version from pom.xml
function get_strimzi_oauth_version {
    grep -m 1 strimzi-oauth.version "$POM_FILE" | sed -e 's/.*>\(.*\)<.*/\1/'
}