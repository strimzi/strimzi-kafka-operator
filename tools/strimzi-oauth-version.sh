#!/usr/bin/env bash
set -e

POM_FILE=pom.xml

# Extracts strimzi-kafka-oauth dependency version from pom.xml
function get_strimzi_oauth_version {
    echo $(cat $POM_FILE | grep -m 1 strimzi-oauth.version | sed -e 's/.*>\(.*\)<.*/\1/')
}