#!/usr/bin/env bash
#
# License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
#
# Alpine images are an experimental feature not supported by the core Strimzi team
#
DOCKER_BUILD_ARGS="--build-arg JAVA_ORDINAL_VERSION=$(echo ${java_version} | sed -E -n 's/^(1\.([5-8])|([0-9]+)).*$/\2\3/p') ${DOCKER_BUILD_ARGS}"