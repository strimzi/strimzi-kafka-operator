#!/usr/bin/env bash

source $(dirname $(realpath $0))/../tools/kafka-versions-tools.sh

# Generates documentation/book/snip-images.adoc
# according to the values in kafka-versions

source $(dirname $(realpath $0))/../tools/multi-platform-support.sh

# Parse the Kafka versions file and get a list of version strings in an array
# called "versions"
get_kafka_versions

cat <<EOF
// Auto generated content - DO NOT EDIT BY HAND
// Edit documentation/snip-images.sh instead
[table,stripes=none]
|===
|Container image |Namespace/Repository |Description

|Kafka
a|
EOF
for kafka_version in "${versions[@]}"
do
echo "* {DockerOrg}/kafka:{DockerTag}-kafka-${kafka_version}"
done
cat <<EOF

a|
Strimzi image for running Kafka, including:

* Kafka Broker
* Kafka Connect
* Kafka MirrorMaker
* ZooKeeper
* TLS Sidecars
* Cruise Control

|Operator
a|
* {DockerOrg}/operator:{DockerTag}

a|
Strimzi image for running the operators:

* Cluster Operator
* Topic Operator
* User Operator
* Kafka Initializer

|Kafka Bridge
a|
* {DockerOrg}/kafka-bridge:{BridgeDockerTag}

a|
Strimzi image for running the Strimzi kafka Bridge

|Strimzi Drain Cleaner
a|
* {DockerOrg}/drain-cleaner:{DrainCleanerDockerTag}

a|
Strimzi image for running the Strimzi Drain Cleaner

|===
EOF
