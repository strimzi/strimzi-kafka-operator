#!/usr/bin/env bash

# Generates documentation/book/snip-images.adoc
# according to the values in kafka-versions

. $(dirname $0)/../multi-platform-support.sh

FILE=$1

# Read the kafka versions file and create an array of version strings
declare -a versions
finished=0
counter=0
while [ $finished -lt 1 ] 
do
    version=$(yq read "$FILE" "[${counter}].version")

    if [ "$version" = "null" ]
    then
        finished=1
    else
        versions+=("$version")
        counter=$((counter + 1))
    fi 
done

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
{ProductName} image for running Kafka, including:

* Kafka Broker
* Kafka Connect / S2I
* Kafka Mirror Maker
* Zookeeper
* TLS Sidecars

|Operator
a|
* {DockerOrg}/operator:{DockerTag}

a|
{ProductName} image for running the operators:

* Cluster Operator
* Topic Operator
* User Operator
* Kafka Initializer

|Kafka Bridge
a|
* {DockerOrg}/kafka-bridge:{DockerTag}

a|
{ProductName} image for running the {ProductName} kafka Bridge

|===
EOF
