#!/bin/bash

# create Zookeeper persistent volume and claim
oc create -f ../cluster/zookeeper-volume.yaml
oc create -f ../cluster/zookeeper-volume-claim.yaml

# create Kafka persistent volume and claim
oc create -f ../cluster/kafka-volume.yaml
oc create -f ../cluster/kafka-volume-claim.yaml

# create Zookeeper service and replication controller
oc create -f ../cluster/zookeeper-service.yaml
oc create -f ../cluster/zookeeper-rc.yaml

# create Kafka service and replication controller
oc create -f ../cluster/kafka-service.yaml
oc create -f ../cluster/kafka-rc.yaml
