#!/bin/bash

# create Zookeeper persistent volume and claim
oc create -f ../cluster/zookeeper-pv.yaml
oc create -f ../cluster/zookeeper-pvc.yaml

# create Kafka persistent volume and claim
oc create -f ../cluster/kafka-pv.yaml
oc create -f ../cluster/kafka-pvc.yaml

# create Zookeeper service and replication controller
oc create -f ../cluster/zookeeper-svc-1.yaml
oc create -f ../cluster/zookeeper-svc-2.yaml
oc create -f ../cluster/zookeeper-svc-3.yaml
oc create -f ../cluster/zookeeper-dc-1.yaml
oc create -f ../cluster/zookeeper-dc-2.yaml
oc create -f ../cluster/zookeeper-dc-3.yaml

# create Kafka service and replication controller
oc create -f ../cluster/kafka-svc.yaml
oc create -f ../cluster/kafka-dc.yaml
