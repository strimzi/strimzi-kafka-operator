#!/bin/bash

# create Zookeeper persistent volume and claim
kubectl create -f ../cluster/zookeeper-volume.yaml
kubectl create -f ../cluster/zookeeper-volume-claim.yaml

# create Kafka persistent volume and claim
kubectl create -f ../cluster/kafka-volume.yaml
kubectl create -f ../cluster/kafka-volume-claim.yaml

# create Zookeeper service and replication controller
kubectl create -f ../cluster/zookeeper-service.yaml
kubectl create -f ../cluster/zookeeper-rc.yaml

# create Kafka service and replication controller
kubectl create -f ../cluster/kafka-service.yaml
kubectl create -f ../cluster/kafka-rc.yaml
