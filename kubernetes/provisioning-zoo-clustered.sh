#!/bin/bash

# create Zookeeper persistent volume and claim
kubectl create -f ../cluster/zookeeper-volume.yaml
kubectl create -f ../cluster/zookeeper-volume-claim.yaml

# create Kafka persistent volume and claim
kubectl create -f ../cluster/kafka-volume.yaml
kubectl create -f ../cluster/kafka-volume-claim.yaml

# create Zookeeper service and replication controller
kubectl create -f ../cluster/zookeeper-service-1.yaml
kubectl create -f ../cluster/zookeeper-service-2.yaml
kubectl create -f ../cluster/zookeeper-service-3.yaml
kubectl create -f ../cluster/zookeeper-rc-1.yaml
kubectl create -f ../cluster/zookeeper-rc-2.yaml
kubectl create -f ../cluster/zookeeper-rc-3.yaml

# create Kafka service and replication controller
kubectl create -f ../cluster/kafka-service.yaml
kubectl create -f ../cluster/kafka-rc.yaml