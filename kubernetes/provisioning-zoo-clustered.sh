#!/bin/bash

# create Zookeeper persistent volume and claim
kubectl create -f zookeeper-volume.yaml
kubectl create -f zookeeper-volume-claim.yaml

# create Kafka persistent volume and claim
kubectl create -f kafka-volume.yaml
kubectl create -f kafka-volume-claim.yaml

# create Zookeeper service and replication controller
kubectl create -f zookeeper-service-1.yaml
kubectl create -f zookeeper-service-2.yaml
kubectl create -f zookeeper-service-3.yaml
kubectl create -f zookeeper-rc-1.yaml
kubectl create -f zookeeper-rc-2.yaml
kubectl create -f zookeeper-rc-3.yaml

# create Kafka service and replication controller
kubectl create -f kafka-service.yaml
kubectl create -f kafka-rc.yaml