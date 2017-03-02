#!/bin/bash

# create Zookeeper persistent volume and claim
kubectl create -f ../cluster/zookeeper-pv.yaml
kubectl create -f ../cluster/zookeeper-pvc.yaml

# create Kafka persistent volume and claim
kubectl create -f ../cluster/kafka-pv.yaml
kubectl create -f ../cluster/kafka-pvc.yaml

# create Zookeeper service and replication controller
kubectl create -f ../cluster/zookeeper-svc.yaml
kubectl create -f ../cluster/zookeeper-dc.yaml

# create Kafka service and replication controller
kubectl create -f ../cluster/kafka-svc.yaml
kubectl create -f ../cluster/kafka-dc.yaml
