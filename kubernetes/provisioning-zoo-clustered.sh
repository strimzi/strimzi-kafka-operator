#!/bin/bash

# create Zookeeper persistent volume and claim
kubectl create -f ../cluster/zookeeper-pv.yaml
kubectl create -f ../cluster/zookeeper-pvc.yaml

# create Kafka persistent volume and claim
kubectl create -f ../cluster/kafka-pv.yaml
kubectl create -f ../cluster/kafka-pvc.yaml

# create Zookeeper service and replication controller
kubectl create -f ../cluster/zookeeper-svc-1.yaml
kubectl create -f ../cluster/zookeeper-svc-2.yaml
kubectl create -f ../cluster/zookeeper-svc-3.yaml
kubectl create -f ../cluster/zookeeper-dc-1.yaml
kubectl create -f ../cluster/zookeeper-dc-2.yaml
kubectl create -f ../cluster/zookeeper-dc-3.yaml

# create Kafka service and replication controller
kubectl create -f ../cluster/kafka-svc.yaml
kubectl create -f ../cluster/kafka-dc.yaml
