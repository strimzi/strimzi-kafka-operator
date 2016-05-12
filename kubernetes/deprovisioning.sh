#!/bin/bash

# delete Zookeeper persistent volume and claim
kubectl delete pv zookeeperpv
kubectl delete pvc zookeeperclaim

# delete Kafka persistent volume and claim
kubectl delete pv kafkapv
kubectl delete pvc kafkaclaim

# delete Zookeeper service and replication controller
kubectl delete svc zookeeper
kubectl delete rc zookeeper

# delete Kafka service and replication controller
kubectl delete svc kafka
kubectl delete rc kafka