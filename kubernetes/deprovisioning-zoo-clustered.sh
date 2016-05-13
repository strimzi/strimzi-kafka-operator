#!/bin/bash

# delete Zookeeper persistent volume and claim
kubectl delete pv zookeeperpv
kubectl delete pvc zookeeperclaim

# delete Kafka persistent volume and claim
kubectl delete pv kafkapv
kubectl delete pvc kafkaclaim

# delete Zookeeper service and replication controller
kubectl delete svc zookeeper1
kubectl delete svc zookeeper2
kubectl delete svc zookeeper3
kubectl delete rc zookeeper1
kubectl delete rc zookeeper2
kubectl delete rc zookeeper3

# delete Kafka service and replication controller
kubectl delete svc kafka
kubectl delete rc kafka