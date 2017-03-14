#!/bin/bash

# delete Zookeeper persistent volume and claim
oc delete pv zookeeperpv
oc delete pvc zookeeperclaim

# delete Kafka persistent volume and claim
oc delete pv kafkapv
oc delete pvc kafkaclaim

# delete Zookeeper service and replication controller
oc delete svc zookeeper1
oc delete svc zookeeper2
oc delete svc zookeeper3
oc delete rc zookeeper1
oc delete rc zookeeper2
oc delete rc zookeeper3

# delete Kafka service and replication controller
oc delete svc kafka
oc delete rc kafka