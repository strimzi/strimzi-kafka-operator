#!/bin/bash

# delete Zookeeper persistent volume and claim
oc delete pv zookeeperpv
oc delete pvc zookeeperclaim

# delete Kafka persistent volume and claim
oc delete pv kafkapv
oc delete pvc kafkaclaim

# delete Zookeeper service and replication controller
oc delete svc zookeeper
oc delete rc zookeeper

# delete Kafka service and replication controller
oc delete svc kafka
oc delete rc kafka