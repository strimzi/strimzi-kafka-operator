# Strimzi backup

Bash script for cold/offline backups of Kafka clusters on Kubernetes/OpenShift.
Only local file system is supported. Make sure to have enough free space in the target directory.

If you think you do not need a backup strategy for Kafka because of its embedded data replication, then consider the 
impact of a misconfiguration, bug or security breach that deleted all your data. For hot/online backups, you can use 
storage snapshotting or streaming into object storage.

To run the script, the Kubernetes user must have permission to work with PVC and Strimzi custom resources. The procedure 
will stop the Cluster Operator and selected cluster for the duration of the backup. Before restoring the Kafka cluster 
you need to make sure to have the right version of Strimzi CRDs installed. If you have a single cluster-wide Cluster 
Operator, then you need to scale it down manually. You can run backup and restore procedures for different Kafka 
clusters in parallel. Consumer group offsets are included, but not Kafka Connect, MirrorMaker and Kafka Bridge custom 
resources.

## Requirements

- bash 5+ (GNU)
- tar 1.33+ (GNU)
- kubectl 1.16+ (K8s CLI)
- yq 4.6+ (YAML processor)
- zip 3+ (Info-ZIP)
- unzip 6+ (Info-ZIP)
- enough disk space

## Test procedure

```sh
CLIENT_IMAGE="quay.io/strimzi/kafka:latest-kafka-2.8.0"
krun() { kubectl run client -it --rm=true --restart=Never --image=$CLIENT_IMAGE -- $@; }

# deploy a test cluster
kubectl create namespace myproject
kubectl config set-context --current --namespace="myproject"
kubectl create -f ./install/cluster-operator
kubectl create -f ./examples/kafka/kafka-persistent.yaml
kubectl create -f ./examples/topic/kafka-topic.yaml

# send and consume 100000 messages
krun ./bin/kafka-producer-perf-test.sh \
  --producer-props acks=1 bootstrap.servers=my-cluster-kafka-bootstrap:9092 \
  --topic my-topic --record-size 1000 --num-records 100000 --throughput -1

krun ./bin/kafka-consumer-perf-test.sh \
  --broker-list my-cluster-kafka-bootstrap:9092 \
  --topic my-topic --group my-group --messages 100000 --timeout 15000

# save consumer group offsets
krun ./bin/kafka-consumer-groups.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --group my-group --describe > /tmp/my-offsets.txt

# send additional 12345 messages
krun ./bin/kafka-producer-perf-test.sh \
  --producer-props acks=1 bootstrap.servers=my-cluster-kafka-bootstrap:9092 \
  --topic my-topic --record-size 1000 --num-records 12345 --throughput -1

# run backup procedure
./tools/cold-backup/run.sh backup -n myproject -c my-cluster -t /tmp/my-cluster.zip

# recreate the namespace and deploy the operator
kubectl delete ns myproject
kubectl create ns myproject
kubectl create -f ./install/cluster-operator

# run restore procedure and wait for provisioning
./tools/cold-backup/run.sh restore -n myproject -c my-cluster -s /tmp/my-cluster.zip

# check consumer group offsets (expected: current-offset match)
cat /tmp/my-offsets.txt

krun ./bin/kafka-consumer-groups.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --group my-group --describe

# check consumer group recovery (expected: 12345)
krun ./bin/kafka-consumer-perf-test.sh \
  --broker-list my-cluster-kafka-bootstrap:9092 \
  --topic my-topic --group my-group --messages 1000000 --timeout 5000

# check total number of messages (expected: 112345)
krun ./bin/kafka-consumer-perf-test.sh \
  --broker-list my-cluster-kafka-bootstrap:9092 \
  --topic my-topic --group new-group --messages 1000000 --timeout 15000
```
