## Log dump script

This script is just a wrapper around `kafka-dump-log.sh`, and is supposed to make partition dumping easier on Strimzi.
Every topic partition is composed of multiple segments, that are replicated to a number of brokers, and they can be in any of the attached volumes.
Moreover, when dealing with internal topics, you have to identify which partition is needed to avoid downloading all of them in the worst case.

## Requirements

- bash 5+ (GNU)
- kubectl 1.19+ (K8s CLI)
- yq 4.6+ (YAML processor)
- jshell (JDK 9+)

## Usage example

This example shows how to dump partitions related to a simple word count transactional application running on Kubernetes. 
In this case we have 2 disks per broker and an output topic with 1 partition and RF=1.

```shell
# Print topic partition segments
./tools/log-dump/run.sh partition --namespace test --cluster my-cluster \
  --topic my-topic --partition 0 --dry-run
    
brokers: 3, storage: jbod, disks: 2
my-topic-0 segments in kafka-0-disk-0
No segment found
my-topic-0 segments in kafka-0-disk-1
No segment found
my-topic-0 segments in kafka-1-disk-0
00000000000000000000.log
my-topic-0 segments in kafka-1-disk-1
No segment found
my-topic-0 segments in kafka-2-disk-0
No segment found
my-topic-0 segments in kafka-2-disk-1
No segment found

# Dump topic partition
./tools/log-dump/run.sh partition --namespace test --cluster my-cluster \
  --topic my-topic --partition 0 --out-path ~/Downloads/my-dump

brokers: 3, storage: jbod, disks: 2
my-topic-0 segments in kafka-0-disk-0
No segment found
my-topic-0 segments in kafka-0-disk-1
No segment found
my-topic-0 segments in kafka-1-disk-0
00000000000000000000.log
my-topic-0 segments in kafka-1-disk-1
No segment found
my-topic-0 segments in kafka-2-disk-0
No segment found
my-topic-0 segments in kafka-2-disk-1
No segment found

# Dump consumer group offsets partition by group.id
./tools/log-dump/run.sh cg_offsets --namespace test --cluster my-cluster \
  --group-id my-group --out-path ~/Downloads/my-dump
  
brokers: 3, storage: jbod, disks: 2
my-group coordinating partition: 12
__consumer_offsets-12 segments in kafka-0-disk-0
00000000000000000000.log
__consumer_offsets-12 segments in kafka-0-disk-1
No segment found
__consumer_offsets-12 segments in kafka-1-disk-0
No segment found
__consumer_offsets-12 segments in kafka-1-disk-1
00000000000000000000.log
__consumer_offsets-12 segments in kafka-2-disk-0
00000000000000000000.log
__consumer_offsets-12 segments in kafka-2-disk-1
No segment found

# Dump transaction state partition by transaction.id
./tools/log-dump/run.sh txn_state --namespace test --cluster my-cluster \
  --txn-id my-app-my-group-my-topic-0 --out-path ~/Downloads/my-dump
  
brokers: 3, storage: jbod, disks: 2
kafka-trans-my-group-wc-input-0 coordinating partition: 31
__transaction_state-31 segments in kafka-0-disk-0
00000000000000000000.log
__transaction_state-31 segments in kafka-0-disk-1
No segment found
__transaction_state-31 segments in kafka-1-disk-0
00000000000000000000.log
__transaction_state-31 segments in kafka-1-disk-1
No segment found
__transaction_state-31 segments in kafka-2-disk-0
No segment found
__transaction_state-31 segments in kafka-2-disk-1
00000000000000000000.log
```
