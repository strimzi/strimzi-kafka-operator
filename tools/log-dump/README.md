## Kafka log dump

Bash script for topic's partitions dump.

Dumping partitions is not easy, because a single partition is composed by X segments, all segments are replicated in the same set of brokers, and they can be in any of the attached disks. 

When dealing with the internal topics `__consumer_offsets` and `__transaction_state`, you need a way to identify which partition is needed, so that you don't have to download all 50 partitions (default value).

Partition dumps can be useful to troubleshoot Kafka issues. For example, we can identify a pending transaction causing `read_committed` consumers to hang and last stable offset (LSO) to grow indefinitely.

## Requirements

- bash 5+ (GNU)
- kubectl 1.16+ (K8s CLI)
- yq 4.6+ (YAML processor)
- jshell (JDK 9+)

## Additional logging

Advanced analysis also requires correlation with Kafka broker logs. If transactions are enabled, then also logs from producer's `TransactionManager` are useful.

```shell
# Kafka broker
kubectl edit k $CLUSTER_NAME
spec:
  kafka:
    logging:
      type: inline
      loggers:
        kafka.root.logger.level: "INFO"
        log4j.logger.kafka.coordinator.transaction: "DEBUG"
        log4j.logger.kafka.request.logger: "DEBUG"
        log4j.logger.kafka.log.LogCleaner: "DEBUG"
        
# Logback client application
cat <<EOF > src/main/resources/logback.xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d %highlight(%p) [%t] %c{0} - %m%n</pattern>
        </encoder>
    </appender>
    <root level="INFO">
        <appender-ref ref="STDOUT" />
    </root>
    <logger name="org.apache.kafka.clients.producer.internals.TransactionManager" level="DEBUG" />
</configuration>
EOF
```

## Usage example

This example shows how to dump partitions related to a simple word count transactional application running on Kubernetes. In this case we have 2 disks per broker and an output topic with 1 partition and RF=1.

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
