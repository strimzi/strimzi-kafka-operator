## Kafka log dump

Bash script for topic's partitions dump.

Dumping partitions is not easy, because a single partition is composed by X segments, all segments are replicated in the same set of brokers, and they can be in any of the attached disks. 

When dealing with internal topics (`__consumer_offsets` and `__transaction_state`) you need a way to identify which partition is needed, so that you don't have to download all 50 partitions (default value).

Partition dumps can be useful to troubleshoot Kafka issues. For example, we can identify a pending transaction causing `read_committed` consumers to hang and last stable offset (LSO) to grow indefinitely.

## Requirements

- bash 5+ (GNU)
- kubectl 1.16+ (K8s CLI)
- yq 4.6+ (YAML processor)
- [klog 1.0-SNAPSHOT](https://github.com/tombentley/klog)

## Additional logging

Advanced analysis also requires correlation with Kafka broker logs. If transactions are enabled, then also logs from producer's `TransactionManager` are useful.

```sh
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

```sh
# Print partition segments across the cluster
./klog-dump.sh partition --namespace test --cluster test-cluster \
  --topic wc-output --partition 0 --dry-run
    
brokers: 3, storage: jbod, disks: 2
wc-output-0 segments in kafka-0-disk-0
No segment found
wc-output-0 segments in kafka-0-disk-1
No segment found
wc-output-0 segments in kafka-1-disk-0
00000000000000000000.log
wc-output-0 segments in kafka-1-disk-1
No segment found
wc-output-0 segments in kafka-2-disk-0
No segment found
wc-output-0 segments in kafka-2-disk-1
No segment found

# Dump partition segments across the cluster
./klog-dump.sh partition --namespace test --cluster test-cluster \
  --topic wc-output --partition 0 --out-path ~/Downloads/my-dump

brokers: 3, storage: jbod, disks: 2
wc-output-0 segments in kafka-0-disk-0
No segment found
wc-output-0 segments in kafka-0-disk-1
No segment found
wc-output-0 segments in kafka-1-disk-0
00000000000000000000.log
wc-output-0 segments in kafka-1-disk-1
No segment found
wc-output-0 segments in kafka-2-disk-0
No segment found
wc-output-0 segments in kafka-2-disk-1
No segment found

# Dump consumer group offsets segments
./klog-dump.sh offsets --namespace test --cluster test-cluster \
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

# Dump application transaction state segments
./klog-dump.sh tx_state --namespace test --cluster test-cluster \
  --trans-id kafka-trans-my-group-wc-input-0 --out-path ~/Downloads/my-dump
  
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
