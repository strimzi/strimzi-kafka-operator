#!/usr/bin/env bash

CC_CAPACITY_FILE="/tmp/capacity.json"
CC_CLUSTER_CONFIG_FILE="/tmp/clusterConfig.json"

# Generate capacity file
# TODO: Update DISK value based on volume sizes
cat <<EOF > $CC_CAPACITY_FILE
{
	"brokerCapacities": [{
		"brokerId": "-1",
		"capacity": {
			"DISK": "100000",
			"CPU": "100",
			"NW_IN": "10000",
			"NW_OUT": "10000"
		},
		"doc": "This is the default capacity. Capacity unit used for disk is in MB, cpu is in percentage, network throughput is in KB."
	}]
}
EOF

# Generate cluster config
cat <<EOF > $CC_CLUSTER_CONFIG_FILE
{
min.insync.replicas=$MIN_INSYNC_REPLICAS
}
EOF

# Write the config file
cat <<EOF
bootstrap.servers=$STRIMZI_KAFKA_BOOTSTRAP_SERVERS
zookeeper.connect=localhost:2181
partition.metric.sample.store.topic=__KafkaCruiseControlPartitionMetricSamples
broker.metric.sample.store.topic=__KafkaCruiseControlModelTrainingSamples
capacity.config.file=$CC_CAPACITY_FILE
cluster.configs.file=$CC_CLUSTER_CONFIG_FILE
webserver.accesslog.path=/tmp/access.log
webserver.http.address=0.0.0.0
${CRUISE_CONTROL_CONFIGURATION}
EOF
