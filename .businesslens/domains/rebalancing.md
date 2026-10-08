# Rebalancing

Redistributing partition replicas across brokers with Cruise Control.

## Boundary

Owns rebalances requested through KafkaRebalance resources. Does not own deploying Cruise Control, which belongs to the Kafka cluster.
