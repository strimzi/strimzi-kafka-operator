---
appliesTo:
  - type: entity
    id: node-pool
  - type: entity
    id: kafka-topic
  - type: entity
    id: kafka-user
  - type: entity
    id: connector
  - type: entity
    id: rebalance
references:
  - kind: code
    role: implementation
    target: topic-operator/src/main/java/io/strimzi/operator/topic/BatchingTopicController.java#BatchingTopicController
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/UserController.java#UserController
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaAssemblyOperator.java#KafkaAssemblyOperator
---

# Node pools, topics, users, connectors and rebalances take effect only when labelled with their cluster

A KafkaNodePool, KafkaTopic, KafkaUser, KafkaConnector or KafkaRebalance resource acts on the Kafka or Kafka Connect cluster named by its strimzi.io/cluster label in the same namespace. The Topic and User Operators ignore resources labelled for another cluster; the other resources without the label are reported Not ready.
