---
domain: kafka-clusters
relations:
  - entity: node-pool
    verb: has
    cardinality: one-to-many
  - entity: listener
    verb: has
    cardinality: one-to-many
  - entity: certificate-authority
    verb: has
    cardinality: one-to-many
  - entity: kafka-topic
    verb: has
    cardinality: one-to-many
  - entity: kafka-user
    verb: has
    cardinality: one-to-many
  - entity: rebalance
    verb: has
    cardinality: one-to-many
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/kafka/KafkaSpec.java#KafkaSpec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/kafka/KafkaClusterSpec.java#KafkaClusterSpec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/kafka/KafkaStatus.java#KafkaStatus
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaAssemblyOperator.java#KafkaAssemblyOperator
---

# Kafka cluster

A KRaft-based Apache Kafka cluster declared by a `Kafka` resource. It is made of the nodes of its node pools and, when asked for, the Topic Operator, User Operator, Cruise Control and Kafka Exporter.

## Information kept

- **Name** — the name of the Kafka resource, which names the cluster and the resources Strimzi creates for it
- **Kafka version** — the Apache Kafka version requested; the latest supported version when omitted
- **Metadata version** — the KRaft metadata version requested
- **Authorization** — simple, custom or no authorization for clients, with its super users
- **Broker configuration** — Kafka broker properties apart from the ones Strimzi manages itself
- **Rack awareness** — the node label used to spread replicas across racks or zones
- **Tiered storage** — the remote storage manager brokers offload log segments to
- **Quotas plugin** — the broker quota plugin and its limits
- **Metrics and logging** — how brokers expose metrics and what they log
- **Template** — customizations of the Kubernetes resources Strimzi generates for the cluster
- **Maintenance time windows** — cron expressions limiting when certificate renewals may roll pods
- **Topic Operator** — whether the Topic Operator is deployed for the cluster, and its settings
- **User Operator** — whether the User Operator is deployed for the cluster, and its settings
- **Cruise Control** — whether Cruise Control is deployed for the cluster, and its configuration
- **Auto-rebalance on scaling** — the rebalance templates run automatically when brokers are added or removed
- **Kafka Exporter** — whether Kafka Exporter is deployed to report consumer lag and topic metrics
- **Skip broker scale-down check** — whether brokers that still hold partition replicas may be removed
- **Cluster ID** — the Kafka cluster ID, reported in status
- **Current Kafka version** — the Kafka version the nodes run, reported in status
- **Current metadata version** — the KRaft metadata version in use, reported in status
- **Node pools in use** — the node pools that make up the cluster, reported in status
- **Auto-rebalance status** — whether an automatic rebalance is idle or running for a scale-up or scale-down, with the brokers it covers
- **Status message** — the reason and message of the latest NotReady or Warning condition

## States

### Not ready

Strimzi has not yet brought the cluster to its declared configuration, or could not.

### Ready

The cluster matches its declared configuration.

### Reconciliation paused

Strimzi ignores the resource until the pause is removed.
