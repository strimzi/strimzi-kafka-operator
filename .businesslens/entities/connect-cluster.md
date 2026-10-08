---
domain: kafka-connect
relations:
  - entity: connector
    verb: runs
    cardinality: one-to-many
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/connect/KafkaConnectSpec.java#KafkaConnectSpec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/connect/KafkaConnectStatus.java#KafkaConnectStatus
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaConnectAssemblyOperator.java#KafkaConnectAssemblyOperator
  - kind: doc
    role: context
    target: documentation/modules/configuring/con-config-kafka-connect.adoc
---

# Kafka Connect cluster

A Kafka Connect cluster declared by a `KafkaConnect` resource, connected to any Kafka cluster through its bootstrap servers, optionally with an image Strimzi builds to include connector plugins.

## Information kept

- **Name** — the name of the KafkaConnect resource
- **Replicas** — how many Connect workers run
- **Kafka Connect version** — the Kafka version the workers run
- **Bootstrap servers** — the Kafka cluster the workers connect to
- **TLS and authentication** — the trusted certificates and client authentication used to reach Kafka
- **Connect configuration** — the group ID, storage topics and worker configuration
- **Build** — the output image and the connector plugin artifacts Strimzi builds into it
- **Mounted plugins** — connector plugins mounted from container images
- **Connector resources enabled** — whether KafkaConnector resources manage this cluster's connectors
- **Rack awareness** — the node label used to pick the closest replicas
- **Metrics and logging** — how the workers expose metrics, trace and log
- **Image rebuild requested** — a request to rebuild the image without changing the build
- **Manual rolling update** — a request to roll every worker
- **REST API URL** — the address of the Kafka Connect REST API, reported in status
- **Connector plugins** — the connector plugins available on the workers, reported in status
- **Status message** — the reason and message of the latest NotReady or Warning condition

## States

### Not ready

The workers do not match the resource yet, or could not be deployed.

### Ready

The workers run as declared.

### Reconciliation paused

Strimzi ignores the resource until the pause is removed.
