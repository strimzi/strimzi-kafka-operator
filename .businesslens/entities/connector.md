---
domain: kafka-connect
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/connector/KafkaConnectorSpec.java#KafkaConnectorSpec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/connector/KafkaConnectorStatus.java#KafkaConnectorStatus
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractConnectOperator.java#AbstractConnectOperator
  - kind: doc
    role: context
    target: documentation/modules/deploying/proc-deploying-kafkaconnector.adoc
---

# Kafka Connect connector

A source or sink connector running in a Kafka Connect cluster, declared by a `KafkaConnector` resource labelled with that cluster's name.

## Information kept

- **Name** — the name of the KafkaConnector resource and of the connector
- **Connector class** — the connector plugin class
- **Plugin version** — the version of the connector plugin to use, when several are installed
- **Tasks max** — the maximum number of tasks
- **Configuration** — the connector configuration
- **Requested state** — running, paused or stopped; running when omitted
- **Auto-restart** — whether failed connectors and tasks restart automatically, and the maximum number of restarts
- **Auto-restart status** — how many automatic restarts happened and when the last one was
- **Connector status** — the connector and task states Kafka Connect reports, such as running, paused, stopped or failed
- **Topics** — the topics the connector uses, reported in status
- **Restart request** — a request to restart the connector, optionally with all or only failed tasks, or to restart one task
- **Offsets request** — a request to list, alter or reset the connector offsets
- **Offsets** — the source partitions and offsets, or consumer group offsets, the connector has committed
- **Status message** — the reason and message of the latest NotReady or Warning condition

## States

### Not ready

The connector does not run as declared, or could not be created.

### Ready

The connector runs with the declared configuration and state.

### Reconciliation paused

Strimzi ignores the resource until the pause is removed.
