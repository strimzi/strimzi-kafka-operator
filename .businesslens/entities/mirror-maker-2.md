---
domain: mirror-maker
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/mirrormaker2/KafkaMirrorMaker2Spec.java#KafkaMirrorMaker2Spec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/mirrormaker2/KafkaMirrorMaker2MirrorSpec.java#KafkaMirrorMaker2MirrorSpec
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaMirrorMaker2AssemblyOperator.java#KafkaMirrorMaker2AssemblyOperator
  - kind: doc
    role: context
    target: documentation/modules/configuring/con-config-mirrormaker2.adoc
---

# MirrorMaker 2

A MirrorMaker 2 deployment declared by a `KafkaMirrorMaker2` resource: a Kafka Connect cluster beside a target Kafka cluster running source and checkpoint connectors that mirror topics and consumer group offsets from one or more source clusters.

## Information kept

- **Name** — the name of the KafkaMirrorMaker2 resource
- **Replicas** — how many workers run
- **Kafka version** — the Kafka version the workers run
- **Target cluster** — the alias, bootstrap servers, TLS, authentication and Connect configuration of the cluster mirrored into
- **Mirrors** — for each source cluster, its connection, the topic and group include and exclude patterns, and the source and checkpoint connector configuration
- **Connector states** — the requested state — running, paused or stopped — of each mirror's source and checkpoint connector
- **Metrics and logging** — how the workers expose metrics, trace and log
- **Connector restart request** — a request to restart one MirrorMaker 2 connector or one of its tasks
- **Offsets request** — a request to list, alter or reset the offsets of one MirrorMaker 2 connector
- **Offsets** — the offsets a MirrorMaker 2 connector has committed
- **Connector status** — the state of each MirrorMaker 2 connector and task, reported in status
- **Auto-restart status** — automatic restarts of failed MirrorMaker 2 connectors, reported in status
- **Status message** — the reason and message of the latest NotReady or Warning condition

## States

### Not ready

The deployment does not match the resource yet, or could not be deployed.

### Ready

The workers and connectors run as declared.

### Reconciliation paused

Strimzi ignores the resource until the pause is removed.
