---
domain: bridge
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/bridge/KafkaBridgeSpec.java#KafkaBridgeSpec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/bridge/KafkaBridgeStatus.java#KafkaBridgeStatus
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaBridgeAssemblyOperator.java#KafkaBridgeAssemblyOperator
  - kind: doc
    role: context
    target: documentation/modules/configuring/con-config-http-bridge.adoc
---

# HTTP Bridge

A deployment of the Strimzi HTTP Bridge declared by a `KafkaBridge` resource, giving HTTP clients access to a Kafka cluster.

## Information kept

- **Name** — the name of the KafkaBridge resource
- **Replicas** — how many bridge instances run
- **Bootstrap servers** — the Kafka cluster the bridge connects to
- **TLS and authentication** — the trusted certificates and client authentication used to reach Kafka
- **HTTP configuration** — the HTTP port, CORS and TLS settings
- **Client configuration** — producer, consumer and admin client settings
- **Metrics and logging** — how the bridge exposes metrics, traces and logs
- **URL** — the address HTTP clients use, reported in status
- **Status message** — the reason and message of the latest NotReady or Warning condition

## States

### Not ready

The bridge does not match the resource yet, or could not be deployed.

### Ready

The bridge runs as declared.

### Reconciliation paused

Strimzi ignores the resource until the pause is removed.
