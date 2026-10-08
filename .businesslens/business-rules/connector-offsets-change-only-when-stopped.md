---
appliesTo:
  - type: capability
    id: alter-connector-offsets
  - type: capability
    id: reset-connector-offsets
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractConnectOperator.java#verifyConnectorStopped
---

# Connector offsets are altered or reset only while the connector is stopped

Altering or resetting the offsets of a Kafka Connect connector or a MirrorMaker 2 connector requires it to be stopped; otherwise the request stays in place, the resource carries a warning, and it is retried on the next reconciliation.
