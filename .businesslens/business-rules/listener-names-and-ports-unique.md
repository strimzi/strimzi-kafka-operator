---
appliesTo:
  - type: entity
    id: listener
    effect: creates
  - type: entity
    id: listener
    effect: changes
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/ListenersValidator.java#validateAndGetErrorMessages
---

# Each listener of a Kafka cluster has its own name and port

Listener names and ports are unique within a Kafka cluster, ports below 9092 and those Strimzi uses itself are refused, and route and ingress listeners need TLS encryption. A Kafka resource that breaks any of these is Not ready, listing every problem found, and its Kafka nodes do not change.
