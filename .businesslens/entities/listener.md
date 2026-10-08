---
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/kafka/listener/GenericKafkaListener.java#GenericKafkaListener
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/kafka/listener/ListenerStatus.java#ListenerStatus
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/ListenersValidator.java#ListenersValidator
  - kind: doc
    role: context
    target: documentation/api/io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener.adoc
---

# Listener

A named endpoint of a Kafka cluster through which clients connect, declared in the `listeners` list of its Kafka resource.

## Information kept

- **Name** — the name that identifies the listener in its cluster
- **Port** — the port clients connect to
- **Type** — internal, cluster-ip, route, load balancer, node port or ingress
- **TLS encryption** — whether connections are encrypted
- **Client authentication** — how clients authenticate: mutual TLS, SCRAM-SHA-512, custom, or none
- **Listener configuration** — bootstrap and per-broker addresses, its own server certificate and other type-specific settings
- **Network policy peers** — which applications may connect to the listener
- **Bootstrap address** — the bootstrap servers clients use, reported in status
- **Broker addresses** — the address of each broker, reported in status
- **Certificates** — the CA certificates clients trust for a TLS listener, reported in status
