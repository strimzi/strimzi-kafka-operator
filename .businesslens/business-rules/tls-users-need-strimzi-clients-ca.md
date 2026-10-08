---
appliesTo:
  - type: entity
    id: kafka-user
    effect: creates
    facts:
      - Authentication
  - type: entity
    id: kafka-user
    effect: changes
    facts:
      - Authentication
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/operator/KafkaUserOperator.java#maybeGenerateTlsCredentials
---

# Users with TLS authentication need a clients CA that Strimzi issues certificates from

When the clients CA of a Kafka cluster is managed by cert-manager, KafkaUser resources with TLS authentication are refused and must use tls-external authentication instead.
