---
appliesTo:
  - type: entity
    id: kafka-user
    effect: creates
    facts:
      - Authentication
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/KafkaUserModel.java#validateTlsUsername
---

# Users with TLS authentication have names of at most 64 characters

The name of a KafkaUser with TLS authentication becomes the common name of its certificate, so longer names are refused. Users with other authentication have no such limit.
