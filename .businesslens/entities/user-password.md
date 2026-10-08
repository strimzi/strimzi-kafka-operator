---
domain: users
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/KafkaUserModel.java#KafkaUserModel
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/operator/KafkaUserOperator.java#KafkaUserOperator
---

# User password

The SCRAM-SHA-512 password of a Kafka user with SCRAM-SHA-512 authentication, kept in the user secret that client applications mount.

## Information kept

- **Password** — the generated or supplied password
- **JAAS configuration** — the SASL JAAS configuration clients use with the password
