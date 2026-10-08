---
domain: users
relations:
  - entity: acl-rule
    verb: has
    cardinality: one-to-many
  - entity: user-certificate
    verb: has
    cardinality: one-to-one
  - entity: user-password
    verb: has
    cardinality: one-to-one
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/user/KafkaUserSpec.java#KafkaUserSpec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/user/KafkaUserStatus.java#KafkaUserStatus
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/KafkaUserModel.java#KafkaUserModel
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/operator/KafkaUserOperator.java#KafkaUserOperator
---

# Kafka user

A client identity in a Kafka cluster declared by a `KafkaUser` resource, with its authentication, ACL rules and quotas. The User Operator keeps it in Kafka and puts its credentials in a user secret of the same name.

## Information kept

- **Name** — the name of the KafkaUser resource
- **Authentication** — mutual TLS with a certificate from the clients CA, TLS with an externally issued certificate, SCRAM-SHA-512, or none
- **Password source** — a Secret and key supplying the SCRAM-SHA-512 password instead of a generated one
- **Quotas** — produce and consume byte rates, request percentage and controller mutation rate
- **Secret template** — labels and annotations for the user secret
- **Username** — the name Kafka knows the user by, reported in status
- **Credentials Secret** — the name of the user secret, reported in status when there is one
- **Status message** — the reason and message of the latest NotReady or Warning condition

## States

### Not ready

The user in Kafka does not match the resource yet, or the resource was refused.

### Ready

The user, its credentials, ACL rules and quotas match the resource.

### Reconciliation paused

The User Operator ignores the resource until the pause is removed.
