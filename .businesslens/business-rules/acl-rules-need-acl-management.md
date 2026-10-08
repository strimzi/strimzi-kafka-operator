---
appliesTo:
  - type: entity
    id: acl-rule
    effect: creates
  - type: entity
    id: acl-rule
    effect: changes
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/KafkaUserModel.java#fromCrd
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/EntityUserOperator.java#EntityUserOperator
---

# A Kafka user with ACL rules is applied only where Strimzi can manage ACL rules in its cluster

A KafkaUser with ACL rules for a Kafka cluster whose authorization is neither simple authorization nor a custom authorizer that supports managing ACL rules through the Kafka Admin API is refused as a whole and reported Not ready.
