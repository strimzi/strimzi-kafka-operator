---
availability:
  - place: custom-resources
domain: users
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/KafkaUserModel.java#KafkaUserModel
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/operator/KafkaUserOperator.java#KafkaUserOperator
  - kind: doc
    role: context
    target: documentation/modules/operators/proc-configuring-kafka-user.adoc
---

# Create a Kafka user

Declare a Kafka user with a `KafkaUser` resource labelled with its Kafka cluster. The User Operator creates its credentials in a user secret of the same name, and applies its ACL rules and quotas in Kafka.
