---
availability:
  - place: custom-resources
domain: users
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/operator/KafkaUserOperator.java#delete
---

# Delete a Kafka user

Delete a Kafka user by deleting its `KafkaUser` resource. Strimzi removes its credentials, ACL rules and quotas from Kafka and deletes its user secret.
