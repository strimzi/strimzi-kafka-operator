---
availability:
  - place: custom-resources
domain: users
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/operator/KafkaUserOperator.java#KafkaUserOperator
---

# Edit a Kafka user

Change a Kafka user's authentication, quotas or the template of its user secret.
