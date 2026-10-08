---
availability:
  - place: custom-resources
domain: users
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/acl/SimpleAclRule.java#SimpleAclRule
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/operator/SimpleAclOperator.java#SimpleAclOperator
---

# Add an ACL rule

Give a Kafka user access to a topic, group, the cluster or a transactional ID by adding an ACL rule to its `KafkaUser` resource.
