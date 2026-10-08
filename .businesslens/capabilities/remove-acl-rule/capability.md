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

# Remove an ACL rule

Take access away from a Kafka user by removing one of the ACL rules from its `KafkaUser` resource.
