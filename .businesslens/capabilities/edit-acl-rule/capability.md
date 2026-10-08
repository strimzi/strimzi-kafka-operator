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

# Edit an ACL rule

Change the resource, operations, host or type of one of a Kafka user's ACL rules.
