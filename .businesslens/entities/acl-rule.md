---
domain: users
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/user/acl/AclRule.java#AclRule
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/acl/SimpleAclRule.java#SimpleAclRule
  - kind: doc
    role: context
    target: documentation/modules/security/con-securing-client-acls.adoc
---

# ACL rule

One simple-authorization rule of a Kafka user, declared in the `acls` list of its KafkaUser resource, allowing or denying it an operation on a Kafka resource.

## Information kept

- **Resource** — the topic, group, cluster or transactional ID the rule covers, by literal name or prefix
- **Operations** — the Kafka operations the rule covers
- **Host** — the client host the rule applies to; any host when omitted
- **Type** — whether the rule allows or denies the operations
