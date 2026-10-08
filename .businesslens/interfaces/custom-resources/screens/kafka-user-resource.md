---
entities:
  - entity: kafka-user
    shows:
      - Name
      - Username
      - Authentication
      - Quotas
      - Credentials Secret
      - Status message
    collects:
      - Name
      - Authentication
      - Password source
      - Quotas
      - Secret template
  - entity: acl-rule
    shows:
      - Resource
      - Operations
      - Host
      - Type
    collects:
      - Resource
      - Operations
      - Host
      - Type
---

# KafkaUser resource

The `KafkaUser` resource of one user, where its authentication, ACL rules and quotas are declared and its username and user secret reported.
