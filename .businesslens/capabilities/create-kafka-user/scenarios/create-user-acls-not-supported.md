---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaUser resource with ACL rules
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: creates
        to: Not ready
        facts:
          - Name
          - Authentication
      - entity: acl-rule
        effect: creates
        facts:
          - Resource
          - Operations
          - Host
          - Type
        with: kafka-user
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Kafka cluster's authorization does not let Strimzi manage ACLs
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Authorization
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product refuses the whole Kafka user and reports why
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-user-resource
---

# Create a user with ACL rules in a cluster that cannot manage ACLs

## Trigger

The cluster authorizes clients without simple authorization, or with a custom authorizer that does not support the Kafka Admin API for ACLs.

## Outcome

No credentials, ACL rules or quotas are created and the user stays Not ready with the reason.
