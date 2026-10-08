---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator adds an ACL rule to the KafkaUser resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: acl-rule
        effect: creates
        facts:
          - Resource
          - Operations
          - Host
          - Type
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product creates the ACL rule in Kafka for the user
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: acl-rule
        effect: reads
        facts:
          - Resource
          - Operations
          - Host
          - Type
    contexts:
      api:
        place: custom-resources::kafka-user-resource
---

# Add an ACL rule to a user

## Trigger

An application needs access to another Kafka resource.

## Outcome

The user has the new access.

## Edge cases

- In a cluster whose authorization does not let Strimzi manage ACL rules, the whole Kafka user is refused and reported Not ready.
