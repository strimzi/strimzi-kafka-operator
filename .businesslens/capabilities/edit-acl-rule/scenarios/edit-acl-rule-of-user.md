---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes an ACL rule in the KafkaUser resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: acl-rule
        effect: changes
        facts:
          - Resource
          - Operations
          - Host
          - Type
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product replaces the old ACL rule in Kafka with the changed one
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

# Change an ACL rule of a user

## Trigger

An application needs different access to a Kafka resource.

## Outcome

The user has exactly the changed access.
