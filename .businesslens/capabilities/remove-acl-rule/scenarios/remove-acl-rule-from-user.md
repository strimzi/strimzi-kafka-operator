---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes an ACL rule from the KafkaUser resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: acl-rule
        effect: removes
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product deletes the corresponding ACL from Kafka
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-user-resource
---

# Remove an ACL rule from a user

## Trigger

An application no longer needs access to a Kafka resource.

## Outcome

The user no longer has that access.
