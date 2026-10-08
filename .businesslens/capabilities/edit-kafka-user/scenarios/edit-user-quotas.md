---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the quotas in the KafkaUser resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        facts:
          - Quotas
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product replaces the user's quotas in Kafka
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

# Change a user's quotas

## Trigger

An application needs different limits.

## Outcome

The user has exactly the new quotas.
