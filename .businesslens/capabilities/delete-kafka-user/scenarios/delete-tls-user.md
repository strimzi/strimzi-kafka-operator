---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the KafkaUser resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: removes
        from: Ready
      - entity: acl-rule
        effect: removes
        with: kafka-user
      - entity: user-certificate
        effect: removes
        with: kafka-user
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product removes the user's ACLs and quotas from Kafka and deletes the user secret
    kind: product
    actor: strimzi-administrator
    entities: []
    contexts:
      api:
        place: custom-resources::kafka-user-resource
---

# Delete a TLS user

## Trigger

An application no longer needs access.

## Outcome

The application's certificate no longer grants it access.
