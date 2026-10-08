---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes a listener from the Kafka resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: listener
        effect: removes
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product rolls the brokers one at a time without it and deletes its services, routes or ingresses
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Remove a listener from a Kafka cluster

## Trigger

A way of connecting is no longer needed.

## Outcome

Clients can no longer connect through the removed listener.
