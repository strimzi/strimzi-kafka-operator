---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes a listener in the Kafka resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: listener
        effect: changes
        facts:
          - Port
          - Type
          - TLS encryption
          - Client authentication
          - Listener configuration
          - Network policy peers
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product rolls the brokers one at a time onto the changed listener, updates its services and reports its addresses
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts: []
      - entity: listener
        effect: changes
        facts:
          - Bootstrap address
          - Broker addresses
          - Certificates
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Change a listener

## Trigger

Clients need to connect differently.

## Outcome

Clients connect through the changed listener.
