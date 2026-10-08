---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator adds a listener whose name or port another listener already uses, or a route or ingress listener without TLS encryption
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: listener
        effect: creates
        facts:
          - Name
          - Port
          - Type
          - TLS encryption
          - Client authentication
          - Listener configuration
          - Network policy peers
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product changes nothing and reports the Kafka cluster not ready with every problem found
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        from: Ready
        to: Not ready
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Add a listener that clashes with another

## Trigger

The new listener breaks a listener rule.

## Outcome

No Kafka node changes and the Kafka resource is Not ready with the reason.
