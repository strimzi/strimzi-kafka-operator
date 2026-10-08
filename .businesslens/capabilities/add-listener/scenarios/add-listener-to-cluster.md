---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator adds a listener with its name, port, type, encryption and authentication to the Kafka resource
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
  - text: The Product rolls the brokers one at a time to open the listener, creates its services, routes or ingresses, and reports its addresses
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

# Add a listener to a Kafka cluster

## Trigger

Clients inside or outside Kubernetes need another way to reach the cluster.

## Outcome

Clients connect through the new listener's bootstrap address.
