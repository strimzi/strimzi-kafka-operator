---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaUser resource with TLS authentication
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: creates
        to: Not ready
        facts:
          - Name
          - Authentication
          - Quotas
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The clients CA is issued by cert-manager
    kind: condition
    entities:
      - entity: certificate-authority
        effect: reads
        facts:
          - Issuer
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product refuses the Kafka user and reports that tls-external must be used instead
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

# Create a TLS user when cert-manager issues the clients CA

## Trigger

The Kafka cluster's clients CA is managed by cert-manager.

## Outcome

No user certificate is issued and the user stays Not ready with the reason.
