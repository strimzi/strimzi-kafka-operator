---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the CA key Secret to replace the key
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: certificate-authority
        effect: changes
        facts:
          - Key replacement requested
    contexts:
      api:
        place: custom-resources::ca-secrets
  - text: The current time is inside a maintenance time window, or the cluster defines none
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Maintenance time windows
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product generates a new key and certificate, keeps the old certificate trusted, and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: certificate-authority
        effect: changes
        facts:
          - CA private key
          - CA certificate
          - Key generation
          - Certificate generation
          - Key replacement requested
    contexts:
      api:
        place: custom-resources::ca-secrets
  - text: The Product rolls the Kafka nodes so that they trust the new certificate, then again so that they use it
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product stops trusting the old certificate once every component uses the new one
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: certificate-authority
        effect: changes
        facts:
          - CA certificate
    contexts:
      api:
        place: custom-resources::ca-secrets
---

# Replace a CA private key on request

## Trigger

The Strimzi administrator needs a new CA key, for example after a compromise.

## Outcome

The cluster uses certificates from the new key and no longer trusts the old certificate.
