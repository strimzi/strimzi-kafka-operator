---
kind: automatic
routes:
  api: Kubernetes API
steps:
  - text: A CA certificate Strimzi generated enters its renewal period
    kind: condition
    unattended: true
    entities:
      - entity: certificate-authority
        effect: reads
        facts:
          - CA certificate
          - Renewal days
          - Expiration policy
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
  - text: The Product issues a new CA certificate with the existing key and keeps the old one trusted
    kind: product
    entities:
      - entity: certificate-authority
        effect: changes
        facts:
          - CA certificate
          - Certificate generation
    contexts:
      api:
        place: custom-resources::ca-secrets
  - text: The Product rolls the cluster's Kafka nodes one at a time
    kind: product
    entities:
      - entity: kafka-node
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Renew a CA certificate when its renewal period starts

## Trigger

A CA certificate managed by Strimzi approaches expiry.

## Outcome

The cluster uses the renewed CA certificate without anyone acting.

## Edge cases

- With the replace-key expiration policy, the Product replaces the CA key instead of renewing with the same one.
- A renewed clients CA certificate makes the User Operator reissue every user certificate.
