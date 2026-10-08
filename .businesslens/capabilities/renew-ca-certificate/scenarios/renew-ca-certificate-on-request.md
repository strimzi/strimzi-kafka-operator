---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the CA certificate Secret to renew it
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: certificate-authority
        effect: changes
        facts:
          - Renewal requested
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
  - text: The Product issues a new CA certificate with the existing key, keeps the old one trusted, and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: certificate-authority
        effect: changes
        facts:
          - CA certificate
          - Certificate generation
          - Renewal requested
    contexts:
      api:
        place: custom-resources::ca-secrets
  - text: The Product rolls the cluster's Kafka nodes one at a time so that they use certificates from the new CA certificate
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

# Renew a Strimzi-managed CA certificate on request

## Trigger

The Strimzi administrator wants a CA certificate renewed before its renewal period.

## Outcome

The cluster uses the renewed CA certificate; clients trusting the old certificate keep working until it expires.
