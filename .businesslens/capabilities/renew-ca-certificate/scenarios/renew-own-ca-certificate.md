---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator puts the renewed CA certificate in the CA certificate Secret and raises its certificate generation
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: certificate-authority
        effect: changes
        facts:
          - CA certificate
          - Certificate generation
    contexts:
      api:
        place: custom-resources::ca-secrets
  - text: The Product rolls the cluster's Kafka nodes one at a time so that they trust the renewed certificate
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

# Renew your own CA certificate

## Trigger

A CA the Strimzi administrator provides approaches expiry.

## Outcome

The cluster trusts and uses the renewed certificate.

## Edge cases

- Strimzi never renews a CA the Strimzi administrator provides; an expiring one is only logged.
