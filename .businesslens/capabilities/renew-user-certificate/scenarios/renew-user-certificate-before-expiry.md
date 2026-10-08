---
kind: automatic
routes:
  api: Kubernetes API
steps:
  - text: A user certificate enters its renewal period
    kind: condition
    unattended: true
    entities:
      - entity: user-certificate
        effect: reads
        facts:
          - Expiry
    contexts:
      api:
        place: custom-resources::user-secret
  - text: The current time is inside a maintenance time window, or the cluster defines none
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Maintenance time windows
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product issues a new user certificate
    kind: product
    entities:
      - entity: user-certificate
        effect: changes
        facts:
          - Certificate
          - Private key
          - 'PKCS #12 store'
          - Expiry
    contexts:
      api:
        place: custom-resources::user-secret
---

# Renew a user certificate before it expires

## Trigger

A user certificate approaches expiry.

## Outcome

The user secret holds a new certificate without anyone acting.
