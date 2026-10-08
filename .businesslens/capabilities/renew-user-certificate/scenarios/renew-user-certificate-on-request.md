---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the user secret to renew the certificate
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: user-certificate
        effect: changes
        facts:
          - Renewal requested
    contexts:
      api:
        place: custom-resources::user-secret
  - text: The Product issues a new user certificate and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: user-certificate
        effect: changes
        facts:
          - Certificate
          - Private key
          - 'PKCS #12 store'
          - Expiry
          - Renewal requested
    contexts:
      api:
        place: custom-resources::user-secret
---

# Renew a user certificate on request

## Trigger

The Strimzi administrator wants a user certificate replaced now.

## Outcome

The user secret holds a new certificate.
