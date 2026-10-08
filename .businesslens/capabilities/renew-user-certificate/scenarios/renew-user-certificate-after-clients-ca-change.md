---
kind: automatic
routes:
  api: Kubernetes API
steps:
  - text: The clients CA certificate differs from the one in the user secret
    kind: condition
    unattended: true
    entities:
      - entity: certificate-authority
        effect: reads
        facts:
          - CA certificate
      - entity: user-certificate
        effect: reads
        facts:
          - Clients CA certificate
    contexts:
      api:
        place: custom-resources::user-secret
  - text: The Product issues a new user certificate signed by the current clients CA, without waiting for a maintenance time window
    kind: product
    entities:
      - entity: user-certificate
        effect: changes
        facts:
          - Certificate
          - Private key
          - 'PKCS #12 store'
          - Clients CA certificate
          - Expiry
    contexts:
      api:
        place: custom-resources::user-secret
---

# Reissue user certificates after the clients CA changes

## Trigger

The clients CA certificate was renewed or replaced.

## Outcome

The user secret holds a certificate signed by the current clients CA.

## Edge cases

- A user secret deleted by hand is recreated with a new certificate, or for a SCRAM-SHA-512 user with a new password.
