---
entities:
  - entity: user-certificate
    shows:
      - Certificate
      - Private key
      - 'PKCS #12 store'
      - Clients CA certificate
    collects:
      - Renewal requested
  - entity: user-password
    shows:
      - Password
      - JAAS configuration
---

# User secret

The Secret named after a Kafka user that holds its credentials for client applications to mount, annotated to renew the user certificate.
