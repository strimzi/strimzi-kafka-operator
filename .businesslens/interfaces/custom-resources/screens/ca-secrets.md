---
entities:
  - entity: certificate-authority
    shows:
      - Purpose
      - CA certificate
      - Certificate generation
      - Key generation
    collects:
      - CA certificate
      - CA private key
      - Certificate generation
      - Key generation
      - Renewal requested
      - Key replacement requested
---

# CA Secrets

The Secrets holding one certificate authority's certificate and its private key, annotated to renew the certificate or replace the key, and replaced by the Strimzi administrator when they run their own CA.
