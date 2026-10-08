---
domain: users
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/KafkaUserModel.java#KafkaUserModel
  - kind: doc
    role: context
    target: documentation/modules/security/ref-certificates-and-secrets.adoc
---

# User certificate

The certificate and private key the clients CA issues to a Kafka user with mutual TLS authentication, kept in the user secret that client applications mount.

## Information kept

- **Certificate** — the user certificate, signed by the clients CA, with the user name as its common name
- **Private key** — the user's private key
- **PKCS #12 store** — the certificate and key as a PKCS #12 store, with its password
- **Clients CA certificate** — the clients CA certificate that signed it
- **Expiry** — when the certificate expires
- **Renewal requested** — a request to renew the certificate now
