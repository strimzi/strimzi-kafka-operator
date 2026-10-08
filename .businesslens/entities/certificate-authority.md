---
domain: certificates
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/common/CertificateAuthority.java#CertificateAuthority
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/CaReconciler.java#CaReconciler
  - kind: doc
    role: context
    target: documentation/modules/security/con-certificate-renewal.adoc
---

# Certificate authority

One of a Kafka cluster's two certificate authorities: the cluster CA, which signs the certificates its components use, and the clients CA, which signs the certificates of its Kafka users.

## Information kept

- **Purpose** — cluster CA or clients CA
- **Issuer** — Strimzi, your own CA, or cert-manager
- **Validity days** — how long generated certificates are valid
- **Renewal days** — how long before expiry the renewal period starts
- **Expiration policy** — whether an expiring CA certificate is renewed with the same key or replaced with a new key
- **Secret owner reference** — whether the CA Secrets are deleted together with the Kafka resource
- **CA certificate** — the current CA certificate, and any older ones still trusted during a renewal
- **CA private key** — the CA's private key
- **Certificate generation** — a counter raised each time the CA certificate changes
- **Key generation** — a counter raised each time the CA key changes
- **Renewal requested** — a request to renew the CA certificate now
- **Key replacement requested** — a request to replace the CA private key now
