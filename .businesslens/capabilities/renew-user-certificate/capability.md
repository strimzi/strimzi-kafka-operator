---
availability:
  - place: custom-resources
domain: users
references:
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/KafkaUserModel.java#maybeGenerateCertificates
  - kind: doc
    role: context
    target: documentation/modules/security/con-certificate-renewal.adoc
---

# Renew a user certificate

Renew a TLS user's certificate on request, when it approaches expiry, or when the clients CA certificate changes.
