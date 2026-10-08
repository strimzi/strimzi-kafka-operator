---
availability:
  - place: custom-resources
domain: certificates
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/CaReconciler.java#CaReconciler
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/InternalCaProvider.java#InternalCaProvider
  - kind: doc
    role: context
    target: documentation/modules/security/con-certificate-renewal.adoc
---

# Renew a CA certificate

Renew a cluster or clients CA certificate. Strimzi renews certificates it generates when their renewal period starts or when asked to; Strimzi administrators who bring their own CA renew it by replacing its certificate. Components are rolled so that they trust the new certificate.
