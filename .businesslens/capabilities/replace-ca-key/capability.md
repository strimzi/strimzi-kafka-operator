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
    target: documentation/modules/security/proc-replacing-private-keys.adoc
---

# Replace a CA private key

Replace the private key of a Strimzi-managed CA, together with its certificate, without interrupting clients that trust the old certificate during the change.
