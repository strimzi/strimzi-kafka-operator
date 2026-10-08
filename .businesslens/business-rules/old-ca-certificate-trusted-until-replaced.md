---
appliesTo:
  - type: capability
    id: replace-ca-key
  - type: capability
    id: renew-ca-certificate
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/CaReconciler.java#CaReconciler
---

# An old CA certificate stays trusted until every component uses the new one

When a CA certificate is renewed or its key replaced, Strimzi keeps trusting the old certificate and removes it only once every pod and certificate Secret of the cluster carries the new certificate generation; expired certificates are dropped.
