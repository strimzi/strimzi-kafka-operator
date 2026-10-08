---
availability:
  - place: custom-resources
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractOperator.java#reconcileResource
  - kind: doc
    role: context
    target: documentation/modules/operators/proc-pausing-reconciliation.adoc
---

# Resume reconciliation

Remove the pause from a resource so its operator applies it again.
