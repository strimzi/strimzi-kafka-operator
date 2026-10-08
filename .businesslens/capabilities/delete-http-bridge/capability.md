---
availability:
  - place: custom-resources
domain: bridge
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaBridgeAssemblyOperator.java#KafkaBridgeAssemblyOperator
---

# Delete an HTTP Bridge

Remove an HTTP Bridge by deleting its `KafkaBridge` resource.
