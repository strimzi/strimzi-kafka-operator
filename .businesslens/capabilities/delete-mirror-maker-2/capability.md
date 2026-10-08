---
availability:
  - place: custom-resources
domain: mirror-maker
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaMirrorMaker2AssemblyOperator.java#KafkaMirrorMaker2AssemblyOperator
---

# Delete MirrorMaker 2

Stop mirroring by deleting the `KafkaMirrorMaker2` resource.
