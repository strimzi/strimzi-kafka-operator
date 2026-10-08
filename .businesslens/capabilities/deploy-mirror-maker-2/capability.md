---
availability:
  - place: custom-resources
domain: mirror-maker
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaMirrorMaker2AssemblyOperator.java#KafkaMirrorMaker2AssemblyOperator
  - kind: doc
    role: context
    target: documentation/modules/configuring/con-config-mirrormaker2.adoc
---

# Deploy MirrorMaker 2

Create a MirrorMaker 2 deployment that mirrors topics and consumer group offsets from source clusters into a target cluster.
