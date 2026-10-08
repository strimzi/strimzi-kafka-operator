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
    target: documentation/modules/configuring/proc-manual-stop-pause-mirrormaker2-connector.adoc
---

# Change a MirrorMaker 2 connector's state

Run, pause or stop the source or checkpoint connector of a MirrorMaker 2 mirror by setting its requested state.
