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
    target: documentation/modules/configuring/proc-manual-restart-mirrormaker2-connector.adoc
---

# Restart a MirrorMaker 2 connector

Restart one MirrorMaker 2 connector, or one of its tasks, on request; and restart failed MirrorMaker 2 connectors automatically when auto-restart is enabled.
