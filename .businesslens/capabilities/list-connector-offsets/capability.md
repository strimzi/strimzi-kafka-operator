---
availability:
  - place: custom-resources
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractConnectOperator.java#AbstractConnectOperator
  - kind: doc
    role: context
    target: documentation/modules/configuring/proc-listing-connector-offsets.adoc
  - kind: doc
    role: context
    target: documentation/modules/configuring/proc-altering-connector-offsets.adoc
  - kind: doc
    role: context
    target: documentation/modules/configuring/proc-resetting-connector-offsets.adoc
  - kind: doc
    role: context
    target: documentation/modules/configuring/con-config-mirrormaker2-sync.adoc
---

# List connector offsets

Write the committed offsets of a Kafka Connect connector or a MirrorMaker 2 connector into the ConfigMap its `listOffsets` setting names.
