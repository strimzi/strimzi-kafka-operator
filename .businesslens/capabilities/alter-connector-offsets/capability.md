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

# Alter connector offsets

Set the offsets of a stopped Kafka Connect connector or MirrorMaker 2 connector to the values in the ConfigMap its `alterOffsets` setting names.
