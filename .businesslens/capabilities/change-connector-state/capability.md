---
availability:
  - place: custom-resources
domain: kafka-connect
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractConnectOperator.java#AbstractConnectOperator
  - kind: doc
    role: context
    target: documentation/modules/configuring/proc-manual-stop-pause-connector.adoc
---

# Change a connector's state

Run, pause or stop a connector by setting its requested state.
