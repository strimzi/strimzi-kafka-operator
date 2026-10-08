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
    target: documentation/modules/configuring/proc-manual-restart-connector.adoc
  - kind: doc
    role: context
    target: documentation/modules/configuring/proc-manual-restart-connector-task.adoc
---

# Restart a connector

Restart a connector, with all or only its failed tasks, or one task, on request; and restart failed connectors automatically when auto-restart is enabled.
