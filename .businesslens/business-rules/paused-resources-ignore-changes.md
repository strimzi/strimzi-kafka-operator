---
appliesTo:
  - type: capability
    id: pause-reconciliation
  - type: capability
    id: resume-reconciliation
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractOperator.java#reconcileResource
  - kind: code
    role: implementation
    target: topic-operator/src/main/java/io/strimzi/operator/topic/BatchingTopicController.java#BatchingTopicController
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/UserControllerLoop.java#reconcile
---

# A resource whose reconciliation is paused is not changed by its operator

While a resource carries the pause annotation, its operator ignores changes to it and reports ReconciliationPaused; what already runs keeps running. Deleting a paused KafkaTopic or KafkaConnector still deletes its topic or connector.
