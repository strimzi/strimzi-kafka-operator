---
appliesTo:
  - type: capability
    id: approve-rebalance
  - type: capability
    id: refresh-rebalance
  - type: capability
    id: stop-rebalance
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/rebalance/KafkaRebalanceState.java#KafkaRebalanceState
---

# A rebalance accepts only the requests its state allows

A pending proposal can be stopped or refreshed, a ready proposal approved or refreshed, a running rebalance stopped or refreshed, and a stopped, finished or failed rebalance refreshed. Any other request stays on the resource with an InvalidAnnotation warning and changes nothing.
