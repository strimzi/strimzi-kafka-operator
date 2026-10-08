---
domain: rebalancing
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/rebalance/KafkaRebalanceSpec.java#KafkaRebalanceSpec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/rebalance/KafkaRebalanceState.java#KafkaRebalanceState
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/rebalance/KafkaRebalanceStatus.java#KafkaRebalanceStatus
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaRebalanceAssemblyOperator.java#KafkaRebalanceAssemblyOperator
---

# Rebalance

A request, declared by a `KafkaRebalance` resource, for Cruise Control to propose and then carry out a redistribution of partition replicas in a Kafka cluster.

## Information kept

- **Name** — the name of the KafkaRebalance resource
- **Mode** — full, add-brokers, remove-brokers or remove-disks
- **Brokers** — the brokers being added or removed, for add-brokers and remove-brokers
- **Volumes to empty** — the broker volumes to move replicas off, for remove-disks
- **Goals** — the optimization goals, and whether hard goals may be skipped
- **Movement limits** — excluded topics, concurrency of partition and leader movements, replication throttle and replica movement strategies
- **Auto-approval** — whether a ready proposal is approved without a person
- **Template** — whether the resource is only a template for automatic rebalancing and never runs itself
- **Rebalance request** — the approve, refresh or stop request set on the resource
- **Optimization result** — the summary of the proposal Cruise Control prepared
- **Progress** — how far an approved rebalance has got
- **Status message** — the reason and message of the latest NotReady or Warning condition

## States

### New

The resource exists and no proposal has been requested yet.

### Pending proposal

Cruise Control is preparing a proposal.

### Proposal ready

A proposal is ready for approval.

### Rebalancing

Cruise Control is moving replicas.

### Ready

The rebalance finished.

### Stopped

The rebalance was stopped before it finished.

### Not ready

The proposal or the rebalance failed.

### Reconciliation paused

Strimzi ignores the resource until the pause is removed.
