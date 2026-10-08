---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the authorization or broker configuration in the Kafka resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Authorization
          - Broker configuration
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product applies the change to the running Kafka nodes
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Change a Kafka cluster's configuration

## Trigger

The Strimzi administrator needs the cluster configured differently.

## Outcome

The cluster runs with the new configuration and stays Ready; clients keep access to their partitions throughout.

## Edge cases

- Broker properties Strimzi manages itself, such as listener and security settings, are ignored when set in the broker configuration.
- A Kafka node that does not become ready after a restart stops the rolling update, and the Kafka resource reports Not ready.
- When a reconciliation fails, the Kafka resource keeps reporting the last known cluster ID and versions.

## Decision points

### Applying the change

Can Kafka apply the change without restarting nodes?

- Only broker properties Kafka can update dynamically changed → the Product updates them on the running brokers, and restarts the nodes only if that fails
- Anything else changed → the Product rolls the affected nodes one at a time, controllers before brokers and the active controller after the other controllers, waiting for each to rejoin and keeping partitions available
