---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the replicas in the KafkaTopic resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts:
          - Replicas
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: Cruise Control is deployed for the Kafka cluster
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Cruise Control
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product asks Cruise Control to change the replication factor and reports the change as pending, then ongoing
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts:
          - Replicas change
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product clears the reported change once Cruise Control finishes
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts:
          - Replicas change
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Change a topic's replication factor

## Trigger

A topic needs more or fewer replicas.

## Outcome

Every partition of the topic has the new replication factor.

## Edge cases

- A replication factor change Cruise Control fails to carry out is reported in the replicas change with its message.
