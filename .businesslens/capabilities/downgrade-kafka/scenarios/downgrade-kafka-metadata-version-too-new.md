---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets a Kafka version older than the metadata version in use
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Kafka version
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The current metadata version is newer than the requested Kafka version
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Current metadata version
          - Kafka version
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product refuses the downgrade and reports the Kafka cluster not ready with the reason
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        from: Ready
        to: Not ready
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Downgrade below the metadata version

## Trigger

The Strimzi administrator asks for a Kafka version that cannot read the cluster's metadata version.

## Outcome

The Kafka nodes keep running the current version and the Kafka resource is Not ready with the reason.
