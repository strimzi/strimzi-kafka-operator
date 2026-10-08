---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets the connector's requested state to running
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Requested state
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product resumes the connector in Kafka Connect
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Run a paused or stopped connector

## Trigger

The connector should process data again.

## Outcome

The connector and its tasks run.
