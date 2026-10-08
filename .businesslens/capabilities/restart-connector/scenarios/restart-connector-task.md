---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaConnector resource with the ID of the task to restart
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Restart request
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product restarts that task and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Connector status
          - Restart request
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Restart one connector task

## Trigger

One task of a connector has failed.

## Outcome

The task has restarted.
