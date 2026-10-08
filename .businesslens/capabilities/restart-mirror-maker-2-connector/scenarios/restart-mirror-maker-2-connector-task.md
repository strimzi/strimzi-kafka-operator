---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaMirrorMaker2 resource with the connector and task to restart
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Connector restart request
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product restarts the task and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Connector status
          - Connector restart request
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Restart a MirrorMaker 2 connector task

## Trigger

One task of a MirrorMaker 2 connector has failed.

## Outcome

The task has restarted.
