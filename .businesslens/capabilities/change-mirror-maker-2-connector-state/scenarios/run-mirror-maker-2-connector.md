---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets the requested state of a mirror's connector to running
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Connector states
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product resumes that connector and reports its status
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Run a stopped or paused MirrorMaker 2 connector

## Trigger

Mirroring from the source should continue.

## Outcome

The connector mirrors again.
