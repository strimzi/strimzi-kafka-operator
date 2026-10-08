---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets the requested state of a mirror's connector to stopped or paused
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
  - text: The Product stops or pauses that connector in its workers and reports its status
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

# Stop or pause a MirrorMaker 2 connector

## Trigger

Mirroring from one source should halt for a while, for example before its offsets are changed.

## Outcome

The connector no longer mirrors until its state is set back to running.
