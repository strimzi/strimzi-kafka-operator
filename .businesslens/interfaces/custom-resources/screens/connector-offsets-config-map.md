---
entities:
  - entity: connector
    shows:
      - Offsets
    collects:
      - Offsets
  - entity: mirror-maker-2
    shows:
      - Offsets
    collects:
      - Offsets
---

# Connector offsets ConfigMap

The ConfigMap a connector names for listing or altering its offsets: Strimzi writes listed offsets into it, and the Strimzi administrator writes the offsets to apply.
