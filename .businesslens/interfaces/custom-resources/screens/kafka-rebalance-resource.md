---
entities:
  - entity: rebalance
    shows:
      - Name
      - Mode
      - Brokers
      - Optimization result
      - Progress
      - Status message
    collects:
      - Name
      - Mode
      - Brokers
      - Volumes to empty
      - Goals
      - Movement limits
      - Auto-approval
      - Template
      - Rebalance request
---

# KafkaRebalance resource

The `KafkaRebalance` resource of one rebalance, where its mode and goals are declared, the proposal is reviewed and approved, refreshed or stopped, and progress reported.
