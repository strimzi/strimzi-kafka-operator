# AutoRebalanceOnImbalanceST

**Description:** This test suite validates the auto-rebalance on imbalance feature, which triggers an automatic rebalancing when Cruise Control detects fixable goal violations in the Kafka cluster.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default installation | Cluster Operator is deployed and running |

**Labels:**

* [cruise-control](labels/cruise-control.md)

<hr style="border:1px solid">

## testAutoRebalanceOnImbalanceCrashRecovery

**Description:** Test crash recovery: when the operator is restarted while a KafkaRebalance resource is in a non-terminal state (e.g., PendingProposal, ProposalReady), the operator resumes monitoring and continues the rebalance to completion.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create broker and controller KafkaNodePools. | Both KafkaNodePools are successfully created. |
| 2. | Create KafkaRebalance template for imbalance mode. | KafkaRebalance template is created. |
| 3. | Deploy Kafka cluster with Cruise Control and imbalance auto-rebalance configured. | Kafka cluster is deployed. |
| 4. | Scale brokers to 5 to create an imbalance — operator transitions to RebalanceOnImbalance. | Auto-rebalance state is RebalanceOnImbalance and imbalance KR is created. |
| 5. | Wait for KafkaRebalance to reach non-terminal state (PendingProposal or ProposalReady). | KafkaRebalance is in one of the non-terminal states. |
| 6. | Simulate operator crash by deleting the operator pod. | Operator pod is deleted and restarted by the Deployment. |
| 7. | Wait for the operator to restart. | Operator pod is running again. |
| 8. | Verify operator finds existing KafkaRebalance and resumes monitoring. | Operator resumes from current KafkaRebalance state. |
| 9. | Wait for the rebalance to complete. | Auto-rebalance state returns to Idle and KafkaRebalance is deleted. |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testAutoRebalanceOnImbalanceIgnoredDuringScaleOperation

**Description:** Test that auto-rebalance on imbalance is not triggered when a scale operation (add-brokers) is already in progress. The operator prioritizes the scale-up rebalance over imbalance detection.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create broker and controller KafkaNodePools with 3 brokers. | Both KafkaNodePools are successfully created. |
| 2. | Create KafkaRebalance templates for imbalance and add-brokers modes. | KafkaRebalance templates are created. |
| 3. | Deploy Kafka cluster with Cruise Control configured for both imbalance and add-brokers auto-rebalance. | Kafka cluster is deployed. |
| 4. | Scale up brokers to 5 to trigger the add-brokers auto-rebalance. | Auto-rebalance state transitions to RebalanceOnScaleUp. |
| 5. | Verify no imbalance KafkaRebalance CR is created while the scale-up rebalance is in progress. | No imbalance KR exists; operator ignores CC violation detection during scale operations. |
| 6. | Wait for the scale-up rebalance to complete. | Auto-rebalance state returns to Idle. |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testAutoRebalanceOnImbalanceMaintenanceWindow

**Description:** Test that auto-rebalance on imbalance does not trigger when the operator is outside the maintenance window, and does trigger once the cluster enters the maintenance window.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create broker and controller KafkaNodePools. | Both KafkaNodePools are successfully created. |
| 2. | Create KafkaRebalance template for imbalance mode. | KafkaRebalance template is created. |
| 3. | Deploy Kafka cluster with Cruise Control and imbalance auto-rebalance configured with a future maintenance window. | Kafka cluster is deployed with maintenance window set to the future. |
| 4. | Scale brokers to 5 to create an imbalance. | Cluster scaled to 5 brokers; imbalance created. |
| 5. | Verify auto-rebalance does NOT trigger outside the maintenance window. | State stays Idle, no imbalance KafkaRebalance is created. |
| 6. | Update the maintenance window to the current time. | Maintenance window is now active. |
| 7. | Verify auto-rebalance triggers within the maintenance window. | State transitions to RebalanceOnImbalance and then Idle. |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testAutoRebalanceOnImbalanceSuccessful

**Description:** Test the full lifecycle of auto-rebalance on imbalance: Cruise Control detects a goal violation after scaling up the cluster, an auto-rebalancing KafkaRebalance CR is automatically created, rebalancing completes, and the auto-rebalance state returns to Idle.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create broker and controller KafkaNodePools with 3 brokers. | Both KafkaNodePools are successfully created. |
| 2. | Create KafkaRebalance template for imbalance mode with anomaly detection goals. | KafkaRebalance template is created. |
| 3. | Deploy Kafka cluster with Cruise Control configured for imbalance auto-rebalance and fast anomaly detection. | Kafka cluster with Cruise Control is deployed. |
| 4. | Ensure the auto-rebalance state is Idle. | Auto-rebalance state is Idle. |
| 5. | Scale brokers to 5 without add-brokers auto-rebalance — new brokers get no replicas, creating an imbalance. | Cluster scaled to 5 brokers; brokers 3 and 4 have no replicas. |
| 6. | Cruise Control detects the goal violation and the operator creates the imbalance auto-rebalancing KafkaRebalance CR. | Auto-rebalance state transitions to RebalanceOnImbalance. |
| 7. | Wait for rebalance to complete. | Auto-rebalance state returns to Idle and KafkaRebalance CR is deleted. |

**Labels:**

* [cruise-control](labels/cruise-control.md)

