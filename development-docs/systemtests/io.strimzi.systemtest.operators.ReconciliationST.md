# ReconciliationST

**Description:** Test suite verifying reconciliation pause functionality for Strimzi custom resources including `Kafka`, `KafkaConnect`, `KafkaConnector`, `KafkaTopic`, and `KafkaRebalance`.

<hr style="border:1px solid">

## testPauseReconciliationInKafkaAndKafkaConnectWithConnector

**Description:** This test verifies that pause reconciliation annotation prevents changes from being applied to `Kafka`, `KafkaConnect`, and `KafkaConnector` resources, and that resuming reconciliation applies pending changes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with broker and controller node pools configured for 3 replicas each. | Kafka cluster deployed with 3 broker and 3 controller replicas. |
| 2. | Add the pause annotation to the `Kafka` resource and scale the broker node pool to 4 replicas. | Kafka reconciliation is paused and no new pods are created. |
| 3. | Remove pause annotation from the `Kafka` resource. | Kafka is scaled to 4 replicas. |
| 4. | Deploy a `KafkaConnect` resource with the pause annotation. | Kafka Connect reconciliation is paused and no pods are created. |
| 5. | Remove pause annotation from the `KafkaConnect` resource. | A Kafka Connect pod is created. |
| 6. | Create a `KafkaConnector` resource. | `Kafka Connect connector is deployed successfully. |
| 7. | Add the pause annotation to the `KafkaConnector` resource and scale `tasksMax` to 4. | `KafkaConnector` reconciliation is paused and configuration is not updated. |
| 8. | Remove pause annotation from the `KafkaConnector` resource. | ``KafkaConnector` `tasksMax` is updated to 4. |

**Labels:**

* [kafka](labels/kafka.md)
* [connect](labels/connect.md)


## testPauseReconciliationInKafkaRebalanceAndTopic

**Description:** This test verifies that pause reconciliation annotation prevents changes from being applied to `KafkaTopic` and `KafkaRebalance` resources, and that resuming reconciliation applies pending changes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with Cruise Control, broker node pool (3 replicas), and controller node pool (1 replica). | Kafka cluster with Cruise Control deployed with 3 broker and 1 controller replicas. |
| 2. | Create a `KafkaTopic` resource. | Topic is created and present in Kafka. |
| 3. | Add the pause annotation to the `KafkaTopic` resource and change partition count to 4. | Topic reconciliation is paused and partitions are not changed. |
| 4. | Remove pause annotation from the `KafkaTopic` resource. | Topic partitions are scaled to 4. |
| 5. | Create a `KafkaRebalance` resource and wait for `ProposalReady` state. | `KafkaRebalance` reaches `ProposalReady` state. |
| 6. | Add pause annotation to the KafkaRebalance resource and approve it. | Rebalance reconciliation is paused and approval is not triggered. |
| 7. | Remove pause annotation from the `KafkaRebalance` resource. | `KafkaRebalance` returns to `ProposalReady` state. |
| 8. | Approve the `KafkaRebalance` resource again. | Rebalance is executed and reaches `Ready` state. |

**Labels:**

* [kafka](labels/kafka.md)
* [cruise-control](labels/cruise-control.md)

