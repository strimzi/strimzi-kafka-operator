# ReconciliationST

**Description:** Test suite for verifying reconciliation pause functionality across various Strimzi custom resources including Kafka, KafkaConnect, KafkaConnector, KafkaTopic, and KafkaRebalance.

<hr style="border:1px solid">

## testPauseReconciliationInKafkaAndKafkaConnectWithConnector

**Description:** Test verifies that pause reconciliation annotation prevents changes from being applied to Kafka, KafkaConnect, and KafkaConnector resources, and that resuming reconciliation applies pending changes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with node pools. | Kafka cluster is deployed with 3 replicas. |
| 2. | Add pause annotation to Kafka and scale broker pool to 4 replicas. | Kafka reconciliation is paused and no new pods are created. |
| 3. | Remove pause annotation from Kafka. | Kafka is scaled to 4 replicas. |
| 4. | Deploy KafkaConnect with pause annotation. | KafkaConnect reconciliation is paused and no pods are created. |
| 5. | Remove pause annotation from KafkaConnect. | KafkaConnect pod is created. |
| 6. | Create KafkaConnector. | KafkaConnector is deployed successfully. |
| 7. | Add pause annotation to KafkaConnector and scale tasksMax to 4. | KafkaConnector reconciliation is paused and configuration is not updated. |
| 8. | Remove pause annotation from KafkaConnector. | KafkaConnector tasksMax is updated to 4. |

**Labels:**

* [kafka](labels/kafka.md)
* [connect](labels/connect.md)


## testPauseReconciliationInKafkaRebalanceAndTopic

**Description:** Test verifies that pause reconciliation annotation prevents changes from being applied to KafkaTopic and KafkaRebalance resources, and that resuming reconciliation applies pending changes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with Cruise Control and node pools. | Kafka cluster with Cruise Control is deployed. |
| 2. | Create KafkaTopic. | Topic is created and present in Kafka. |
| 3. | Add pause annotation to KafkaTopic and change partition count to 4. | Topic reconciliation is paused and partitions are not changed. |
| 4. | Remove pause annotation from KafkaTopic. | Topic partitions are scaled to 4. |
| 5. | Create KafkaRebalance and wait for ProposalReady state. | KafkaRebalance reaches ProposalReady state. |
| 6. | Add pause annotation to KafkaRebalance and approve it. | Rebalance reconciliation is paused and approval is not triggered. |
| 7. | Remove pause annotation from KafkaRebalance. | KafkaRebalance returns to ProposalReady state. |
| 8. | Approve KafkaRebalance again. | Rebalance is executed and reaches Ready state. |

**Labels:**

* [kafka](labels/kafka.md)
* [cruise-control](labels/cruise-control.md)

