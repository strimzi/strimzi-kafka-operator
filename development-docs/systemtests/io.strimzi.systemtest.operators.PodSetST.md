# PodSetST

**Description:** Test suite for StrimziPodSet related functionality and features, which verifies pod set reconciliation behavior.

<hr style="border:1px solid">

## testPodSetOnlyReconciliation

**Description:** Test verifies that when STRIMZI_POD_SET_RECONCILIATION_ONLY environment variable is enabled, only StrimziPodSet resources are reconciled while Kafka configuration changes do not trigger pod rolling updates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with 3 replicas and configure topics for resilience. | Kafka cluster with node pools and topics is deployed successfully. |
| 2. | Start continuous producer and consumer clients. | Clients are producing and consuming messages. |
| 3. | Enable STRIMZI_POD_SET_RECONCILIATION_ONLY environment variable in Cluster Operator. | Cluster Operator rolls out with the new environment variable. |
| 4. | Change Kafka readiness probe timeout. | No pod rolling update occurs despite configuration change. |
| 5. | Delete one Kafka pod. | Pod is recreated by StrimziPodSet controller. |
| 6. | Remove STRIMZI_POD_SET_RECONCILIATION_ONLY environment variable from Cluster Operator. | Cluster Operator rolls out again. |
| 7. | Verify pod rolling update occurs. | Kafka pods are rolled due to the pending configuration change. |
| 8. | Verify StrimziPodSet status. | All StrimziPodSets are ready with matching pod counts. |
| 9. | Verify message continuity. | Continuous clients successfully produced and consumed all messages. |

**Labels:**

* [kafka](labels/kafka.md)

