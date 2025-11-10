# PodSetST

**Description:** Test suite for `StrimziPodSet` related functionality and features, which verifies pod set reconciliation behavior.

<hr style="border:1px solid">

## testPodSetOnlyReconciliation

**Description:** This test verifies that when the `STRIMZI_POD_SET_RECONCILIATION_ONLY` environment variable is enabled, only `StrimziPodSet` resources are reconciled, and Kafka configuration changes do not trigger rolling updates of pods.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with broker and controller node pools configured for 3 replicas each. | Kafka cluster with node pools and topics deployed successfully. |
| 2. | Start continuous producer and consumer clients. | Clients are producing and consuming messages. |
| 3. | Enable `STRIMZI_POD_SET_RECONCILIATION_ONLY` environment variable in Cluster Operator. | Cluster Operator restarts with the new environment variable. |
| 4. | Change Kafka readiness probe timeout in the `Kafka` resource. | No pod rolling update occurs despite configuration change. |
| 5. | Delete one Kafka pod. | Pod is recreated by `StrimziPodSet` controller. |
| 6. | Remove `STRIMZI_POD_SET_RECONCILIATION_ONLY` environment variable from Cluster Operator. | Cluster Operator restarts again. |
| 7. | Verify pod rolling update occurs. | Kafka pods are restarted due to the pending configuration change. |
| 8. | Verify `StrimziPodSet` status. | All `StrimziPodSet` resources are ready with matching pod counts. |
| 9. | Verify message continuity. | Clients continued to successfully produce and consume all messages. |

**Labels:**

* [kafka](labels/kafka.md)

