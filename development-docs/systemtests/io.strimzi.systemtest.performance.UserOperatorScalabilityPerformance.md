# UserOperatorScalabilityPerformance

**Description:** Test suite for measuring User Operator scalability.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default configuration. | Cluster Operator is deployed and running. |

**Labels:**

* [user-operator](labels/user-operator.md)

<hr style="border:1px solid">

## testLatencyUnderLoad

**Description:** This test measures user modification latency statistics under different load levels by performing multiple user modifications to understand how response time scales with system load.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with User Operator configured with more resources to handle load and also non-default `STRIMZI_WORK_QUEUE_SIZE` set to 2048. | Kafka cluster with User Operator is deployed and ready. |
| 2. | For each configured load level (1000, 1500, 2000 existing users), create N KafkaUsers to establish the load. | N KafkaUsers are created and ready, establishing baseline load on the User Operator. |
| 3. | Perform 100 individual user modifications sequentially, measuring the latency of each modification. | Each modification latency is recorded independently. |
| 4. | Calculate latency statistics: min, max, average, P50, P95, and P99 percentiles from the 100 measurements. | Statistical analysis shows how single-user modification latency degrades as system load (number of existing users) increases. |
| 5. | Clean up all users and persist latency metrics to user-operator report directory. | Namespace is cleaned, latency data is saved showing how responsiveness changes at different load levels. |

**Labels:**

* [user-operator](labels/user-operator.md)


## testScalability

**Description:** This test measures throughput (time to process N users in parallel), NOT latency (response time for a single user).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with User Operator configured with more resources to handle load. | Kafka cluster with User Operator is deployed and ready. |
| 2. | For each configured number of users (10, 100, 200, 500), spawn one thread per KafkaUser to perform its full lifecycle concurrently. | N concurrent threads are created, each responsible for one KafkaUser full lifecycle (create, modify, delete). |
| 3. | Each thread performs CREATE: Creates KafkaUser with TLS authentication and ACL authorization. | KafkaUser is created and ready. |
| 4. | Each thread performs MODIFY: Updates ACL rules and adds quotas. | KafkaUser is updated and reconciled. |
| 5. | Each thread performs DELETE: Deletes the KafkaUser. | KafkaUser and associated Secret are deleted. |
| 6. | Wait for all threads to complete their full lifecycle operations and measure total elapsed time. | All KafkaUsers have completed create-modify-delete lifecycle. Total time represents THROUGHPUT capacity (time for all N users to complete), not individual user LATENCY. |
| 7. | Clean up any remaining users and collect performance metrics (e.g., total time to complete all user lifecycles) i.e., reconciliation time. | Namespace is cleaned, performance data is persisted to user-operator report directory for analysis. |

**Labels:**

* [user-operator](labels/user-operator.md)

