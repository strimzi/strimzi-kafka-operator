# UserOperatorScalabilityPerformance

**Description:** Test suite for measuring User Operator scalability.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default configuration. | Cluster Operator is deployed and running. |
| 2. | Deploy Kafka cluster with User Operator configured with more resources to handle load. | Kafka cluster with User Operator is deployed and ready. |

**Labels:**

* [user-operator](labels/user-operator.md)

<hr style="border:1px solid">

## testScalability

**Description:** This test measures throughput (time to process N users in parallel), NOT latency (response time for a single user).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | For each configured number of users (10, 100, 200, 500), spawn one thread per KafkaUser to perform its full lifecycle concurrently. | N concurrent threads are created, each responsible for one KafkaUser full lifecycle (create, modify, delete). |
| 2. | Each thread performs CREATE: Creates KafkaUser with TLS authentication and ACL authorization. | KafkaUser is created and ready. |
| 3. | Each thread performs MODIFY: Updates ACL rules and adds quotas. | KafkaUser is updated and reconciled. |
| 4. | Each thread performs DELETE: Deletes the KafkaUser. | KafkaUser and associated Secret are deleted. |
| 5. | Wait for all threads to complete their full lifecycle operations and measure total elapsed time. | All KafkaUsers have completed create-modify-delete lifecycle. Total time represents THROUGHPUT capacity (time for all N users to complete), not individual user LATENCY. |
| 6. | Clean up any remaining users and collect performance metrics (e.g., total time to complete all user lifecycles) i.e., reconciliation time. | Namespace is cleaned, performance data is persisted to user-operator report directory for analysis. |

**Labels:**

* [user-operator](labels/user-operator.md)

