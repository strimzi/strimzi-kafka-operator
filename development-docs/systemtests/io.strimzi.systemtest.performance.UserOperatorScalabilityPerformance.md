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

**Description:** Verifies User Operator scalability by processing concurrent KafkaUser operations with varying event batch sizes (70, 700, 1400, 3500 events), measuring reconciliation time and collecting performance metrics. What is worth of nothing is that KafkaUser operation generates multiple Kubernetes watch events:  (i.) CREATE triggers 3 events (KafkaUser ADDED + Secret ADDED + KafkaUser status MODIFIED),  (ii.) MODIFY triggers 2 events (KafkaUser MODIFIED + KafkaUser status MODIFIED),  (iii.) DELETE triggers 2 events (KafkaUser DELETED + Secret DELETED). With 7 events per task, batch sizes correspond to 10, 100, 200, and 500 KafkaUsers.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | For each batch size (70, 700, 1400, 3500 events = 10, 100, 200, 500 users), spawn a separate thread for each KafkaUser to perform its full lifecycle (create, modify, delete) concurrently. | Each thread creates a KafkaUser with TLS authentication and ACL authorization rules, waits for it to be ready, modifies it, waits for reconciliation, then deletes it. |
| 2. | Wait for all threads to complete their KafkaUser lifecycle operations. | All KafkaUsers have been created, modified, deleted, and their associated Secrets are removed. |
| 3. | Measure the total reconciliation time for the batch. | Reconciliation time is recorded for performance analysis. |
| 4. | Collect and log performance metrics including work queue size, batch configuration, number of users, and reconciliation time. | Performance data is persisted to the report directory for analysis. |

**Labels:**

* [user-operator](labels/user-operator.md)

