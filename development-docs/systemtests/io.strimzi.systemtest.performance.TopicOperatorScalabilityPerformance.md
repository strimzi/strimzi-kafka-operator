# TopicOperatorScalabilityPerformance

**Description:** Test suite for measuring Topic Operator scalability under concurrent topic operations.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with the Topic Operator configured with specific resource limits and batch settings. | Kafka cluster with Topic Operator is deployed and ready. |

**Labels:**

* [topic-operator](labels/topic-operator.md)

<hr style="border:1px solid">

## testScalability

**Description:** This test measures throughput (time to process N topics in parallel), NOT latency (response time for a single topic).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | For each configured number of topics (10, 100, 500, 1000), spawn one thread per KafkaTopic to perform its full lifecycle concurrently. | N concurrent threads are created, each responsible for one KafkaTopic full lifecycle (create, modify, delete). |
| 2. | Each thread performs CREATE: Creates KafkaTopic with specified partitions and replicas. | KafkaTopic is created and ready. |
| 3. | Each thread performs MODIFY: Updates topic configuration. | KafkaTopic is updated and reconciled. |
| 4. | Each thread performs DELETE: Deletes the KafkaTopic. | KafkaTopic is deleted from the cluster. |
| 5. | Wait for all threads to complete their full lifecycle operations and measure total elapsed time. | All KafkaTopics have completed create-modify-delete lifecycle. Total time represents THROUGHPUT capacity (time for all N topics to complete), not individual topic LATENCY. |
| 6. | Clean up any remaining topics and collect performance metrics, including total reconciliation time. | Namespace is cleaned, performance data is persisted to topic-operator report directory for analysis. |

**Labels:**

* [topic-operator](labels/topic-operator.md)

