# TopicOperatorPerformance

**Description:** Test suite for measuring Topic Operator capacity and performance limits.

**Labels:**

* [topic-operator](labels/topic-operator.md)

<hr style="border:1px solid">

## testCapacity

**Description:** This test measures the maximum capacity of KafkaTopics that can be managed by the Topic Operator by incrementally creating topics until failure.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with the Topic Operator configured with specified batch size and linger time. | Kafka cluster with Topic Operator is deployed and ready. |
| 2. | Start collecting Topic Operator metrics. | Metrics collection is running. |
| 3. | Create KafkaTopics in batches of 100, each with 12 partitions and 3 replicas. | Topics are created and reach Ready state. |
| 4. | Continue creating topic batches until the Topic Operator fails to reconcile. | Maximum capacity is reached and failure is detected. |
| 5. | Collect logs from Topic Operator and Kafka pods for analysis. | Logs are collected for identifying bottlenecks. |
| 6. | Clean up all KafkaTopics and persist performance metrics. | Namespace is cleaned and performance data is saved to topic-operator report directory. |

**Labels:**

* [topic-operator](labels/topic-operator.md)

