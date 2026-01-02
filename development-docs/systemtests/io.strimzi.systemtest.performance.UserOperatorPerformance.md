# UserOperatorPerformance

**Description:** Test suite for measuring User Operator capacity and performance limits.

**Labels:**

* [user-operator](labels/user-operator.md)

<hr style="border:1px solid">

## testCapacity

**Description:** This test measures the maximum capacity of KafkaUsers that can be managed by the User Operator by incrementally creating users until failure.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with User Operator configured with specified thread pool sizes, cache refresh interval, and batch settings. | Kafka cluster with User Operator is deployed and ready. |
| 2. | Start collecting User Operator metrics. | Metrics collection is running. |
| 3. | Create KafkaUsers with TLS authentication in batches of 100. | Users are created and reach Ready state. |
| 4. | Continue creating user batches until the User Operator fails to reconcile. | Maximum capacity is reached. |
| 5. | Collect logs from User Operator and Kafka pods for analysis. | Logs are collected for identifying bottlenecks. |
| 6. | Clean up all KafkaUsers and persist performance metrics. | Namespace is cleaned and performance data is saved to user-operator report directory. |

**Labels:**

* [user-operator](labels/user-operator.md)

