# KafkaVersionsST

**Description:** Test checking basic functionality for each supported Kafka version. Ensures that Kafka functionality such as deployment, Topic Operator, User Operator, and message transmission via PLAIN and TLS listeners are working correctly.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy cluster operator with default installation | Cluster operator is deployed |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testKafkaWithVersion

**Description:** Test checking basic functionality for each supported Kafka version. Ensures that Kafka functionality such as deployment, Topic Operator, User Operator, and message transmission via PLAIN and TLS listeners are working correctly.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with specified version | Kafka cluster is deployed without any issue |
| 2. | Verify the Topic Operator creation | Topic Operator is working correctly |
| 3. | Verify the User Operator creation | User Operator is working correctly with SCRAM-SHA and ACLs |
| 4. | Send and receive messages via PLAIN with SCRAM-SHA | Messages are sent and received successfully |
| 5. | Send and receive messages via TLS | Messages are sent and received successfully |

**Labels:**

* [kafka](labels/kafka.md)

