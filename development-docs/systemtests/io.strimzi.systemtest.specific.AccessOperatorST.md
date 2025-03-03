# AccessOperatorST

## testAccessOperator

**Description:** The `testAccessOperator` test verifies the functionality of Kafka Access Operator together with Kafka and KafkaUser CRs in a real environment. It also verifies that with the credentials and information about the Kafka cluster, the Kafka clients are able to connect and do the message transmission

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka Access Operator using the installation files from packaging folder. | Kafka Access Operator is successfully deployed. |
| 2. | Deploy and create NodePools, Kafka with configured plain and TLS listeners, TLS KafkaUser, and KafkaTopic where we will produce the messages. | All of the resources are successfully deployed/created. |
| 3. | Create KafkaAccess resource pointing to our Kafka cluster (and TLS listener) and TLS KafkaUser; Wait for KafkaAccess' Secret creation. | Both KafkaAccess resource and KafkaAccess' Secret is created. |
| 4. | Collect KafkaAccess' Secret and its data. | Data successfully collected. |
| 5. | Use the data from KafkaAccess Secret in producer and consumer clients, do message transmission. | With the data provided by KAO, both clients can connect to Kafka and do the message transmission. |

**Labels:**

* [kafka-access](labels/kafka-access.md)

