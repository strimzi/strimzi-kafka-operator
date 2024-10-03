# TieredStorageST

**Description:** This test suite covers scenarios for Tiered Storage integration implemented within Strimzi.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create test namespace. | Namespace is created. |
| 2. | Build Kafka image based on passed parameters like image full name, base image, Dockerfile path (via Kaniko or OpenShift build), and include the Aiven Tiered Storage plugin from (<a href="https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main">tiered-storage-for-apache-kafka</a>). | Kafka image is built with the Aiven Tiered Storage plugin integrated. |
| 3. | Deploy Minio in test namespace and init the client inside the Minio pod. | Minio is deployed and client is initialized. |
| 4. | Init bucket in Minio for purposes of these tests. | Bucket is initialized in Minio. |
| 5. | Deploy Cluster Operator. | Cluster Operator is deployed. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testTieredStorageWithAivenPlugin

**Description:** This testcase is focused on testing of Tiered Storage integration implemented within Strimzi. The tests use Aiven Tiered Storage plugin (<a href="https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main">tiered-storage-for-apache-kafka</a>).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploys KafkaNodePool resource with PV of size 10Gi. | KafkaNodePool resource is deployed successfully with specified configuration. |
| 2. | Deploys Kafka resource with configuration of Tiered Storage for Aiven plugin, pointing to Minio S3, and with image built in beforeAll. | Kafka resource is deployed successfully with Tiered Storage configuration. |
| 3. | Creates topic with enabled Tiered Storage sync with size of segments set to 10mb (this is needed to speed up the sync). | Topic is created successfully with Tiered Storage enabled and segment size of 10mb. |
| 4. | Starts continuous producer to send data to Kafka. | Continuous producer starts sending data to Kafka. |
| 5. | Wait until Minio size is not empty (contains data from Kafka). | Minio contains data from Kafka. |

**Labels:**

* [kafka](labels/kafka.md)

