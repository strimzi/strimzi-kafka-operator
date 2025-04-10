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

## testTieredStorageWithAivenFileSystemPlugin

**Description:** This testcase is focused on testing of Tiered Storage integration implemented within Strimzi. The tests use the FileSystem plugin in Aiven Tiered Storage project (<a href="https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main">tiered-storage-for-apache-kafka</a>).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploys KafkaNodePool resource with PV of size 10Gi. | KafkaNodePool resource is deployed successfully with specified configuration. |
| 2. | Deploys a NFS instance with RoleBinding, serviceAccount, service, StorageClass... related resources. | NFS resources are deployed successfully. |
| 3. | Deploy Kafka CustomResource with additional NFS volume mounted and Tiered Storage configuration pointing to NFS path, using a built Kafka image. Reduce the `remote.log.manager.task.interval.ms` and `log.retention.check.interval.ms` to minimize delays during log uploads and deletions. | Kafka CustomResource is deployed successfully with optimized intervals to speed up log uploads and local log deletions. |
| 4. | Creates topic with enabled Tiered Storage sync with size of segments set to 10mb (this is needed to speed up the sync). | Topic is created successfully with Tiered Storage enabled and segment size of 10mb. |
| 5. | Starts continuous producer to send data to Kafka. | Continuous producer starts sending data to Kafka. |
| 6. | Wait until the NFS size is greater than one log segment size (contains data from Kafka). | The NFS contains at least one log segment from Kafka. |
| 7. | Wait until the earliest-local offset to be higher than 0. | The log segments uploaded to NFS are deleted locally. |
| 8. | Starts a consumer to consume all the produced messages, some of the messages should be located in NFS. | Consumer can consume all the messages successfully. |
| 9. | Alter the topic config to retention.ms=10 sec to test the remote log deletion. | The topic config is altered successfully. |
| 10. | Wait until the NFS data is deleted. | The data in the NFS data is deleted. |

**Labels:**

* [kafka](labels/kafka.md)


## testTieredStorageWithAivenS3Plugin

**Description:** This testcase is focused on testing of Tiered Storage integration implemented within Strimzi. The tests use the S3 plugin in Aiven Tiered Storage project (<a href="https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main">tiered-storage-for-apache-kafka</a>).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploys KafkaNodePool resource with PV of size 10Gi. | KafkaNodePool resource is deployed successfully with specified configuration. |
| 2. | Deploy Kafka CustomResource with Tiered Storage configuration pointing to Minio S3, using a built Kafka image. Reduce the `remote.log.manager.task.interval.ms` and `log.retention.check.interval.ms` to minimize delays during log uploads and deletions. | Kafka CustomResource is deployed successfully with optimized intervals to speed up log uploads and local log deletions. |
| 3. | Creates topic with enabled Tiered Storage sync with size of segments set to 10mb (this is needed to speed up the sync). | Topic is created successfully with Tiered Storage enabled and segment size of 10mb. |
| 4. | Starts continuous producer to send data to Kafka. | Continuous producer starts sending data to Kafka. |
| 5. | Wait until Minio size is not empty (contains data from Kafka). | Minio contains data from Kafka. |
| 6. | Wait until the earliest-local offset to be higher than 0. | The log segments uploaded to Minio are deleted locally. |
| 7. | Starts a consumer to consume all the produced messages, some of the messages should be located in Minio. | Consumer can consume all the messages successfully. |
| 8. | Alter the topic config to retention.ms=10 sec to test the remote log deletion. | The topic config is altered successfully. |
| 9. | Wait until Minio size is 0. | The data in Minio are deleted. |

**Labels:**

* [kafka](labels/kafka.md)

