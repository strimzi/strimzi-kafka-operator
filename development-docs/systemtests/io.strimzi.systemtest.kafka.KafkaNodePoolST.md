# KafkaNodePoolST

**Description:** This test suite verifies various functionalities of KafkaNodePools in a Kafka cluster.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Ensure the environment is not using OLM or Helm and KafkaNodePools are enabled. | Environment is validated. |
| 2. | Install the default Cluster Operator. | Cluster operator is installed. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testKafkaManagementTransferToAndFromKafkaNodePool

**Description:** This test verifies Kafka cluster migration to and from KafkaNodePools, using the necessary Kafka and KafkaNodePool resources and annotations.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with the annotation to enable KafkaNodePool management, and configure a KafkaNodePool resource to target the Kafka cluster. | Kafka is deployed, and the KafkaNodePool resource targets the cluster as expected. |
| 2. | Modify KafkaNodePool by increasing number of Kafka replicas. | Number of Kafka Pods is increased to match specification from KafkaNodePool. |
| 3. | Produce and consume messages in given Kafka cluster. | Clients can produce and consume messages. |
| 4. | Disable KafkaNodePool management in the Kafka CustomResource using the KafkaNodePool annotation. |  StrimziPodSet is modified, pods are replaced, and any KafkaNodePool specifications (i.e., changed replica count) are ignored. |
| 5. | Produce and consume messages in given Kafka cluster. | Clients can produce and consume messages. |
| 6. | Enable KafkaNodePool management in the Kafka CustomResource using the KafkaNodePool annotation. | New StrimziPodSet is created, pods are replaced , and any KafkaNodePool specifications  (i.e., changed replica count) take priority over Kafka specifications. |
| 7. | Produce and consume messages in given Kafka cluster. | Clients can produce and consume messages. |

**Labels:**

* [kafka](labels/kafka.md)


## testKafkaNodePoolBrokerIdsManagementUsingAnnotations

**Description:** This test case verifies the management of broker IDs in KafkaNodePools using annotations.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka instance with annotations to manage KafkaNodePools and one initial KafkaNodePool to hold topics and act as controller. | Kafka instance is deployed according to Kafka and KafkaNodePool CustomResource, with IDs 90, 91. |
| 2. | Deploy additional 2 KafkaNodePools (A,B) with 1 and 2 replicas, and preset 'next-node-ids' annotations holding resp. values ([4],[6]). | KafkaNodePools are deployed, KafkaNodePool A contains ID 4, KafkaNodePool B contains IDs 6, 0. |
| 3. | Annotate KafkaNodePool A 'next-node-ids' and KafkaNodePool B 'remove-node-ids' respectively ([20-21],[6,55]) afterward scale to 4 and 1 replicas resp. | KafkaNodePools are scaled, KafkaNodePool A contains IDs 4, 20, 21, 1. KafkaNodePool B contains ID 0. |
| 4. | Annotate KafkaNodePool A 'remove-node-ids' and KafkaNodePool B 'next-node-ids' respectively ([20],[1]) afterward scale to 2 and 6 replicas resp. | KafkaNodePools are scaled, KafkaNodePool A contains IDs 1, 4. KafkaNodePool B contains IDs 2, 3, 5. |

**Labels:**

* [kafka](labels/kafka.md)


## testNodePoolsAdditionAndRemoval

**Description:** This test case verifies the possibility of adding and removing KafkaNodePools into an existing Kafka cluster.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka instance with annotations to manage KafkaNodePools and 2 initial KafkaNodePools. | Kafka instance is deployed according to Kafka and KafkaNodePool CustomResource. |
| 2. | Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present. | KafkaTopic created. |
| 3. | Deploy clients and transmit messages and remove KafkaTopic. | Transition of messages is finished successfully. |
| 4. | Remove KafkaTopic. | KafkaTopic is cleaned as expected. |
| 5. | Add extra KafkaNodePool with broker role to the Kafka. | KafkaNodePool is deployed and ready. |
| 6. | Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present. | KafkaTopic created. |
| 7. | Deploy clients and transmit messages and remove KafkaTopic. | Transition of messages is finished successfully. |
| 8. | Remove KafkaTopic. | KafkaTopic is cleaned as expected. |
| 9. | Remove one KafkaNodePool with broker role. | KafkaNodePool is removed, Pods are deleted, but other pods in Kafka are stable and ready. |
| 10. | Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present. | KafkaTopic created. |
| 11. | Deploy clients and transmit messages and remove KafkaTopic. | Transition of messages is finished successfully. |
| 12. | Remove KafkaTopic. | KafkaTopic is cleaned as expected. |

**Labels:**

* [kafka](labels/kafka.md)


## testNodePoolsRolesChanging

**Description:** This test case verifies changing of roles in KafkaNodePools.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka instance with annotations to manage KafkaNodePools and 2 initial KafkaNodePools, both with mixed role, first one stable, second one which will be modified. | Kafka instance with initial KafkaNodePools is deployed. |
| 2. | Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present. | KafkaTopic created. |
| 3. | Deploy clients and transmit messages and remove KafkaTopic. | Transition of messages is finished successfully. |
| 4. | Remove KafkaTopic. | KafkaTopic is cleaned as expected. |
| 5. | Annotate one of KafkaNodePools to perform manual rolling update. | rolling update started. |
| 6. | Change role of KafkaNodePool from mixed to controller only role. | Role Change is prevented due to existing KafkaTopic replicas and ongoing rolling update. |
| 7. | Original rolling update finishes successfully. | rolling update is completed. |
| 8. | Delete previously created KafkaTopic. | KafkaTopic is deleted and KafkaNodePool role change is initiated. |
| 9. | Change role of KafkaNodePool from controller only to mixed role. | KafkaNodePool changes role to mixed role. |
| 10. | Produce and consume messages on newly created KafkaTopic with replica count requiring also new brokers to be present. | Messages are produced and consumed successfully. |

**Labels:**

* [kafka](labels/kafka.md)

