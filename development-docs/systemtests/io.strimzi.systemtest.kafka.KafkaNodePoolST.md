# KafkaNodePoolST

**Description:** This test suite verifies various functionalities of Kafka Node Pools in a Kafka cluster.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Ensure the environment is not using OLM or Helm and Kafka Node Pools are enabled | Environment is validated |
| 2. | Install the default cluster operator | Cluster operator is installed |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testKafkaManagementTransferToAndFromKafkaNodePool

**Description:** This test case verifies transfer of Kafka Cluster from and to management by KafkaNodePool, by creating corresponding Kafka and KafkaNodePool custom resources and manipulating according Kafka annotation.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with annotated to enable management by KafkaNodePool, and KafkaNodePool targeting given Kafka Cluster. | Kafka is deployed, KafkaNodePool custom resource is targeting Kafka Cluster as expected. |
| 2. | Modify KafkaNodePool by increasing number of Kafka Replicas. | Number of Kafka Pods is increased to match specification from KafkaNodePool. |
| 3. | Produce and consume messages in given Kafka Cluster. | Clients can produce and consume messages. |
| 4. | Modify Kafka custom resource annotation strimzi.io/node-pool to disable management by KafkaNodePool. | StrimziPodSet is modified, replacing former one, Pods are replaced and specification from KafkaNodePool (i.e., changed replica count) are ignored. |
| 5. | Produce and consume messages in given Kafka Cluster. | Clients can produce and consume messages. |
| 6. | Modify Kafka custom resource annotation strimzi.io/node-pool to enable management by KafkaNodePool. | New StrimziPodSet is created, replacing former one, Pods are replaced and specification from KafkaNodePool (i.e., changed replica count) has priority over Kafka specification. |
| 7. | Produce and consume messages in given Kafka Cluster. | Clients can produce and consume messages. |

**Labels:**

* [kafka](labels/kafka.md)


## testKafkaNodePoolBrokerIdsManagementUsingAnnotations

**Description:** This test case verifies the management of broker IDs in Kafka Node Pools using annotations.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka instance with annotations to manage Node Pools and Initial NodePool (Initial) to hold Topics and act as controller. | Kafka instance is deployed according to Kafka and KafkaNodePool custom resource, with IDs 90, 91. |
| 2. | Deploy additional 2 NodePools (A,B) with 1 and 2 replicas, and preset 'next-node-ids' annotations holding resp. values ([4],[6]). | NodePools are deployed, NodePool A contains ID 4, NodePool B contains IDs 6, 0. |
| 3. | Annotate NodePool A 'next-node-ids' and NodePool B 'remove-node-ids' respectively ([20-21],[6,55]) afterward scale to 4 and 1 replicas resp. | NodePools are scaled, NodePool A contains IDs 4, 20, 21, 1. NodePool B contains ID 0. |
| 4. | Annotate NodePool A 'remove-node-ids' and NodePool B 'next-node-ids' respectively ([20],[1]) afterward scale to 2 and 6 replicas resp. | NodePools are scaled, NodePool A contains IDs 1, 4. NodePool B contains IDs 2, 3, 5. |

**Labels:**

* [kafka](labels/kafka.md)


## testNodePoolsAdditionAndRemoval

**Description:** This test case verifies the possibility of adding and removing Kafka Node Pools into an existing Kafka cluster.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka instance with annotations to manage Node Pools and Initial 2 NodePools. | Kafka instance is deployed according to Kafka and KafkaNodePool custom resource. |
| 2. | Create KafkaTopic with replica number requiring all Kafka Brokers to be present, Deploy clients and transmit messages and remove KafkaTopic. | Transition of messages is finished successfully, KafkaTopic created and cleaned as expected. |
| 3. | Add extra KafkaNodePool with broker role to the Kafka. | KafkaNodePool is deployed and ready. |
| 4. | Create KafkaTopic with replica number requiring all Kafka Brokers to be present, Deploy clients and transmit messages and remove KafkaTopic. | Transition of messages is finished successfully, KafkaTopic created and cleaned as expected. |
| 5. | Remove one of kafkaNodePool with broker role. | KafkaNodePool is removed, Pods are deleted, but other pods in Kafka are stable and ready. |
| 6. | Create KafkaTopic with replica number requiring all the remaining Kafka Brokers to be present, Deploy clients and transmit messages and remove KafkaTopic. | Transition of messages is finished successfully, KafkaTopic created and cleaned as expected. |

**Labels:**

* [kafka](labels/kafka.md)


## testNodePoolsRolesChanging

**Description:** This test case verifies changing of roles in Kafka Node Pools.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka instance with annotations to manage Node Pools and Initial 2 NodePools, both with mixed role, first one stable, second one which will be modified. | Kafka instance with initial Node Pools is deployed. |
| 2. | Create KafkaTopic with replica number requiring all Kafka Brokers to be present. | KafkaTopic is created. |
| 3. | Annotate one of Node Pools to perform manual Rolling Update. | Rolling Update started. |
| 4. | Change role of Kafka Node Pool from mixed to controller only role. | Role Change is prevented due to existing KafkaTopic replicas and ongoing Rolling Update. |
| 5. | Original Rolling Update finishes successfully. | Rolling Update is completed. |
| 6. | Delete previously created KafkaTopic. | KafkaTopic is deleted and Node Pool role change is initiated. |
| 7. | Change role of Kafka Node Pool from controller only to mixed role. | Kafka Node Pool changes role to mixed role. |
| 8. | Produce and consume messages on newly created KafkaTopic with replica count requiring also new brokers to be present. | Messages are produced and consumed successfully. |

**Labels:**

* [kafka](labels/kafka.md)

