# KafkaST

**Description:** Test suite containing kafka related stuff (i.e., JVM resources, EO, TO or UO removal from Kafka cluster), which ensures proper functioning of Kafka clusters.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator across all namespaces, with custom configuration. | Cluster Operator is deployed. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testAdditionalVolumes

**Description:** This test validates the mounting and usage of additional volumes for Kafka, Kafka Connect, and Kafka Bridge components. It tests whether secret and config map volumes are correctly created, mounted, and accessible across various deployments.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Setup environment prerequisites and configure test storage. | Ensure the environment is in KRaft mode. |
| 2. | Create necessary Kafka resources with additional volumes for secrets and config maps. | Resources are correctly instantiated with specified volumes. |
| 3. | Deploy Kafka, Kafka Connect, and Kafka Bridge with these volumes. | Components are correctly configured with additional volumes. |
| 4. | Verify that all pods (Kafka, Connect, and Bridge) have additional volumes mounted and accessible. | Volumes are correctly mounted and usable within pods. |

**Labels:**

* [kafka](labels/kafka.md)


## testDeployUnsupportedKafka

**Description:** Test to ensure that deploying Kafka with an unsupported version results in the expected error.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage with current context. | Test storage is initialized. |
| 2. | Create KafkaNodePools | KafkaNodePools are created and ready |
| 3. | Deploy Kafka with a non-existing version | Kafka deployment with non-supported version begins |
| 4. | Log Kafka deployment process | Log entry for Kafka deployment is created |
| 5. | Wait for Kafka to not be ready | Kafka is not ready as expected |
| 6. | Verify Kafka status message for unsupported version | Error message for unsupported version is found in Kafka status |

**Labels:**

* [kafka](labels/kafka.md)


## testJvmAndResources

**Description:** This test case verifies that Pod's resources (limits and requests), custom JVM configurations, and expected Java configuration are propagated correctly to Pods, containers, and processes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka and its components with custom specifications, including specifying resources and JVM configuration. | Kafka and its components (ZooKeeper, Entity Operator) are deployed. |
| 2. | For each component (Kafka, ZooKeeper, Topic Operator, User Operator), verify specified configuration of JVM, resources, and also environment variables. | Each of the components has requests and limits assigned correctly, JVM, and environment variables configured according to the specification. |
| 3. | Wait for a time to observe that no initiated components need rolling update. | All Kafka components remain in stable state. |

**Labels:**

* [kafka](labels/kafka.md)


## testKRaftMode

**Description:** This test case verifies basic working of Kafka Cluster managed by Cluster Operator with KRaft.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka annotated to enable KRaft (and additionally annotated to enable node pool management), and configure a KafkaNodePool resource to target the Kafka cluster. | Kafka is deployed, and the KafkaNodePool resource targets the cluster as expected. |
| 2. | Produce and consume messages in given Kafka Cluster. | Clients can produce and consume messages. |
| 3. | Trigger manual Rolling Update. | Rolling update is triggered and completed shortly after. |

**Labels:**

* [kafka](labels/kafka.md)


## testKafkaJBODDeleteClaimsTrueFalse

**Description:** This test case verifies Kafka running with persistent JBOD storage, and configured with the `deleteClaim`  storage property.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with persistent storage and JBOD storage with 2 volumes, both of which are configured to delete their Persistent Volume Claims on Kafka cluster un-provision. | Kafka is deployed, volumes are labeled and linked to Pods correctly. |
| 2. | Verify that labels in Persistent Volume Claims are set correctly. | Persistent Volume Claims contains expected labels and values. |
| 3. | Modify Kafka Custom Resource, specifically 'deleteClaim' property of its first Kafka Volume. | Kafka CR is successfully modified, annotation of according Persistent Volume Claim is changed afterwards by Cluster Operator. |
| 4. | Delete Kafka cluster. | Kafka cluster and its components are deleted, including Persistent Volume Claim of Volume with 'deleteClaim' property set to true. |
| 5. | Verify remaining Persistent Volume Claims. | Persistent Volume Claim referenced by volume of formerly deleted Kafka Custom Resource with property 'deleteClaim' set to true is still present. |

**Labels:**

* [kafka](labels/kafka.md)


## testLabelsExistenceAndManipulation

**Description:** This test case verifies the presence of expected Strimzi specific labels, also labels and annotations specified by user. Some user-specified labels are later modified (new one is added, one is modified) which triggers rolling update after which all changes took place as expected.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with persistent storage and specify custom labels in CR metadata, and also other labels and annotation in PVC metadata. | Kafka is deployed with its default labels and all others specified by user. |
| 2. | Deploy Producer and Consumer configured to produce and consume default number of messages, to make sure Kafka works as expected. | Producer and Consumer are able to produce and consume messages respectively. |
| 3. | Modify configuration of Kafka CR with addition of new labels and modification of existing. | Kafka is rolling and new labels are present in Kafka CR, and managed resources. |
| 4. | Deploy Producer and Consumer configured to produce and consume default number of messages, to make sure Kafka works as expected. | Producer and Consumer are able to produce and consume messages respectively. |

**Labels:**

* [kafka](labels/kafka.md)


## testMessagesAndConsumerOffsetFilesOnDisk

**Description:** This test case verifies correct storage of messages on disk, and their presence even after rolling update of all Kafka Pods. Test case also checks if offset topic related files are present.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy persistent Kafka with corresponding configuration of offsets topic. | Kafka is created with expected configuration. |
| 2. | Create KafkaTopic with corresponding configuration. | KafkaTopic is created with expected configuration. |
| 3. | Execute command to check presence of offsets topic related files. | Files related to Offset topic are present. |
| 4. | Produce default number of messages to already created topic. | Produced messages are present. |
| 5. | Perform rolling update on all Kafka Pods, in this case single broker. | After rolling update is completed all messages are again present, as they were successfully stored on disk. |

**Labels:**

* [kafka](labels/kafka.md)


## testReadOnlyRootFileSystem

**Description:** This test case verifies that Kafka (with all its components, including Zookeeper, Entity Operator, KafkaExporter, CruiseControl) configured with 'withReadOnlyRootFilesystem' can be deployed and also works correctly.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy persistent Kafka with 3 Kafka and Zookeeper replicas, Entity Operator, CruiseControl, and KafkaExporter. Each component has configuration 'withReadOnlyRootFilesystem' set to true. | Kafka and its components are deployed. |
| 2. | Create Kafka producer and consumer. | Kafka clients are successfully created. |
| 3. | Produce and consume messages using created clients. | Messages are successfully sent and received. |

**Labels:**

* [kafka](labels/kafka.md)


## testRegenerateCertExternalAddressChange

**Description:** Test regenerates certificates after changing Kafka's external address.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka without external listener. | Kafka instance is created without an external listener. |
| 2. | Edit Kafka to include an external listener. | External listener is correctly added to the Kafka instance. |
| 3. | Wait until the Kafka component has rolled. | Kafka component rolls successfully with the new external listener. |
| 4. | Compare Kafka broker secrets before and after adding external listener. | Secrets are different before and after adding the external listener. |

**Labels:**

* [kafka](labels/kafka.md)


## testRemoveComponentsFromEntityOperator

**Description:** This test case verifies the correct deployment of the Entity Operator, including both the User Operator and Topic Operator. First, the Entity Operator is modified to exclude the User Operator. Then, it's restored to its default configuration, which includes the User Operator. Next, the Topic Operator is removed, followed by the User Operator, with the Topic Operator already excluded

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with Entity Operator set. | Kafka is deployed, and Entity Operator consists of both Topic Operator and User Operator. |
| 2. | Remove User Operator from the Kafka specification. | User Operator container is deleted. |
| 3. | Set User Operator back in the Kafka specification. | User Operator container is recreated. |
| 4. | Remove Topic Operator from the Kafka specification. | Topic Operator container is removed from Entity Operator. |
| 5. | Remove User Operator from the Kafka specification. | Entity Operator Pod is removed, as there are no other containers present. |

**Labels:**

* [kafka](labels/kafka.md)


## testResizeJbodVolumes

**Description:** This test verifies the functionality of resizing JBOD storage volumes on a Kafka cluster. It checks that the system can handle volume size changes and performs a rolling update to apply these changes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with JBOD storage and initial volume sizes. | Kafka cluster is operational. |
| 2. | Produce and consume messages continuously to simulate cluster activity. | Message traffic is consistent. |
| 3. | Increase the size of one of the JBOD volumes. | Volume size change is applied. |
| 4. | Verify that the updated volume size is reflected. | PVC reflects the new size. |
| 5. | Ensure continuous message production and consumption are unaffected during the update process. | Message flow continues without interruption. |

**Labels:**

* [kafka](labels/kafka.md)

