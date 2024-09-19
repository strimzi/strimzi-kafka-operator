# CruiseControlST

**Description:** Verify the behavior and correct functionality of Cruise Control in various scenarios using Kafka.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy cluster operator with default installation | Cluster operator is deployed and running |

**Labels:**

* [cruise-control](labels/cruise-control.md)

<hr style="border:1px solid">

## testAutoCreationOfCruiseControlTopicsWithResources

**Description:** Test verifying the automatic creation and configuration of Cruise Control topics with resources.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create test storage and resource manager | Test storage and resource manager are initialized |
| 2. | Set up Kafka brokers and Cruise Control with necessary configurations | Resources are created with the desired configurations |
| 3. | Validate Cruise Control pod's memory resource limits and JVM options | Memory limits and JVM options are correctly set on the Cruise Control pod |
| 4. | Create a Kafka topic and an AdminClient | Kafka topic and AdminClient are successfully created |
| 5. | Verify Cruise Control topics are present | Cruise Control topics are found present in the configuration |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlChangesFromRebalancingtoProposalReadyWhenSpecUpdated

**Description:** Test that ensures Cruise Control transitions from Rebalancing to ProposalReady state when the KafkaRebalance spec is updated.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create test storage instance | TestStorage instance is created |
| 2. | Create broker and controller node pools | Node pools are successfully created |
| 3. | Create Kafka cluster with Cruise Control | Kafka cluster with Cruise Control is created and running |
| 4. | Create KafkaRebalance resource | KafkaRebalance resource is created and running |
| 5. | Wait until KafkaRebalance is in ProposalReady state | KafkaRebalance reaches ProposalReady state |
| 6. | Annotate KafkaRebalance with 'approve' | KafkaRebalance is annotated with approval |
| 7. | Update KafkaRebalance spec to configure replication throttle | KafkaRebalance resource's spec is updated |
| 8. | Wait until KafkaRebalance returns to ProposalReady state | KafkaRebalance re-enters ProposalReady state following the update |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlDuringBrokerScaleUpAndDown

**Description:** Testing the behavior of Cruise Control during both scaling up and down of Kafka brokers using node pools.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage | Test storage is set up with relevant settings and Kubernetes namespace. |
| 2. | Create initial Kafka cluster setup with Cruise Control and topic | Kafka cluster, topic, and scraper pod are created successfully. |
| 3. | Scale Kafka up to a higher number of brokers | Kafka brokers are scaled up to the specified number of replicas. |
| 4. | Create a KafkaRebalance resource with add_brokers mode | KafkaRebalance proposal is ready and processed for adding brokers. |
| 5. | Check the topic's replicas on the new brokers | Topic has replicas on one of the newly added brokers. |
| 6. | Create a KafkaRebalance resource with remove_brokers mode | KafkaRebalance proposal is ready and processed for removing brokers. |
| 7. | Check the topic's replicas only on initial brokers | Topic replicas are only on the initial set of brokers. |
| 8. | Scale Kafka down to the initial number of brokers | Kafka brokers are scaled down to the original number of replicas. |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlIntraBrokerBalancing

**Description:** Test ensuring the intra-broker disk balancing with Cruise Control works as expected.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the test storage and JBOD storage configuration | Test storage and JBOD storage with specific disk sizes are initialized |
| 2. | Create Kafka broker and controller pools using the initialized storage | Kafka broker and controller pools are created and available |
| 3. | Deploy Kafka with Cruise Control enabled | Kafka deployment with Cruise Control is successfully created |
| 4. | Create Kafka Rebalance resource with disk rebalancing configured | Kafka Rebalance resource is created and configured for disk balancing |
| 5. | Wait for the Kafka Rebalance to reach the 'ProposalReady' state | Kafka Rebalance resource reaches the 'ProposalReady' state |
| 6. | Check the status of the Kafka Rebalance for intra-broker disk balancing | The 'provisionStatus' in the optimization result is 'UNDECIDED' |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlReplicaMovementStrategy

**Description:** Test that verifies the configuration and application of custom Cruise Control replica movement strategies.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage and relevant resource configurations | TestStorage and Kafka cluster configurations are initialized |
| 2. | Create Kafka and Cruise Control resources | Kafka and Cruise Control resources are created and deployed |
| 3. | Verify default Cruise Control replica movement strategy | Default replica movement strategy is verified in the configuration |
| 4. | Update Cruise Control configuration with non-default replica movement strategies | Cruise Control configuration is updated with new strategies |
| 5. | Ensure that Cruise Control pod is rolled due to configuration change | Cruise Control pod is rolled and new configuration is applied |
| 6. | Verify the updated Cruise Control configuration | Updated replica movement strategies are verified in Cruise Control configuration |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlTopicExclusion

**Description:** Verify that Kafka Cruise Control excludes specified topics and includes others.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage and set topic names | TestStorage instance is initialized and topic names are set |
| 2. | Create resource with Node Pools for Kafka brokers and controllers | Resources for Kafka brokers and controllers are created and ready |
| 3. | Deploy Kafka with Cruise Control enabled | Kafka cluster with Cruise Control is deployed |
| 4. | Create topics to be excluded and included | Topics 'excluded-topic-1', 'excluded-topic-2', and 'included-topic' are created |
| 5. | Create KafkaRebalance resource excluding specific topics | KafkaRebalance resource is created with 'excluded-.*' topics pattern |
| 6. | Wait for KafkaRebalance to reach ProposalReady state | KafkaRebalance reaches the ProposalReady state |
| 7. | Check optimization result for excluded and included topics | Excluded topics are in the optimization result and included topic is not |
| 8. | Approve the KafkaRebalance proposal | KafkaRebalance proposal is approved |
| 9. | Wait for KafkaRebalance to reach Ready state | KafkaRebalance reaches the Ready state |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlWithApiSecurityDisabled

**Description:** Test the Cruise Control functionality when API security is disabled.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage and resource manager objects | Test storage and resource manager objects are created |
| 2. | Create Kafka Node Pools with 3 brokers each | Kafka Node Pools are successfully created and ready |
| 3. | Create a Kafka cluster with Cruise Control having API security disabled | Kafka cluster with Cruise Control and no API security is deployed |
| 4. | Create a Kafka Rebalance resource | Kafka Rebalance resource is successfully created |
| 5. | Wait for the Kafka Rebalance custom resource state to become ProposalReady | Kafka Rebalance custom resource state is ProposalReady |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlWithRebalanceResourceAndRefreshAnnotation

**Description:** Using multiple Kafka clusters within a single namespace to test Cruise Control with rebalance resource and refresh annotation.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create test storage from the test context | Test storage is initialized |
| 2. | Create broker and controller node pools | Resources for broker and controller node pools are created and available |
| 3. | Create Kafka cluster | Kafka cluster with ephemeral storage is created and available |
| 4. | Deploy Kafka Rebalance resource | Kafka Rebalance resource is deployed and in NotReady state |
| 5. | Enable Cruise Control with tuned spec | Cruise Control is enabled and configured |
| 6. | Perform rolling update on broker pods | All broker pods have rolled successfully |
| 7. | Execute rebalance process | Rebalancing process executed successfully |
| 8. | Annotate Kafka Rebalance resource with 'refresh' | Kafka Rebalance resource is annotated with 'refresh' and reaches ProposalReady state |
| 9. | Execute rebalance process again | Rebalancing process re-executed successfully |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlWithSingleNodeKafka

**Description:** Test verifying that Cruise Control cannot be deployed with a Kafka cluster that has only one broker and ensuring that increasing the broker count resolves the configuration error.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage and set up the error message | Test storage initialized and error message set |
| 2. | Deploy single-node Kafka with Cruise Control | Kafka and Cruise Control deployment initiated |
| 3. | Verify that the Kafka status contains the error message related to single-node configuration | Error message confirmed in Kafka status |
| 4. | Increase the Kafka nodes to 3 | Kafka node count increased to 3 |
| 5. | Check that the Kafka status no longer contains the single-node error message | Error message resolved |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testKafkaRebalanceAutoApprovalMechanism

**Description:** Test the Kafka Rebalance auto-approval mechanism.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage and resource manager. | Test storage and resource manager initialized. |
| 2. | Create Kafka broker and controller node pools. | Kafka broker and controller node pools are created. |
| 3. | Deploy Kafka cluster with Cruise Control. | Kafka cluster with Cruise Control is deployed. |
| 4. | Create KafkaRebalance resource with auto-approval enabled. | KafkaRebalance resource with auto-approval is created. |
| 5. | Perform re-balancing process with auto-approval. | Re-balancing process completes successfully with auto-approval. |

**Labels:**

* [cruise-control](labels/cruise-control.md)

