# CruiseControlConfigurationST

**Description:** This test suite, verify configuration of the Cruise Control component.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set up the Cluster Operator | Cluster Operator is installed and running |

**Labels:**

* [cruise-control](labels/cruise-control.md)

<hr style="border:1px solid">

## testConfigurationUpdate

**Description:** Test verifying configuration update for Cruise Control and ensuring Kafka Pods did not roll unnecessarily.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create broker and controller KafkaNodePools | Both KafkaNodePools are successfully created |
| 2. | Create and wait for Kafka with Cruise Control | Kafka and Cruise Control are deployed successfully |
| 3. | Take initial snapshots of Kafka and Cruise Control deployments | Snapshots of current deployments are stored |
| 4. | Update Cruise Control configuration with new performance tuning options | Configuration update initiated |
| 5. | Verify Cruise Control Pod rolls after configuration change | Cruise Control Pod restarts to apply new configurations |
| 6. | Verify Kafka Pods did not roll after configuration change | Kafka Pods remain unchanged |
| 7. | Verify new configurations are applied to Cruise Control in Kafka CR | New configurations are correctly applied |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testDeployAndUnDeployCruiseControl

**Description:** Deploy and subsequently remove Cruise Control from Kafka cluster to verify system stability and correctness of configuration management.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create broker and controller KafkaNodePools | Both KafkaNodePools are successfully created |
| 2. | Deploy Kafka with Cruise Control | Kafka cluster with Cruise Control is deployed |
| 3. | Take a snapshot of broker pods | Snapshot of the current broker pods is taken |
| 4. | Remove Cruise Control from Kafka | Cruise Control is removed from Kafka and configuration is updated |
| 5. | Verify Cruise Control is removed | No Cruise Control related pods or configurations are found |
| 6. | Create Admin client to verify Cruise Control topics | Admin client is created and Cruise Control topics are verified to exist |
| 7. | Re-add Cruise Control to Kafka | Cruise Control is added back to Kafka |
| 8. | Verify Cruise Control and related configurations | Cruise Control and its configurations are verified to be present |

**Labels:**

* [cruise-control](labels/cruise-control.md)

