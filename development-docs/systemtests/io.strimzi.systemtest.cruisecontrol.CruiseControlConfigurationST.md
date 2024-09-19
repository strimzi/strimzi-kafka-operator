# CruiseControlConfigurationST

**Description:** This test suite, verify configuration of the CruiseControl component.

**Before tests execution steps:**

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
| 1. | Initialize the test storage. | Test storage object is created. |
| 2. | Create and wait for resources with node pools. | Resources are created successfully. |
| 3. | Create and wait for Kafka with Cruise Control. | Kafka and Cruise Control are deployed successfully. |
| 4. | Take initial snapshots of Kafka and Cruise Control deployments. | Snapshots of current deployments are stored. |
| 5. | Update Cruise Control configuration with new performance tuning options. | Configuration update initiated. |
| 6. | Verify Cruise Control Pod rolls after configuration change. | Cruise Control Pod restarts to apply new configurations. |
| 7. | Verify Kafka Pods did not roll after configuration change. | Kafka Pods remain unchanged. |
| 8. | Verify new configurations are applied to Cruise Control in Kafka CR. | New configurations are correctly applied. |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testDeployAndUnDeployCruiseControl

**Description:** Deploy and subsequently remove CruiseControl from Kafka cluster to verify system stability and correctness of configuration management.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the test storage. | Test storage object is created. |
| 2. | Create broker and controller node pools | Node pools are created and configured |
| 3. | Deploy Kafka with CruiseControl | Kafka cluster with CruiseControl is deployed |
| 4. | Take a snapshot of broker pods | Snapshot of the current broker pods is taken |
| 5. | Remove CruiseControl from Kafka | CruiseControl is removed from Kafka and configuration is updated |
| 6. | Verify CruiseControl is removed | No CruiseControl related pods or configurations are found |
| 7. | Create Admin client to verify CruiseControl topics | Admin client is created and CruiseControl topics are verified to exist |
| 8. | Re-add CruiseControl to Kafka | CruiseControl is added back to Kafka |
| 9. | Verify CruiseControl and related configurations | CruiseControl and its configurations are verified to be present |

**Labels:**

* [cruise-control](labels/cruise-control.md)

