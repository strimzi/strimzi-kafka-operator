# DynamicConfST

**Description:** Responsible for verifying that changes in dynamic Kafka configuration do not trigger a rolling update.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the cluster operator. | Cluster operator is installed successfully. |

<hr style="border:1px solid">

## testClusterWideDynamicConfiguration

**Description:** Test for verifying dynamic configuration changes in a Kafka cluster with multiple clusters in one namespace.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deep copy shared Kafka configuration. | Configuration map is duplicated with deep copy. |
| 2. | Create resources with wait. | Resources are created and ready. |
| 3. | Create scraper pod. | Scraper pod is created. |
| 4. | Retrieve and verify Kafka configurations from ConfigMaps. | Configurations meet expected values. |
| 5. | Retrieve Kafka broker configuration via CLI. | Dynamic configurations are retrieved. |
| 6. | Upgrade eligible.leader.replicas.version feature to 1. | Feature is upgraded. |
| 7. | Update Kafka configuration for min.insync.replicas. | Configuration is updated and verified for dynamic property. |
| 8. | Verify updated Kafka configurations. | Updated configurations are persistent and correct. |

**Labels:**

* `dynamic-configuration` (description file doesn't exist)
* [kafka](labels/kafka.md)


## testSimpleDynamicConfiguration

**Description:** Test for verifying dynamic configuration changes in a Kafka cluster with multiple clusters in one namespace.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deep copy shared Kafka configuration. | Configuration map is duplicated with deep copy. |
| 2. | Create resources with wait. | Resources are created and ready. |
| 3. | Create scraper pod. | Scraper pod is created. |
| 4. | Retrieve and verify Kafka configurations from ConfigMaps. | Configurations meet expected values. |
| 5. | Retrieve Kafka broker configuration via CLI. | Dynamic configurations are retrieved. |
| 6. | Update Kafka configuration for unclean leader election. | Configuration is updated and verified for dynamic property. |
| 7. | Verify updated Kafka configurations. | Updated configurations are persistent and correct. |

**Labels:**

* `dynamic-configuration` (description file doesn't exist)
* [kafka](labels/kafka.md)


## testUpdateToExternalListenerCausesRollingRestart

**Description:** Ensures that updating to an external listener causes a rolling restart of the Kafka brokers.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka cluster with internal and external listeners. | Kafka cluster is created with the specified listeners. |
| 2. | Verify initial configurations are correctly set in the broker. | Initial broker configurations are verified. |
| 3. | Update Kafka cluster to change listener types. | Change in listener types triggers rolling update. |
| 4. | Verify the rolling restart is successful. | All broker nodes successfully rolled and Kafka configuration updated. |

**Labels:**

* `dynamic-configuration` (description file doesn't exist)
* [kafka](labels/kafka.md)


## testUpdateToExternalListenerCausesRollingRestartUsingExternalClients

**Description:** Test validating that updating Kafka cluster listeners to use external clients causes a rolling restart.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Setup initial Kafka cluster and resources. | Kafka cluster and resources are successfully created. |
| 2. | Create external Kafka clients and verify message production/consumption on plain listener. | Messages are successfully produced and consumed using plain listener. |
| 3. | Attempt to produce/consume messages using TLS listener before update. | Exception is thrown because the listener is plain. |
| 4. | Update Kafka cluster to use external TLS listener. | Kafka cluster is updated and rolling restart occurs. |
| 5. | Verify message production/consumption using TLS listener after update. | Messages are successfully produced and consumed using TLS listener. |
| 6. | Attempt to produce/consume messages using plain listener after TLS update. | Exception is thrown because the listener is TLS. |
| 7. | Revert Kafka cluster listener to plain. | Kafka cluster listener is reverted and rolling restart occurs. |
| 8. | Verify message production/consumption on plain listener after reverting. | Messages are successfully produced and consumed using plain listener. |
| 9. | Attempt to produce/consume messages using TLS listener after reverting. | Exception is thrown because the listener is plain. |

**Labels:**

* `dynamic-configuration` (description file doesn't exist)
* [kafka](labels/kafka.md)

