# DynamicConfSharedST

**Description:** DynamicConfigurationSharedST is responsible for verifying that changing dynamic Kafka configuration will not trigger a rolling update. Shared -> for each test case we use the same Kafka resource configuration.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Run Cluster Operator installation. | Cluster Operator is installed. |
| 2. | Deploy shared Kafka across all test cases. | Shared Kafka is deployed. |
| 3. | Deploy scraper pod. | Scraper pod is deployed. |

**Labels:**

* `dynamic-configuration` (description file doesn't exist)
* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testDynamicConfiguration

**Description:** Parametrized test taking 3 per-broker and 3 cluster-wide configuration that are being tested if dynamic configuration works.For each of the configuration (and its value), it goes through following steps:
 1. Apply the configuration
 2. Wait for stability of the cluster - no Pods will be rolled.
 3. Verify that configuration is correctly set in CR and either all Pods or for whole cluster (based on scope).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Update configuration (with value) in Kafka. | Configuration is successfully updated. |
| 2. | For one minute, periodically check that there is no rolling update of Kafka Pods. | No Kafka Pods will be rolled. |
| 3. | Verify the applied configuration on both the Kafka CustomResource and the Kafka pods. | The applied configurations are correctly reflected in the Kafka CustomResource and the kafka pods. |

**Labels:**

* `dynamic-configuration` (description file doesn't exist)
* [kafka](labels/kafka.md)

