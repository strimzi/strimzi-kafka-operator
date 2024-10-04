# DynamicConfSharedST

**Description:** DynamicConfigurationSharedST is responsible for verifying that changing dynamic Kafka configuration will not trigger a rolling update. Shared -> for each test case we use the same Kafka resource configuration.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Run cluster operator installation. | Cluster operator is installed. |
| 2. | Deploy shared Kafka across all test cases. | Shared Kafka is deployed. |
| 3. | Deploy scraper pod. | Scraper pod is deployed. |

**Labels:**

* `dynamic-configuration` (description file doesn't exist)
* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testDynConfiguration

**Description:** This test dynamically selects and applies three Kafka dynamic configuration properties to verify that the changes do not trigger a rolling update in the Kafka cluster. It applies the configurations, waits for stability, and then verifies that the new configuration is applied both to the CustomResource (CR) and the running Kafka pods.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Randomly choose three configuration properties for dynamic update. | Three configurations are selected without duplication. |
| 2. | Apply the chosen configuration properties to the Kafka CustomResource. | The configurations are applied successfully without triggering a rolling update. |
| 3. | Verify the applied configuration on both the Kafka CustomResource and the Kafka pods. | The applied configurations are correctly reflected in the Kafka CustomResource and the kafka pods. |

**Labels:**

* `dynamic-configuration` (description file doesn't exist)
* [kafka](labels/kafka.md)

