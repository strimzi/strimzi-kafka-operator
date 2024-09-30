# DynamicConfSharedST

**Description:** DynamicConfigurationSharedST is responsible for verifying that changing dynamic Kafka configuration will not trigger a rolling update. Shared -> for each test case we use the same Kafka resource configuration.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Run cluster operator installation | Cluster operator is installed |
| 2. | Deploy shared Kafka across all test cases | Shared Kafka is deployed |
| 3. | Deploy scraper pod | Scraper pod is deployed |

**Labels:**

* `dynamic-configuration` (description file doesn't exist)
* [kafka](labels/kafka.md)

<hr style="border:1px solid">
