# StrimziMetricsReporterST

**Description:** This test suite is designed for testing metrics exposed by the Strimzi Metrics Reporter.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace {@namespace}. | Namespace {@namespace} is created. |
| 2. | Deploy Cluster Operator. | Cluster Operator is deployed. |
| 3. | Deploy Kafka {@clusterName} with Strimzi Metrics Reporter. | Kafka @{clusterName} is deployed. |
| 4. | Deploy scraper Pod in namespace {@namespace} for collecting metrics from Strimzi pods. | Scraper Pods is deployed. |
| 5. | Create KafkaTopic resource. | KafkaTopic resource is Ready. |
| 6. | Create collector for Kafka. | Metrics collected in collectors structs. |

**After tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Common cleaning of all resources created by this test class. | All resources deleted. |

**Labels:**

* [metrics](labels/metrics.md)

<hr style="border:1px solid">

## testKafkaMetrics

**Description:** This test case checks several metrics exposed by Kafka.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Check if specific metrics are available in collected metrics from Kafka Pods. | Metrics are available with expected values. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)

