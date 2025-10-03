# StrimziMetricsReporterST

**Description:** This test suite is designed for testing metrics exposed by the Strimzi Metrics Reporter.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace {@namespace}. | Namespace {@namespace} is created. |
| 2. | Deploy Cluster Operator. | Cluster Operator is deployed. |
| 3. | Deploy Kafka {@clusterName} with Strimzi Metrics Reporter. | Kafka @{clusterName} is deployed. |
| 4. | Deploy scraper Pod in namespace {@namespace} for collecting metrics from Strimzi pods. | Scraper Pod is deployed. |
| 5. | Create KafkaTopic resource. | KafkaTopic resource is Ready. |
| 6. | Create collector for Kafka. | Metrics collected in collectors structs. |

**After test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Common cleaning of all resources created by this test class. | All resources deleted. |

**Labels:**

* [metrics](labels/metrics.md)

<hr style="border:1px solid">

## testKafkaBridgeMetrics

**Description:** This test case checks several metrics exposed by KafkaBridge.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaBridge into {@namespace}. | KafkaBridge is deployed and Ready |
| 2. | Attach producer and consumer clients to KafkaBridge | Clients are up and running, continuously producing and pooling messages |
| 3. | Collect metrics from KafkaBridge pod | Metrics are collected |
| 4. | Check that specific metric is available in collected metrics from KafkaBridge pods | Metric is available with expected value |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)
* [bridge](labels/bridge.md)


## testKafkaConnectAndConnectorMetrics

**Description:** This test case checks several random metrics exposed by Kafka Connect.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaConnect into {@namespace}. | KafkaConnect is up and running. |
| 2. | Create KafkaConnector for KafkaConnect from step 1. | KafkaConnector is in Ready state. |
| 3. | Create metrics collector and collect metrics from KafkaConnect Pods. | Metrics are collected. |
| 4. | Check if specific metric is available in collected metrics from KafkaConnect Pods. | Metric is available with expected value. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)
* [connect](labels/connect.md)


## testKafkaMetrics

**Description:** This test case checks several metrics exposed by Kafka.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Check if specific metrics are available in collected metrics from Kafka Pods. | Metrics are available with expected values. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)


## testMirrorMaker2Metrics

**Description:** This test case checks several metrics exposed by KafkaMirrorMaker2.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaMirrorMaker2 into {@namespace}. | KafkaMirrorMaker2 is in Ready state. |
| 2. | Collect metrics from KafkaMirrorMaker2 pod. | Metrics are collected. |
| 3. | Check if specific metric is available in collected metrics from KafkaMirrorMaker2 pods. | Metric is available with expected value. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)
* `mirror-maker-2` (description file doesn't exist)

