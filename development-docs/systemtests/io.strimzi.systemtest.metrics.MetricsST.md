# MetricsST

**Description:** This test suite is designed for testing metrics exposed by operators and operands.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespaces {@namespaceFirst} and {@namespaceSecond}. | Namespaces {@namespaceFirst} and {@namespaceSecond} are created. |
| 2. | Deploy Cluster Operator. | Cluster Operator is deployed. |
| 3. | Deploy Kafka {@kafkaClusterFirstName} with metrics and CruiseControl configured. | Kafka @{kafkaClusterFirstName} is deployed. |
| 4. | Deploy Kafka {@kafkaClusterSecondtName} with metrics configured. | Kafka @{kafkaClusterFirstName} is deployed. |
| 5. | Deploy scraper Pods in namespace {@namespaceFirst} and {@namespaceSecond} for collecting metrics from Strimzi pods. | Scraper Pods are deployed. |
| 6. | Create KafkaUsers and KafkaTopics. | All KafkaUsers and KafkaTopics are Ready. |
| 7. | Setup NetworkPolicies to grant access to Operator Pods and KafkaExporter. | NetworkPolicies created. |
| 8. | Create collectors for Cluster Operator, Kafka, and KafkaExporter. | Metrics collected in collectors structs. |

**After tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Common cleaning of all resources created by this test class. | All resources deleted. |

**Labels:**

* [metrics](labels/metrics.md)

<hr style="border:1px solid">

## testClusterOperatorMetrics

**Description:** This test case checks several random metrics exposed by Cluster Operator.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Check that specific metrics for Kafka reconciliation are available in metrics from Cluster Operator pod. | Metric is available with expected value. |
| 2. | Check that collected metrics contain data about Kafka resource. | Metric is available with expected value. |
| 3. | Check that collected metrics don't contain data about KafkaRebalance resource. | Metric is not exposed. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)


## testCruiseControlMetrics

**Description:** This test case checks several random metrics exposed by CruiseControl.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Check if specific metric is available in collected metrics from CruiseControl pods | Metric is available with expected value |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)
* [cruise-control](labels/cruise-control.md)


## testKafkaBridgeMetrics

**Description:** This test case checks several metrics exposed by KafkaBridge.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaBridge into namespaceFirst and ensure KafkaMirrorMaker2 is in Ready state | KafkaBridge is deployed and KafkaMirrorMaker2 is Ready |
| 2. | Attach producer and consumer clients to KafkaBridge | Clients are up and running |
| 3. | Collect metrics from KafkaBridge pod | Metrics are collected |
| 4. | Check that specific metric is available in collected metrics from KafkaBridge pods | Metric is available with expected value |
| 5. | Collect current metrics from Cluster Operator pod | Cluster Operator metrics are collected |
| 6. | Check that CO metrics contain data about KafkaBridge in namespace namespaceFirst | CO metrics contain expected data |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)
* [bridge](labels/bridge.md)


## testKafkaConnectAndConnectorMetrics

**Description:** This test case checks several random metrics exposed by Kafka Connect.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaConnect into {@namespaceFirst} with {@Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES} set to true. | KafkaConnect is up and running. |
| 2. | Create KafkaConnector for KafkaConnect from step 1. | KafkaConnector is in Ready state. |
| 3. | Create metrics collector and collect metrics from KafkaConnect Pods. | Metrics are collected. |
| 4. | Check if specific metric is available in collected metrics from KafkaConnect Pods. | Metric is available with expected value. |
| 5. | Collect current metrics from Cluster Operator Pod. | Cluster Operator metrics are collected. |
| 6. | Check that CO metrics contain data about KafkaConnect and KafkaConnector in namespace {@namespaceFirst}. | CO metrics contain expected data. |
| 7. | Check that CO metrics don't contain data about KafkaConnect and KafkaConnector in namespace {@namespaceFirst}. | CO metrics don't contain expected data. |
| 8. | Check that CO metrics contain data about KafkaConnect state. | CO metrics contain expected data. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)
* [connect](labels/connect.md)


## testKafkaExporterDifferentSetting

**Description:** This test case checks several metrics exposed by KafkaExporter with different from default configuration. Rolling update is performed during the test case to change KafkaExporter configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Get KafkaExporter run.sh script and check it has configured proper values. | Script has proper values set. |
| 2. | Check that KafkaExporter metrics contains info about consumer_offset topic. | Metrics contains proper data. |
| 3. | Change configuration of KafkaExporter in Kafka CR to match 'my-group.*' group regex and {@topicName} as topic name regex, than wait for KafkaExporter rolling update. | Rolling update finished. |
| 4. | Get KafkaExporter run.sh script and check it has configured proper values. | Script has proper values set. |
| 5. | Check that KafkaExporter metrics don't contain info about consumer_offset topic. | Metrics contains proper data (consumer_offset is not in the metrics). |
| 6. | Revert all changes in KafkaExporter configuration and wait for Rolling Update. | Rolling update finished. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)


## testKafkaExporterMetrics

**Description:** This test case checks several metrics exposed by KafkaExporter.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka producer and consumer and exchange some messages. | Clients successfully exchange the messages. |
| 2. | Check if metric kafka_topic_partitions is available in collected metrics from KafkaExporter Pods. | Metric is available with expected value. |
| 3. | Check if metric kafka_broker_info is available in collected metrics from KafkaExporter pods for each Kafka Broker pod. | Metric is available with expected value. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)


## testKafkaMetrics

**Description:** This test case checks several random metrics exposed by Kafka.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Check if specific metric is available in collected metrics from Kafka Pods. | Metric is available with expected value. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)


## testKafkaMetricsSettings

**Description:** This test case checks that the Cluster Operator propagates changes from metrics configuration done in Kafka CR into corresponding config maps.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create config map with external metrics configuration. | Config map created. |
| 2. | Set ConfigMap reference from step 1 into Kafka CR and wait for pod stabilization (CO shouldn't trigger rolling update). | Wait for Kafka pods stability (60 seconds without rolling update in the row). |
| 3. | Check that metrics config maps for each pod contains data from external metrics config map. | All config maps contains proper values. |
| 4. | Change config in external metrics config map. | Config map changed. |
| 5. | Wait for Kafka pods stabilization (CO shouldn't trigger rolling update). | Wait for Kafka pods stability (60 seconds without rolling update in the row). |
| 6. | Check that metrics config maps for each pod contains data from external metrics config map. | All config maps contains proper values. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)


## testMirrorMaker2Metrics

**Description:** This test case checks several metrics exposed by KafkaMirrorMaker2.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaMirrorMaker2 into {@namespaceFirst}. | KafkaMirrorMaker2 is in Ready state. |
| 2. | Collect metrics from KafkaMirrorMaker2 pod. | Metrics are collected. |
| 3. | Check if specific metric is available in collected metrics from KafkaMirrorMaker2 pods. | Metric is available with expected value. |
| 4. | Collect current metrics from Cluster Operator pod. | Cluster Operator metrics are collected. |
| 5. | Check that CO metrics contain data about KafkaMirrorMaker2 in namespace {@namespaceFirst}. | CO metrics contain expected data. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)
* `mirror-maker` (description file doesn't exist)


## testUserOperatorMetrics

**Description:** This test case checks several metrics exposed by User Operator.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Collect metrics from User Operator pod. | Metrics are collected. |
| 2. | Check that specific metrics about KafkaUser are available in collected metrics. | Metric is available with expected value. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)

