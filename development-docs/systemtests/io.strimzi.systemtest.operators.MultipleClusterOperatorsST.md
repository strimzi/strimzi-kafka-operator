# MultipleClusterOperatorsST

**Description:** Test suite for verifying multiple Cluster Operator deployment scenarios, including resource selectors, leader election, and metrics collection across different namespace configurations.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Skip this test suite if using Helm or OLM installation. | The test suite only runs with YAML-based installations. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testMultipleCOsInDifferentNamespaces

**Description:** This test verifies how two Cluster Operators operate resources deployed in namespaces watched by both operators, and how operands can be transitioned from one Cluster Operator to another using label selectors.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy two Cluster Operators in separate namespaces, both watching all namespaces. | Both Cluster Operators are successfully deployed. |
| 2. | Set up scrapers and metric collectors for both Cluster Operators. | Scraper Pods and metrics collectors are configured and ready. |
| 3. | Create namespace for test resources. | Namespace `multiple-co-cluster-test` is created and set as default. |
| 4. | Deploy Kafka without operator selector label. | The `Kafka` resource is created but no pods are deployed because it is not managed by any operator. |
| 5. | Verify Kafka metrics are absent in both operators. | Metric `strimzi_resource` for Kafka is null or zero in both Cluster Operators. |
| 6. | Add label selector pointing to first Cluster Operator. | Kafka is deployed and managed by the first Cluster Operator. |
| 7. | Deploy `KafkaConnect` and `KafkaConnector` with a label selecting the first Cluster Operator. | Both resources are successfully deployed and managed by the first Cluster Operator. |
| 8. | Produce and consume messages using Sink Connector. | Messages are produced to topic and consumed by the Connector successfully. |
| 9. | Switch Kafka management to the second Cluster Operator by changing the label. | Kafka management transfers to second operator, as confirmed by updated metrics. |
| 10. | Verify metrics for all operands on both operators. | Metric `strimzi_resource` shows correct counts for each Cluster Operator. |

**Labels:**

* [kafka](labels/kafka.md)
* [connect](labels/connect.md)
* [metrics](labels/metrics.md)

