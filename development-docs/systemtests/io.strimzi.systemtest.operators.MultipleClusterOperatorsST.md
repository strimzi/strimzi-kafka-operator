# MultipleClusterOperatorsST

**Description:** Test suite for verifying multiple Cluster Operators deployment scenarios, including Cluster Operator resource selectors, leader election, and metrics collection across different namespace configurations.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Skip test suite if using Helm or OLM installation. | Test suite only runs with YAML-based installations. |

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
| 3. | Create namespace for test resources. | Namespace 'multiple-co-cluster-test' is created and set as default. |
| 4. | Deploy Kafka without operator selector label. | Kafka is created but no Pods appear as no operator manages it. |
| 5. | Verify Kafka metrics are absent in both operators. | Metric 'strimzi_resource' for Kafka is null or zero in both Cluster Operators. |
| 6. | Add label selector pointing to first Cluster Operator. | Kafka is deployed and managed by the first Cluster Operator. |
| 7. | Deploy KafkaConnect and KafkaConnector with first operator label. | Both resources are successfully deployed and managed by the first Cluster Operator. |
| 8. | Produce and consume messages using Sink Connector. | Messages are produced to topic and consumed by the Connector successfully. |
| 9. | Switch Kafka management to second Cluster Operator by changing label. | Kafka management transfers to second operator, confirmed by metrics change. |
| 10. | Verify metrics for all operands on both operators. | Metric 'strimzi_resource' shows correct counts for each Cluster Operator. |

**Labels:**

* [kafka](labels/kafka.md)
* [connect](labels/connect.md)

