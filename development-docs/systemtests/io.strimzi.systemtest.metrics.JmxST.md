# JmxST

**Description:** This suite verifies JMX metrics behavior under various configurations and scenarios.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Install the Cluster Operator and ensure environment readiness. | Cluster Operator is deployed and environment is ready. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)

<hr style="border:1px solid">

## testKafkaAndKafkaConnectWithJMX

**Description:** Test verifying Kafka and KafkaConnect with JMX authentication enabled and correct version reporting.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster (ephemeral storage) with JMX authentication. | Kafka cluster is deployed with JMX enabled and authentication set. |
| 2. | Deploy a KafkaConnect cluster with JMX authentication. | KafkaConnect is deployed with JMX enabled and authentication set. |
| 3. | Create a Scraper Pod, install jmxterm, and collect JMX metrics from both Kafka and KafkaConnect. | JMX metrics are retrieved; Kafka version is reported correctly. |

**Labels:**

* [kafka](labels/kafka.md)
* [metrics](labels/metrics.md)

