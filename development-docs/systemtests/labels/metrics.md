# **Metrics**

## Description

These tests ensure that all metrics exposed by Strimzi components (including Kafka, Kafka Connect, Kafka Bridge, Kafka MirrorMaker2, and the Cluster Operator) adhere to expected behaviors and formats. 
They validate data correctness, confirm that metrics are exposed under various configurations, and ensure integration with external monitoring systems. 
Proper verification of metrics is crucial for observability, debugging, and performance tuning in production environments.


<!-- generated part -->
**Tests:**
- [testKafkaExporterDifferentSetting](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testKafkaExporterMetrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testKafkaMetrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testKafkaConnectAndConnectorMetrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testKafkaAndKafkaConnectWithJMX](../io.strimzi.systemtest.metrics.JmxST.md)
- [testKafkaBridgeMetrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testCruiseControlMetrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testClusterOperatorMetrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testUserOperatorMetrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testKafkaMetricsSettings](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testMirrorMaker2Metrics](../io.strimzi.systemtest.metrics.MetricsST.md)
