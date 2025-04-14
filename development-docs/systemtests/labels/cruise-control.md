# **Cruise Control**

## Description

These tests validate the Cruise Control component within the Strimzi ecosystem, ensuring efficient resource utilization and cluster balancing for Kafka clusters. 
Cruise Control provides automated workload balancing and optimization, enabling better performance and resilience in Kafka deployments. 
These tests cover scenarios such as configuration changes, resource scaling, security settings, and integration with Kafka Rebalance operations. 
Ensuring the correctness of Cruise Control behavior under different configurations and workloads is critical for maintaining optimal cluster performance and reliability.

<!-- generated part -->
**Tests:**
- [testAutoCreationOfCruiseControlTopicsWithResources](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testAutoKafkaRebalanceScaleUpScaleDown](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testConfigurationUpdate](../io.strimzi.systemtest.cruisecontrol.CruiseControlConfigurationST.md)
- [testCruiseControlAPIUsers](../io.strimzi.systemtest.cruisecontrol.CruiseControlApiST.md)
- [testCruiseControlBasicAPIRequestsWithSecurityDisabled](../io.strimzi.systemtest.cruisecontrol.CruiseControlApiST.md)
- [testCruiseControlChangesFromRebalancingtoProposalReadyWhenSpecUpdated](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testCruiseControlDuringBrokerScaleUpAndDown](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testCruiseControlIntraBrokerBalancing](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testCruiseControlLogChange](../io.strimzi.systemtest.log.LogSettingST.md)
- [testCruiseControlMetrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testCruiseControlRemoveDisksMode](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testCruiseControlReplicaMovementStrategy](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testCruiseControlTopicExclusion](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testCruiseControlWithRebalanceResourceAndRefreshAnnotation](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testCruiseControlWithSingleNodeKafka](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
- [testDeployAndUnDeployCruiseControl](../io.strimzi.systemtest.cruisecontrol.CruiseControlConfigurationST.md)
- [testKafkaRebalanceAutoApprovalMechanism](../io.strimzi.systemtest.cruisecontrol.CruiseControlST.md)
