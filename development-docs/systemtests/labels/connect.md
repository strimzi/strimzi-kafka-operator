# **Connect**

## Description

These tests validate the Kafka Connect component, ensuring reliable integration between Kafka and external systems through connectors. 
Kafka Connect enables data streaming between Kafka clusters and various data sources or sinks. 
These tests cover scenarios like plugin management, build processes, network configurations, and various security protocols. 
Ensuring the correctness of Kafka Connect behavior under different configurations and scaling scenarios is critical to 
maintaining data consistency and availability in a streaming ecosystem.

<!-- generated part -->
**Tests:**
- [testBuildFailsWithWrongChecksumOfArtifact](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
- [testBuildOtherPluginTypeWithAndWithoutFileName](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
- [testBuildPluginUsingMavenCoordinatesArtifacts](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
- [testBuildWithJarTgzAndZip](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
- [testConnectLogSetting](../io.strimzi.systemtest.log.LogSettingST.md)
- [testConnectScramShaAuthWithWeirdUserName](../io.strimzi.systemtest.connect.ConnectST.md)
- [testConnectTlsAuthWithWeirdUserName](../io.strimzi.systemtest.connect.ConnectST.md)
- [testConnectorOffsetManagement](../io.strimzi.systemtest.connect.ConnectST.md)
- [testConnectorTaskAutoRestart](../io.strimzi.systemtest.connect.ConnectST.md)
- [testCustomAndUpdatedValues](../io.strimzi.systemtest.connect.ConnectST.md)
- [testDeployExampleKafkaConnect](../io.strimzi.systemtest.olm.OlmSingleNamespaceST.md)
- [testDeployRollUndeploy](../io.strimzi.systemtest.connect.ConnectST.md)
- [testJvmAndResources](../io.strimzi.systemtest.connect.ConnectST.md)
- [testKafkaConnectAndConnectorFileSinkPlugin](../io.strimzi.systemtest.connect.ConnectST.md)
- [testKafkaConnectAndConnectorMetrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testKafkaConnectAndConnectorStateWithFileSinkPlugin](../io.strimzi.systemtest.connect.ConnectST.md)
- [testKafkaConnectScaleUpScaleDown](../io.strimzi.systemtest.connect.ConnectST.md)
- [testKafkaConnectWithPlainAndScramShaAuthentication](../io.strimzi.systemtest.connect.ConnectST.md)
- [testKafkaConnectWithScramShaAuthenticationRolledAfterPasswordChanged](../io.strimzi.systemtest.connect.ConnectST.md)
- [testMountingSecretAndConfigMapAsVolumesAndEnvVars](../io.strimzi.systemtest.connect.ConnectST.md)
- [testMultiNodeKafkaConnectWithConnectorCreation](../io.strimzi.systemtest.connect.ConnectST.md)
- [testPushIntoImageStream](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
- [testScaleConnectAndConnectorSubresource](../io.strimzi.systemtest.connect.ConnectST.md)
- [testScaleConnectWithConnectorToZero](../io.strimzi.systemtest.connect.ConnectST.md)
- [testScaleConnectWithoutConnectorToZero](../io.strimzi.systemtest.connect.ConnectST.md)
- [testSecretsWithKafkaConnectWithTlsAndScramShaAuthentication](../io.strimzi.systemtest.connect.ConnectST.md)
- [testSecretsWithKafkaConnectWithTlsAndTlsClientAuthentication](../io.strimzi.systemtest.connect.ConnectST.md)
- [testUpdateConnectWithAnotherPlugin](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
