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
- [testKafkaConnectAndConnectorStateWithFileSinkPlugin](../io.strimzi.systemtest.connect.ConnectST.md)
- [testScaleConnectWithoutConnectorToZero](../io.strimzi.systemtest.connect.ConnectST.md)
- [testScaleConnectWithConnectorToZero](../io.strimzi.systemtest.connect.ConnectST.md)
- [testBuildWithJarTgzAndZip](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
- [testKafkaConnectAndConnectorFileSinkPlugin](../io.strimzi.systemtest.connect.ConnectST.md)
- [testKafkaConnectWithPlainAndScramShaAuthentication](../io.strimzi.systemtest.connect.ConnectST.md)
- [testKafkaConnectScaleUpScaleDown](../io.strimzi.systemtest.connect.ConnectST.md)
- [testDeployRollUndeploy](../io.strimzi.systemtest.connect.ConnectST.md)
- [testCustomAndUpdatedValues](../io.strimzi.systemtest.connect.ConnectST.md)
- [testSecretsWithKafkaConnectWithTlsAndTlsClientAuthentication](../io.strimzi.systemtest.connect.ConnectST.md)
- [testScaleConnectAndConnectorSubresource](../io.strimzi.systemtest.connect.ConnectST.md)
- [testMountingSecretAndConfigMapAsVolumesAndEnvVars](../io.strimzi.systemtest.connect.ConnectST.md)
- [testMultiNodeKafkaConnectWithConnectorCreation](../io.strimzi.systemtest.connect.ConnectST.md)
- [testConnectorTaskAutoRestart](../io.strimzi.systemtest.connect.ConnectST.md)
- [testKafkaConnectWithScramShaAuthenticationRolledAfterPasswordChanged](../io.strimzi.systemtest.connect.ConnectST.md)
- [testPushIntoImageStream](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
- [testJvmAndResources](../io.strimzi.systemtest.connect.ConnectST.md)
- [testConnectTlsAuthWithWeirdUserName](../io.strimzi.systemtest.connect.ConnectST.md)
- [testBuildOtherPluginTypeWithAndWithoutFileName](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
- [testSecretsWithKafkaConnectWithTlsAndScramShaAuthentication](../io.strimzi.systemtest.connect.ConnectST.md)
- [testConnectScramShaAuthWithWeirdUserName](../io.strimzi.systemtest.connect.ConnectST.md)
- [testBuildPluginUsingMavenCoordinatesArtifacts](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
- [testUpdateConnectWithAnotherPlugin](../io.strimzi.systemtest.connect.ConnectBuilderST.md)
