# Logging

## Description

These tests validate the logging configuration and behavior of Kafka and its components within the Strimzi ecosystem. 
Logging is essential for monitoring, debugging, and maintaining Kafka clusters and associated components like Kafka Connect, 
Kafka Bridge, and KafkaMirrorMaker2. 
These tests cover scenarios such as dynamic logging level changes, logging format adjustments (e.g., JSON format), 
external and inline logging configurations, and ensuring that logging changes do not cause unnecessary rolling updates. 
Proper logging facilitates operational visibility and aids in troubleshooting, making it crucial for the reliability 
and stability of Kafka deployments in production environments.

<!-- generated part -->
**Tests:**
- [testBridgeLogSetting](../io.strimzi.systemtest.log.LogSettingST.md)
- [testChangingInternalToExternalLoggingTriggerRollingUpdate](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testConnectLogSetting](../io.strimzi.systemtest.log.LogSettingST.md)
- [testCruiseControlLogChange](../io.strimzi.systemtest.log.LogSettingST.md)
- [testDynamicallyAndNonDynamicSetConnectLoggingLevels](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testDynamicallySetBridgeLoggingLevels](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testDynamicallySetClusterOperatorLoggingLevels](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testDynamicallySetEOloggingLevels](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testDynamicallySetKafkaExternalLogging](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testDynamicallySetKafkaLoggingLevels](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testDynamicallySetMM2LoggingLevels](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testDynamicallySetUnknownKafkaLogger](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testDynamicallySetUnknownKafkaLoggerValue](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testJSONFormatLogging](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testJsonTemplateLayoutFormatLogging](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testKafkaLogSetting](../io.strimzi.systemtest.log.LogSettingST.md)
- [testLoggingHierarchy](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testMM2LoggingLevelsHierarchy](../io.strimzi.systemtest.log.LoggingChangeST.md)
- [testMirrorMaker2LogSetting](../io.strimzi.systemtest.log.LogSettingST.md)
- [testNotExistingCMSetsDefaultLogging](../io.strimzi.systemtest.log.LoggingChangeST.md)
