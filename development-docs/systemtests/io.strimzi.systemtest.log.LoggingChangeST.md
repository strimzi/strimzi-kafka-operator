# LoggingChangeST

**Description:** This suite verifies logging behavior under various configurations and scenarios.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the cluster operator. | Cluster operator is deployed successfully. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)

<hr style="border:1px solid">

## testChangingInternalToExternalLoggingTriggerRollingUpdate

**Description:** This test case checks that changing Logging configuration from internal to external triggers Rolling Update.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster, without any logging related configuration. | Cluster is deployed. |
| 2. | Modify Kafka by changing specification of logging to new external value. | Change in logging specification triggers Rolling Update. |
| 3. | Modify ConfigMap to new logging format. | Change in logging specification triggers Rolling Update. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testDynamicallyAndNonDynamicSetConnectLoggingLevels

**Description:** This test dynamically changes the logging levels of the Kafka Bridge and verifies the application behaves correctly with respect to these changes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster and KafkaConnect cluster, the latter with Log level Off. | Clusters are deployed with KafkaConnect log level set to Off. |
| 2. | Deploy all additional resources, Scraper Pod and network policies (in order to gather the stuff from the Scraper Pod). | Additional resources, Scraper Pod and network policies are deployed. |
| 3. | Verify that no logs are present in KafkaConnect Pods. | KafkaConnect Pods contain no logs. |
| 4. | Set inline log level to Debug in KafkaConnect CustomResource. | Log level is set to Debug, and pods provide logs on Debug level. |
| 5. | Change inline log level from Debug to Info in KafkaConnect CustomResource. | Log level is changed to Info, and pods provide logs on Info level. |
| 6. | Create ConfigMap with necessary data for external logging and modify KafkaConnect CustomResource to use external logging setting log level Off. | Log level is set to Off using external logging, and pods provide no logs. |
| 7. | Disable the use of connector resources via annotations and verify KafkaConnect pod rolls. | KafkaConnect deployment rolls and pod changes are verified. |
| 8. | Enable DEBUG logging via inline configuration. | Log lines contain DEBUG level messages after configuration changes. |
| 9. | Check the propagation of logging level change to DEBUG under the condition where connector resources are not used. | DEBUG logs are present and correct according to the log4j pattern. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testDynamicallySetBridgeLoggingLevels

**Description:** This test dynamically changes the logging levels of the Kafka Bridge and verifies that it behaves correctly in response to these changes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Configure the initial logging levels of the Kafka Bridge to OFF using inline logging. | Kafka Bridge logging levels are set to OFF. |
| 2. | Asynchronously deploy the Kafka Bridge with the initial logging configuration, along with the required Kafka cluster and Scraper Pod. | Kafka Bridge and all required resources become ready. |
| 3. | Verify that the Kafka Bridge logs are empty due to logging level being OFF. | Kafka Bridge logs are confirmed to be empty. |
| 4. | Change the Kafka Bridge's rootLogger level to DEBUG using inline logging and apply the changes. | Kafka Bridge logging level is updated to DEBUG and reflected in its log4j2.properties. |
| 5. | Verify that the Kafka Bridge logs now contain records at the DEBUG level. | Kafka Bridge logs contain expected DEBUG level records. |
| 6. | Switch the Kafka Bridge to use an external logging configuration from a ConfigMap with rootLogger level set to OFF. | Kafka Bridge logging levels are updated to OFF using the external ConfigMap. |
| 7. | Verify that the Kafka Bridge logs are empty again after applying the external logging configuration. | Kafka Bridge logs are confirmed to be empty. |
| 8. | Ensure that the Kafka Bridge Pod did not restart or roll during the logging level changes. | Kafka Bridge Pod maintains its original state without restarting. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testDynamicallySetClusterOperatorLoggingLevels

**Description:** Tests dynamic reconfiguration of logging levels for the Cluster Operator and ensures changes are applied correctly.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Capture current Cluster Operator pod snapshot. | Snapshot of the current Cluster Operator pod is captured. |
| 2. | Verify initial logging configuration differs from new setting. | Initial log configuration is not equal to the new configuration. |
| 3. | Update ConfigMap with the new logging settings. | ConfigMap is updated with the new log settings. |
| 4. | Wait for the Cluster Operator to apply the new logging settings. | Log configuration is updated in the Cluster Operator pod. |
| 5. | Verify the Cluster Operator pod is rolled to apply new settings. | Cluster Operator pod is rolled successfully with the new config. |
| 6. | Change logging levels from OFF to INFO. | Log levels in log4j2.properties are updated to INFO. |
| 7. | Verify new INFO settings are applied correctly. | Updated log level is applied and visible in logs. |
| 8. | Check for Cluster Operator pod roll after level update. | Cluster Operator pod roll is verified after changing log levels. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testDynamicallySetEOloggingLevels

**Description:** Test verifying the dynamic update of logging levels for Entity Operator components.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with Inline logging set to OFF. | Kafka deployed with no logs from Entity Operator. |
| 2. | Set logging level to DEBUG using Inline logging. | Logs appear for Entity Operator with level DEBUG. |
| 3. | Switch to external logging configuration with level OFF. | Logs from Entity Operator components are suppressed. |
| 4. | Update external logging configuration to level DEBUG. | Logs appear again with level DEBUG. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testDynamicallySetKafkaExternalLogging

**Description:** Test dynamic updates to Kafka's external logging configuration to ensure that logging updates are applied correctly, with or without triggering a rolling update based on the nature of the changes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a ConfigMap 'external-cm' with initial logging settings for Kafka. | ConfigMap 'external-cm' is created with initial logging settings. |
| 2. | Deploy a Kafka cluster configured to use the external logging ConfigMap, along with a Scraper Pod. | Kafka cluster and Scraper Pod are successfully deployed and become ready. |
| 3. | Update the ConfigMap 'external-cm' to change logger levels that can be updated dynamically. | ConfigMap is updated, and dynamic logging changes are applied without triggering a rolling update. |
| 4. | Verify that no rolling update occurs and that the new logger levels are active in the Kafka brokers. | No rolling update occurs; Kafka brokers reflect the new logger levels. |
| 5. | Update the ConfigMap 'external-cm' to change logger settings that require a rolling update. | ConfigMap is updated with changes that cannot be applied dynamically. |
| 6. | Verify that a rolling update occurs and that the new logging settings are applied after the restart. | Rolling update completes successfully; Kafka brokers are updated with the new logging settings. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testDynamicallySetKafkaLoggingLevels

**Description:** Test dynamically changing Kafka logging levels using inline and external logging.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set logging levels to OFF using inline logging. | All logging levels in Kafka are set to OFF. |
| 2. | Deploy Kafka cluster with the logging configuration. | Kafka cluster is deployed successfully with logging set to OFF. |
| 3. | Verify that the logs are not empty and do not match default log pattern. | Logs are verified as expected. |
| 4. | Change rootLogger level to DEBUG using inline logging. | Root logger level is changed to DEBUG dynamically. |
| 5. | Verify log change in Kafka Pod. | Logs reflect DEBUG level setting. |
| 6. | Set external logging configuration with INFO level. | External logging is configured with rootLogger set to INFO. |
| 7. | Verify log change with external logging. | Logs reflect INFO level setting through external logging. |
| 8. | Assert no rolling update of Kafka Pods occurred. | Pods did not roll due to logging changes. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testDynamicallySetMM2LoggingLevels

**Description:** Test to dynamically set logging levels for MirrorMaker2 and verify the changes are applied without pod rolling.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set initial logging level to OFF and deploy resources. | Resources are deployed |
| 2. | Verify the log is empty with level OFF. | Log is confirmed empty. |
| 3. | Change logging level to DEBUG dynamically. | Logging level changes to DEBUG without pod roll. |
| 4. | Verify the log is not empty with DEBUG level. | Log contains entries with DEBUG level. |
| 5. | Create external ConfigMap for logging configuration. | ConfigMap created with log4j settings. |
| 6. | Apply external logging configuration with level OFF. | External logging configuration applied. |
| 7. | Verify changes to OFF level are applied and no pod roll occurs. | Logging changes to OFF correctly without rolling the pod. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testDynamicallySetUnknownKafkaLogger

**Description:** Test dynamically setting an unknown Kafka logger in the resource.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve the name of a broker pod and Scraper Pod. | Pod names are retrieved successfully. |
| 2. | Take a snapshot of broker pod states. | Broker pod states are captured. |
| 3. | Set new logger configuration with InlineLogging. | Logger is configured to 'log4j.logger.paprika'='INFO'. |
| 4. | Wait for rolling update of broker components to finish. | Brokers are updated and rolled successfully. |
| 5. | Verify that logger configuration has changed. | Logger 'paprika=INFO' is configured correctly. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testDynamicallySetUnknownKafkaLoggerValue

**Description:** Test the dynamic setting of an unknown Kafka logger value without triggering a rolling update.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Take a snapshot of current broker pods. | Snapshot of broker pods is captured successfully. |
| 2. | Set Kafka root logger level to an unknown value 'PAPRIKA'. | Logger level is updated in the Kafka resource spec. |
| 3. | Wait to ensure no rolling update is triggered for broker pods. | Kafka brokers do not roll. |
| 4. | Assert that Kafka Pod should not roll. | Assertion confirms no rolling update was triggered. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testJSONFormatLogging

**Description:** Test verifying that the logging in JSON format works correctly across Kafka and operators.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Assume non-Helm and non-OLM installation. | Assumption holds true. |
| 2. | Create ConfigMaps for Kafka and operators with JSON logging configuration. | ConfigMaps created and applied. |
| 3. | Deploy Kafka cluster with the configured logging setup. | Kafka cluster deployed successfully. |
| 4. | Perform pod snapshot for controllers, brokers, and entity operators. | Pod snapshots successfully captured. |
| 5. | Verify logs are in JSON format for all components. | Logs are in JSON format. |
| 6. | Restore original logging configuration. | Original logging is restored. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testJsonTemplateLayoutFormatLogging

**Description:** Test verifying that the logging in JSON format works correctly across Kafka and operators using the JsonTemplateLayout.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Assume non-Helm and non-OLM installation. | Assumption holds true. |
| 2. | Create ConfigMaps for Kafka and operators with JSON logging configuration. | ConfigMaps created and applied. |
| 3. | Deploy Kafka cluster with the configured logging setup. | Kafka cluster deployed successfully. |
| 4. | Perform pod snapshot for controllers, brokers, and entity operators. | Pod snapshots successfully captured. |
| 5. | Verify logs are in JSON format for all components. | Logs are in JSON format. |
| 6. | Restore original logging configuration. | Original logging is restored. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testLoggingHierarchy

**Description:** Test to validate the logging hierarchy in Kafka Connect and Kafka Connector.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create an ephemeral Kafka cluster. | Ephemeral Kafka cluster is deployed successfully. |
| 2. | Deploy KafkaConnect with File Plugin and configure loggers. | KafkaConnect is configured and deployed with inline logging set to ERROR for FileStreamSourceConnector. |
| 3. | Apply network policies for KafkaConnect. | Network policies are successfully applied. |
| 4. | Change rootLogger level to ERROR for KafkaConnector. | Logging for KafkaConnector is updated to ERROR. |
| 5. | Restart KafkaConnector and verify its status. | KafkaConnector is restarted and running. |
| 6. | Validate that the logger level change is retained. | Logger level is consistent with ERROR. |
| 7. | Update KafkaConnect's root logger to WARN and check if KafkaConnector inherits it. | KafkaConnector logging remains independent of KafkaConnect root logger. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testMM2LoggingLevelsHierarchy

**Description:** Test to ensure logging levels in Kafka MirrorMaker2 follow a specific hierarchy.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set up source and target Kafka cluster. | Kafka clusters are deployed and ready. |
| 2. | Configure Kafka MirrorMaker2 with initial logging level set to `OFF`. | Logging configuration is applied with log level 'OFF'. |
| 3. | Verify initial logging levels in Kafka MirrorMaker2 pod. | Log level 'OFF' is confirmed. |
| 4. | Update logging configuration to change log levels. | New logging configuration is applied. |
| 5. | Verify updated logging levels in Kafka MirrorMaker2 pod. | Log levels 'INFO' and 'WARN' are correctly set as per hierarchy. |
| 6. | Ensure Kafka MirrorMaker2 pod remains stable (no roll). | No unexpected pod restarts detected. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testNotExistingCMSetsDefaultLogging

**Description:** Test that verifies Kafka falls back to default logging configuration when a specified ConfigMap does not exist.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create an external ConfigMap with custom logging. | ConfigMap is successfully created. |
| 2. | Deploy Kafka with the external logging ConfigMap. | Kafka is deployed with the custom logging configuration. |
| 3. | Verify log4j.properties on Kafka broker contains custom ConfigMap data. | Kafka broker uses custom logging settings. |
| 4. | Update Kafka to use a non-existing ConfigMap for logging. | Kafka is updated with new logging configuration. |
| 5. | Ensure there is no rolling update and verify configuration falls back to default. | No rolling update occurs, and default logging configuration is used. |
| 6. | Check Kafka status for error message regarding non-existing ConfigMap. | Kafka status indicates error about missing ConfigMap. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)

