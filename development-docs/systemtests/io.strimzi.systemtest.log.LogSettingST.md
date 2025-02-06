# LogSettingST

**Description:** This suite verifies logging behavior under various configurations and scenarios.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the Cluster Operator. | Cluster Operator is deployed successfully. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)

<hr style="border:1px solid">

## testBridgeLogSetting

**Description:** Test verifies Kafka Bridge logging configuration including inline logging changes and enabling/disabling GC logging.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka Bridge with custom inline logging and GC logging enabled. | Bridge is deployed with the desired log settings and GC logging on. |
| 2. | Check the associated ConfigMap to ensure logging levels match expectations. | Log levels are applied correctly in the Bridge. |
| 3. | Update JVM options to disable GC logging for the Bridge. | Bridge configuration is updated to turn off GC logging. |
| 4. | Confirm that pods, after any rolling updates, now run without GC logging. | Bridge runs with GC logging disabled and correct log levels. |
| 5. | Ensure the logging hierarchy and inline logging adjustments persist and function correctly. | Bridge continues to operate with the desired logging setup. |

**Labels:**

* [bridge](labels/bridge.md)
* [logging](labels/logging.md)


## testConnectLogSetting

**Description:** Test verifies Kafka Connect logging configuration, dynamic GC logging changes, and inline logging modifications.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a KafkaConnect instance with specified inline logging and GC logging enabled. | KafkaConnect is deployed with correct log levels and GC logging. |
| 2. | Verify log settings in the ConfigMap and confirm GC logging is enabled in the Connect pod. | Log configuration is correct and GC logging is initially enabled. |
| 3. | Update KafkaConnect JVM options to disable GC logging. | KafkaConnect configuration is updated successfully. |
| 4. | Wait for any rolling updates and confirm GC logging is disabled afterwards. | KafkaConnect pods reflect disabled GC logging. |
| 5. | Check that the configured log levels remain consistent after the JVM option changes. | Logging remains functional and at the expected levels. |

**Labels:**

* [connect](labels/connect.md)
* [logging](labels/logging.md)


## testCruiseControlLogChange

**Description:** Test verifies that changing the Cruise Control logging configuration dynamically applies without requiring a full restart.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with Cruise Control enabled and default logging settings. | Kafka (with Cruise Control) is deployed and running normally. |
| 2. | Check the Cruise Control log level and confirm it's at the default (INFO) level. | Cruise Control logs confirm the current default logging level. |
| 3. | Change the Cruise Control logging level to DEBUG via inline logging updates. | Logging configuration changes are applied to Cruise Control dynamically. |
| 4. | Wait for Cruise Control to detect and apply the new logging configuration. | Cruise Control logs now show DEBUG level messages. |
| 5. | Verify no unnecessary restarts occur and Cruise Control continues to function properly. | Cruise Control remains stable and reflects the new DEBUG logging level. |

**Labels:**

* [cruise-control](labels/cruise-control.md)
* [logging](labels/logging.md)


## testKafkaLogSetting

**Description:** Test verifies Kafka logging configuration (both inline and GC logging) and dynamic changes in Kafka components.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with specified inline logging configurations and GC logging enabled. | Kafka cluster is deployed successfully with correct log and GC settings. |
| 2. | Check Kafka logging levels in the generated ConfigMaps. | Logging levels match the expected configuration. |
| 3. | Verify that GC logging is enabled in Kafka and Entity Operator components. | GC logging is confirmed to be enabled. |
| 4. | Change JVM options to disable GC logging. | Kafka resources are updated to disable GC logging. |
| 5. | Wait for rolling updates (if any) and verify that GC logging is disabled. | No unexpected rolling updates occur, and GC logging is now disabled. |
| 6. | Ensure that the changes do not break logging hierarchy or default loggers. | Logging functions normally and retains specified levels. |

**Labels:**

* [kafka](labels/kafka.md)
* [logging](labels/logging.md)


## testMirrorMaker2LogSetting

**Description:** Test verifies Kafka MirrorMaker2 logging configuration, inline logging updates, and toggling of GC logging.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka MirrorMaker2 with inline logging set and GC logging enabled. | MirrorMaker2 is deployed with desired log configuration and GC logging. |
| 2. | Verify that logs in MirrorMaker2 match the configured levels and GC logging status. | Log checks confirm correct levels and GC logging enabled. |
| 3. | Update MirrorMaker2 JVM options to disable GC logging. | Configuration is updated and applied to MirrorMaker2. |
| 4. | Wait for any rolling updates and check that GC logging is disabled now. | MirrorMaker2 pods reflect disabled GC logging. |
| 5. | Confirm that the log levels remain correct and that no unexpected changes occurred. | MirrorMaker2 runs with correct logging hierarchy and no issues. |

**Labels:**

* [logging](labels/logging.md)
* `mirror-maker-2` (description file doesn't exist)

