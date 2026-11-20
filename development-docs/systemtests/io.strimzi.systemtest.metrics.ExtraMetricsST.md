# ExtraMetricsST

**Description:** This test suite verifies the Extra Metrics consumer lag exporter configured through `spec.extraMetrics` in the Kafka custom resource. A single small Kafka cluster is shared by all test cases. Each test case only changes the ExtraMetrics configuration, and the default Extra Metrics configuration is restored after each test case.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator. | Cluster Operator is deployed. |
| 2. | Deploy Kafka with Extra Metrics enabled and a scraper Pod. | Kafka and Extra Metrics are deployed. |
| 3. | Create three topics and exchange messages on each of them with three consumer groups. | Topics and consumer groups exist and Extra Metrics exposes the first metrics. |

**After test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Restore the default Extra Metrics configuration after each test case if it was changed. | Extra Metrics runs with the default configuration and has collected the metrics. |
| 2. | Common cleaning of all resources created by this test class. | All resources deleted. |

**Labels:**

* [metrics](labels/metrics.md)

<hr style="border:1px solid">

## testCollectionTuning

**Description:** This test case checks that `collectionIntervalMs` and the Kafka Admin client options in `config` are passed to Extra Metrics.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set `collectionIntervalMs` to 10000 and `config` with `default.api.timeout.ms` of 60000 and wait for the Extra Metrics rolling update. | Extra Metrics starts with a 10 second collection interval and the Admin client configuration contains the timeout. |
| 2. | Wait for the first collection cycle of the new Pod. | Extra Metrics exposes freshly collected metrics. |

**Labels:**

* [metrics](labels/metrics.md)


## testEnableDisable

**Description:** This test case checks that Extra Metrics can be disabled and enabled again by removing and adding `spec.extraMetrics`.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Remove `spec.extraMetrics` from the Kafka custom resource. | Extra Metrics Deployment is deleted. |
| 2. | Add `spec.extraMetrics` back. | Extra Metrics Deployment is created and exposes the metrics again. |

**Labels:**

* [metrics](labels/metrics.md)


## testJvmAndMemoryOptions

**Description:** This test case checks that the memory limit and `jvmOptions` are passed to the Extra Metrics JVM.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set a memory limit of 256Mi and wait for the Extra Metrics rolling update. | Extra Metrics starts with the heap derived from the memory limit (-Xmx set). |
| 2. | Set `jvmOptions` with `-Xmx` of 128m and `-XX` option `MaxMetaspaceSize` of 128m. | Extra Metrics starts with the configured options. |

**Labels:**

* [metrics](labels/metrics.md)


## testLoggingChange

**Description:** This test case checks that a change of the Extra Metrics logging is applied without rolling the Extra Metrics Pod.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Change the root logger level to DEBUG using inline logging. | The Extra Metrics Pod is not rolled. |
| 2. | Wait for the logging configuration to be reloaded. | Extra Metrics prints DEBUG messages. |
| 3. | Remove the logging configuration. | The Extra Metrics Pod is not rolled. |

**Labels:**

* [metrics](labels/metrics.md)


## testRegexFilters

**Description:** This test case checks that the group and topic regular expressions are applied to the whole name and that the exclude expressions win over the include ones.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set `groupRegex` to `sova-or.*` and `topicRegex` to `sova-checkout` and wait for the Extra Metrics rolling update. | Only group `sova-orders` and topic `sova-checkout` are exposed. |
| 2. | Set `topicRegex` to `sova` (a prefix of the topic names, not a full match). | No topic is exposed, because the expression must match the whole name. |
| 3. | Set `groupRegex` to `sova-.*` with `groupExcludeRegex` `sova-audit` and `topicRegex` to `sova-.*` with `topicExcludeRegex` `sova-audit`. | Group and topic `sova-audit` are not exposed, the others are. |

**Labels:**

* [metrics](labels/metrics.md)

