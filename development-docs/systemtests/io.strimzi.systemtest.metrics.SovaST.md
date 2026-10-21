# SovaST

**Description:** This test suite verifies the Sova consumer lag exporter configured through `spec.extraMetrics` in the Kafka custom resource. A single small Kafka cluster is shared by all test cases and each test case only changes the Sova configuration and restores it at the end.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator. | Cluster Operator is deployed. |
| 2. | Deploy Kafka with Sova enabled and a scraper Pod. | Kafka and Sova are deployed. |
| 3. | Create three topics and exchange messages on each of them with three consumer groups. | Topics and consumer groups exist and Sova exposes the first metrics. |

**After test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Common cleaning of all resources created by this test class. | All resources deleted. |

**Labels:**

* [metrics](labels/metrics.md)

<hr style="border:1px solid">

## testEnableDisable

**Description:** This test case checks that Sova can be disabled and enabled again by removing and adding `spec.extraMetrics`.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Remove `spec.extraMetrics` from the Kafka custom resource. | Sova Deployment is deleted. |
| 2. | Add `spec.extraMetrics` back. | Sova Deployment is created and exposes the metrics again. |

**Labels:**

* [metrics](labels/metrics.md)


## testJvmAndMemoryOptions

**Description:** This test case checks that the memory limit and `jvmOptions` are passed to the Sova JVM.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set a memory limit of 256Mi and wait for the Sova rolling update. | Sova starts with the heap derived from the memory limit (-Xmx set). |
| 2. | Set `jvmOptions` with `-Xmx` of 128m and `-XX` option `MaxMetaspaceSize` of 128m. | Sova starts with the configured options. |
| 3. | Restore the default configuration. | Sova rolls back to the default configuration. |

**Labels:**

* [metrics](labels/metrics.md)


## testLoggingChange

**Description:** This test case checks that a change of the Sova logging is applied without rolling the Sova Pod.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Change the root logger level to DEBUG using inline logging. | The Sova Pod is not rolled. |
| 2. | Wait for the logging configuration to be reloaded. | Sova prints DEBUG messages. |
| 3. | Remove the logging configuration. | The Sova Pod is not rolled. |

**Labels:**

* [metrics](labels/metrics.md)


## testRegexFilters

**Description:** This test case checks that the group and topic regular expressions are applied to the whole name and that the exclude expressions win over the include ones.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set `groupRegex` to `sova-or.*` and `topicRegex` to `sova-checkout` and wait for the Sova rolling update. | Only group `sova-orders` and topic `sova-checkout` are exposed. |
| 2. | Set `topicRegex` to `sova` (a prefix of the topic names, not a full match). | No topic is exposed, because the expression must match the whole name. |
| 3. | Set `groupRegex` to `sova-.*` with `groupExcludeRegex` `sova-audit` and `topicRegex` to `sova-.*` with `topicExcludeRegex` `sova-audit`. | Group and topic `sova-audit` are not exposed, the others are. |
| 4. | Restore the default configuration. | Sova rolls and exposes everything again. |

**Labels:**

* [metrics](labels/metrics.md)

