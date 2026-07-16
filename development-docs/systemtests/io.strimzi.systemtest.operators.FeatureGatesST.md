# FeatureGatesST

**Description:** Feature Gates test suite verifying that feature gates provide additional options to control operator behavior, specifically testing Server Side Apply functionality.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with configurable feature gates. | Cluster Operator is deployed with feature gate support. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testUseBackgroundPodDeletion

**Description:** This test verifies that the UseBackgroundPodDeletion feature gate works correctly. When enabled, Kafka broker pods are deleted with BACKGROUND propagation during rolling restarts, and the rolling restart completes successfully.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with UseBackgroundPodDeletion enabled. | Cluster Operator is deployed with background pod deletion feature gate. |
| 2. | Create Kafka cluster with broker and controller node pools. | Kafka cluster is deployed and ready. |
| 3. | Trigger manual rolling update of broker pods. | Rolling update is triggered via manual-rolling-update annotation. |
| 4. | Wait for broker pods to finish rolling. | All broker pods are rolled and ready, confirming background deletion works correctly. |

**Labels:**

* [kafka](labels/kafka.md)

