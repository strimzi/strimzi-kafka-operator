# FeatureGatesST

**Description:** Feature Gates test suite verifying that feature gates provide additional options to control operator behavior, specifically testing Server Side Apply functionality.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with configurable feature gates. | Cluster Operator is deployed with feature gate support. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testServerSideApply

**Description:** This test verifies that Server Side Apply Phase 1 feature gate works correctly by testing annotation preservation behavior. When SSA is disabled, manual annotations are removed during reconciliation. When SSA is enabled, manual annotations are preserved.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with Server Side Apply Phase 1 disabled. | Cluster Operator is deployed without SSA feature gate. |
| 2. | Create Kafka cluster with broker and controller node pools. | Kafka cluster is deployed and ready. |
| 3. | Add manual annotations to Kafka resources and verify they are removed. | Manual annotations are removed during reconciliation when SSA is disabled. |
| 4. | Enable Server Side Apply Phase 1 feature gate. | Cluster Operator is reconfigured with SSA enabled and redeployed. |
| 5. | Add manual annotations to Kafka resources and verify they are preserved. | Manual annotations are preserved during reconciliation when SSA is enabled. |
| 6. | Disable Server Side Apply Phase 1 feature gate. | Cluster Operator is reconfigured with SSA disabled and rolled. |
| 7. | Add manual annotations to Kafka resources and verify they are removed again. | Manual annotations are removed during reconciliation when SSA is disabled again. |

**Labels:**

* [kafka](labels/kafka.md)


## testUseBackgroundPodDeletion

**Description:** Verifies that the UseBackgroundPodDeletion feature gate correctly switches pod deletion propagation from FOREGROUND to BACKGROUND. With FOREGROUND deletion, terminating pods have the 'foregroundDeletion' finalizer set. With BACKGROUND deletion, terminating pods do not have this finalizer.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default deletion propagation (FOREGROUND). | Cluster Operator is deployed without background pod deletion feature gate. |
| 2. | Create Kafka cluster with broker and controller node pools. | Kafka cluster is deployed and ready. |
| 3. | Trigger manual rolling update and verify pods are deleted with FOREGROUND propagation. | Terminating broker pods have the finalizer. |
| 4. | Enable UseBackgroundPodDeletion feature gate. | Cluster Operator is reconfigured with background pod deletion enabled. |
| 5. | Trigger manual rolling update and verify pods are deleted with BACKGROUND propagation. | Terminating broker pods do not have the finalizer. |

**Labels:**

* [kafka](labels/kafka.md)

