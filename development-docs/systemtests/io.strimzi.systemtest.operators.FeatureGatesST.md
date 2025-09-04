# FeatureGatesST

**Description:** Feature Gates test suite verifying that feature gates provide additional options to control and mature different behaviors in the operators, specifically testing Server Side Apply functionality.

**Before tests execution steps:**

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
| 4. | Enable Server Side Apply Phase 1 feature gate. | Cluster Operator is reconfigured with SSA enabled and rolled. |
| 5. | Add manual annotations to Kafka resources and verify they are preserved. | Manual annotations are preserved during reconciliation when SSA is enabled. |
| 6. | Disable Server Side Apply Phase 1 feature gate. | Cluster Operator is reconfigured with SSA disabled and rolled. |
| 7. | Add manual annotations to Kafka resources and verify they are removed again. | Manual annotations are removed during reconciliation when SSA is disabled again. |

**Labels:**

* [kafka](labels/kafka.md)

