# ClusterOperatorRbacST

**Description:** Test suite containing Cluster Operator RBAC scenarios related to ClusterRoleBinding (CRB) permissions, ensuring proper functionality when namespace-scoped RBAC is used instead of cluster-wide permissions.

**Labels:**

* [connect](labels/connect.md)

<hr style="border:1px solid">

## testCRBDeletionErrorIsIgnoredWhenRackAwarenessIsNotEnabled

**Description:** This test verifies that Cluster Operator can deploy Kafka and KafkaConnect resources even when ClusterRoleBinding permissions are not available, as long as rack awareness is not enabled. The test ensures that forbidden access to CRB resources is properly ignored when not required.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with namespace-scoped RBAC configuration. | Cluster Operator is configured with limited ClusterRoleBinding access. |
| 2. | Create KafkaNodePools for brokers and controllers without rack awareness. | KafkaNodePools are created successfully. |
| 3. | Deploy Kafka cluster without rack awareness configuration. | Kafka cluster is deployed successfully despite missing CRB permissions. |
| 4. | Verify Cluster Operator logs contain information about ignoring forbidden CRB access for Kafka. | Log contains message about ignoring forbidden access to ClusterRoleBindings for Kafka. |
| 5. | Deploy KafkaConnect without rack awareness configuration. | KafkaConnect is deployed successfully despite missing CRB permissions. |
| 6. | Verify Cluster Operator logs contain information about ignoring forbidden CRB access for KafkaConnect. | Log contains message about ignoring forbidden access to ClusterRoleBindings for KafkaConnect. |

**Labels:**

* [connect](labels/connect.md)


## testCRBDeletionErrorsWhenRackAwarenessIsEnabled

**Description:** This test verifies that Cluster Operator fails to deploy Kafka and KafkaConnect resources when ClusterRoleBinding permissions are not available and rack awareness is enabled. The test ensures that proper error conditions are reported when CRB access is required but not available.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with namespace-scoped RBAC configuration. | Cluster Operator is configured with limited ClusterRoleBinding access. |
| 2. | Create KafkaNodePools for brokers and controllers. | KafkaNodePools are created successfully. |
| 3. | Deploy Kafka cluster with rack awareness configuration enabled. | Kafka deployment fails due to missing CRB permissions. |
| 4. | Verify Kafka status condition contains 403 forbidden error message. | Kafka status shows NotReady condition with code=403 error. |
| 5. | Deploy KafkaConnect with rack awareness configuration enabled. | KafkaConnect deployment fails due to missing CRB permissions. |
| 6. | Verify KafkaConnect status condition contains 403 forbidden error message. | KafkaConnect status shows NotReady condition with code=403 error. |

**Labels:**

* [connect](labels/connect.md)

