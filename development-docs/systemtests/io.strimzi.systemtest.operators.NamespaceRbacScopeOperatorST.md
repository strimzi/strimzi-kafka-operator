# NamespaceRbacScopeOperatorST

**Description:** Test suite for verifying the namespace-scoped RBAC deployment mode for the Cluster Operator, ensuring that `Role` resources are created instead of `ClusterRole` resources when operating in namespace-scoped mode.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Skip this test suite if using OLM or Helm installation. | This test suite only runs with YAML-based installations. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testNamespacedRbacScopeDeploysRoles

**Description:** This test verifies that when the Cluster Operator is deployed with namespace-scoped RBAC, it creates `Role` resources instead of `ClusterRole` resources, ensuring proper isolation and avoiding cluster-wide permissions.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the Cluster Operator with namespace-scoped RBAC configuration. | The Cluster Operator is deployed with RBAC type set to `NAMESPACE`. |
| 2. | Deploy a Kafka cluster with broker and controller node pools. | The Kafka cluster is deployed and becomes ready. |
| 3. | Verify no `ClusterRole` resources with Strimzi labels exist. | No `ClusterRole` resources labeled with `app=strimzi` are found in the cluster. |

**Labels:**

* [kafka](labels/kafka.md)

