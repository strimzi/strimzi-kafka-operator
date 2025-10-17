# NamespaceRbacScopeOperatorST

**Description:** Test suite for verifying namespace-scoped RBAC deployment mode for Cluster Operator, ensuring that Roles are created instead of ClusterRoles when operating in namespace-scoped mode.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Skip test suite if using OLM or Helm installation. | Test suite only runs with YAML-based installations. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testNamespacedRbacScopeDeploysRoles

**Description:** This test verifies that when Cluster Operator is deployed with namespace-scoped RBAC, it creates Roles instead of ClusterRoles, ensuring proper isolation and avoiding cluster-wide permissions.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with namespace-scoped RBAC configuration. | Cluster Operator is deployed with RBAC type set to NAMESPACE. |
| 2. | Deploy Kafka cluster with broker and controller node pools. | Kafka cluster is deployed and becomes ready. |
| 3. | Verify no ClusterRoles with Strimzi labels exist. | No ClusterRoles labeled with 'app=strimzi' are found in the cluster. |

**Labels:**

* [kafka](labels/kafka.md)

