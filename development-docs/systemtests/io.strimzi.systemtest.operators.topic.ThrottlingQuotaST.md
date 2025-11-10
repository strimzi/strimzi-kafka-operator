# ThrottlingQuotaST

**Description:** Tests user throttling quotas during topic operations.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with both SCRAM-SHA-512 and TLS authentication listeners. | Kafka cluster is deployed and ready. |
| 2. | Create two Admin clients: one with throttling quotas applied, and one without. | Both clients are successfully initialized for use in tests. |

**Labels:**

* [topic-operator](labels/topic-operator.md)

<hr style="border:1px solid">

## testThrottlingQuotasDuringAllTopicOperations

**Description:** Verifies throttling quotas on create, alter and delete topic operations.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Use the limited admin client to create a large number of topics. | Quota is exceeded and topic creation fails with a throttling error. |
| 2. | Use the unlimited admin client to delete all previously created topics. | Topics are successfully removed. |
| 3. | Create and alter a small number of topics within quota limits using the limited client. | All operations complete successfully. |
| 4. | Attempt to delete remaining topics exceeding the quota. | Deletion partially fails due to throttling. |
| 5. | Clean up all remaining topics using the unlimited client. | All topics are deleted successfully. |

**Labels:**

* [topic-operator](labels/topic-operator.md)

