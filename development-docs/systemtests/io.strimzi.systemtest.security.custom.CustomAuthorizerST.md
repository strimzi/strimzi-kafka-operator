# CustomAuthorizerST

**Description:** Test suite for verifying custom authorization functionality using ACLs (Access Control Lists) with simple authorization and TLS listener.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with custom authorization, TLS listener, and configured superuser. | Kafka cluster is deployed and ready. |

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testAclRuleReadAndWrite

**Description:** This test case verifies Access Control Lists with simple authorization and TLS listener.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create first KafkaUser with ACLs to write and describe specific topic. | KafkaUser authorized to produce into specific topic is ready. |
| 2. | Create second KafkaUser with ACLs to read and describe specific topic. | KafkaUser authorized to consume from specific topic is ready. |
| 3. | Deploy Kafka clients using first KafkaUser authorized to produce data into specific topic. | Producer completes successfully whereas consumer times out. |
| 4. | Deploy Kafka clients using second KafkaUser authorized to consume data from specific topic. | Consumer completes successfully. |
| 5. | Verify that KafkaUser with read-only ACLs cannot produce messages. | Producer times out. |

**Labels:**

* [security](labels/security.md)


## testAclWithSuperUser

**Description:** This test case verifies that a superuser can produce and consume messages without explicit ACL rules.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaUser with name matching the configured superuser. | Admin KafkaUser is ready. |
| 2. | Deploy Kafka clients using admin KafkaUser. | Producer and consumer complete successfully. |

**Labels:**

* [security](labels/security.md)

