# CustomAuthorizerST

**Description:** Test suite for verifying custom authorization using ACLs (Access Control Lists) with simple authorization and a TLS listener.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster configured with custom authorization, TLS listener, and a superuser. | Kafka cluster is deployed and ready. |

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testAclRuleReadAndWrite

**Description:** This test case verifies access control lists (ACLs) with simple authorization and a TLS listener.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a KafkaUser with ACLs to write to and describe a specific topic. | KafkaUser authorized to produce to the topic is ready. |
| 2. | Create a second KafkaUser with ACLs to read from and describe the same topic. | KafkaUser authorized to consume from the topic is ready. |
| 3. | Deploy Kafka clients using the first KafkaUser to produce data to the topic. | The producer completes successfully, and the consumer times out. |
| 4. | Deploy Kafka clients using the second KafkaUser to consume data from the topic. | The consumer completes successfully. |
| 5. | Verify that KafkaUser with read-only ACLs cannot produce messages. | Producer times out. |

**Labels:**

* [security](labels/security.md)


## testAclWithSuperUser

**Description:** This test verifies that a superuser can produce and consume messages without explicit ACL rules.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaUser with TLS certificate CN matching the configured superuser DN. | The KafkaUser representing the superuser is ready. |
| 2. | Deploy Kafka clients using the superuser KafkaUser. | The producer and consumer complete successfully. |

**Labels:**

* [security](labels/security.md)

