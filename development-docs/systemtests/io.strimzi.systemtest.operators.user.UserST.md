# UserST

**Description:** Test suite for various Kafka User operations and configurations.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize shared test storage and deploy Kafka cluster with necessary configuration. | Kafka cluster and scraper pod are deployed and ready for testing. |

**Labels:**

* [user-operator](labels/user-operator.md)

<hr style="border:1px solid">

## testCreatingUsersWithSecretPrefix

**Description:** Verifies creating Kafka users with custom secret prefixes for organizing user secrets.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Configure cluster operator with custom secret prefix. | Cluster operator is reconfigured with the specified secret prefix. |
| 2. | Create TLS and SCRAM-SHA-512 users. | Users are created successfully with TLS and SCRAM-SHA-512 authentication. |
| 3. | Verify user secrets are created with correct prefix. | User secrets are created with the configured prefix in their names. |
| 4. | Test message sending and receiving. | Messages are successfully sent and received using both authentication methods. |
| 5. | Update users and verify secret updates. | User updates are reflected in the prefixed secrets. |
| 6. | Delete users and verify cleanup. | User deletion removes the prefixed secrets properly. |

**Labels:**

* [user-operator](labels/user-operator.md)


## testScramUserWithQuotas

**Description:** Verifies that SCRAM-SHA-512 authenticated Kafka users can be configured with quotas.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create SCRAM-SHA-512 user with quota configuration. | User is created successfully with SCRAM-SHA-512 authentication and quota settings applied. |

**Labels:**

* [user-operator](labels/user-operator.md)


## testTlsExternalUser

**Description:** Verifies TLS external user authentication with custom certificates and ACL authorization.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with TLS authentication and Simple authorization. | Kafka cluster is deployed with TLS listener and Simple ACL authorization enabled. |
| 2. | Create TLS external user with ACL permissions. | TLS external user is created with specified ACL rules for topic access. |
| 3. | Create custom external TLS secret for user. | External TLS secret is created with custom certificates. |
| 4. | Test message sending and receiving with TLS external user. | Messages are successfully sent and received using external TLS certificates. |

**Labels:**

* [user-operator](labels/user-operator.md)


## testTlsExternalUserWithQuotas

**Description:** Verifies that Kafka users authenticated with external TLS can be configured with quotas.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TLS external user with quota configuration. | User is created successfully with TLS external authentication and quota settings applied. |

**Labels:**

* [user-operator](labels/user-operator.md)


## testTlsUserWithQuotas

**Description:** Verifies that TLS authenticated Kafka users can be configured with quotas.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TLS user with quota configuration. | User is created successfully with TLS authentication and quota settings applied. |

**Labels:**

* [user-operator](labels/user-operator.md)


## testUpdateUser

**Description:** Verifies updating a Kafka user from TLS to SCRAM-SHA-512 authentication and validates user secret contents.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TLS Kafka user. | User is created with TLS authentication and secret contains TLS certificates. |
| 2. | Verify TLS user secret contents. | Secret contains `ca.crt`, `user.crt`, and `user.key` fields. |
| 3. | Test message sending and receiving with TLS user. | Messages are successfully sent and received. |
| 4. | Update user authentication to SCRAM-SHA-512. | User authentication is updated successfully. |
| 5. | Verify SCRAM-SHA-512 user secret contents. | Secret contains SCRAM-SHA-512 `password` field and TLS certificates are removed. |
| 6. | Test message sending and receiving with SCRAM-SHA-512 user. | Messages are successfully sent and received with SCRAM-SHA-512 authentication. |

**Labels:**

* [user-operator](labels/user-operator.md)


## testUserWithNameMoreThan64Chars

**Description:** Verifies that Kafka users with names longer than 64 characters are rejected, while users with valid names are accepted.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka user with valid name (64 characters). | User is successfully created and becomes ready. |
| 2. | Create SASL user with long name (65 characters). | SASL user is created successfully since SASL users support longer names. |
| 3. | Attempt to create TLS user with long name (65 characters). | User creation fails with validation error. |
| 4. | Verify error condition and message. | Error condition indicates username limitation and provides an appropriate error message. |

**Labels:**

* [user-operator](labels/user-operator.md)

