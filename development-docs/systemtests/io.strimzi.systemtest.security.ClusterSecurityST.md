# ClusterSecurityST

**Description:** Test suite for verifying configurable security of the Kafka cluster's internal communication.

**Labels:**

* [security](labels/security.md)
* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testClusterSecurityConfiguration

**Description:** Test a Kafka cluster that uses different combinations of authentication and encryption for internal communication.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with specified encryption and authentication (including Entity Operator and Cruise Control). | The cluster is ready with the requested security configuration. |
| 2. | Create a TLS user with ACLs and send and consume messages over mTLS. | The messages are authorized, sent, and consumed successfully. |
| 3. | Change dynamic Kafka configuration. | The configuration is applied without rolling the Kafka pods. |
| 4. | Change read-only Kafka configuration. | The Kafka controllers and brokers roll and remain functional. |
| 5. | Run a Kafka rebalance. | Cruise Control completes the rebalance. |

**Labels:**

* [security](labels/security.md)
* [kafka](labels/kafka.md)
* [cruise-control](labels/cruise-control.md)


## testClusterSecurityMigration

**Description:** Test migrating internal cluster security from the default TLS and mTLS configuration to no security and back.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a persistent Kafka cluster with authorization and send and consume messages using a TLS user with ACLs. | The cluster is ready and the messages are authorized and available. |
| 2. | Pause reconciliation, stop all cluster workloads, remove status.clusterSecurity, disable TLS and mTLS, and unpause. | The cluster restarts without internal encryption or authentication. |
| 3. | Consume the existing messages and send and consume new messages. | Both the existing and new messages are available. |
| 4. | Repeat the stopped migration and remove the internal security annotation. | The cluster restarts with the default TLS and mTLS configuration. |
| 5. | Consume all existing messages and send and consume more messages. | All existing and new messages are available after the round-trip migration. |

**Labels:**

* [security](labels/security.md)
* [kafka](labels/kafka.md)

