# OauthAuthorizationST

**Description:** Test suite for verifying OAuth 2.0 authorization using Keycloak as an external authorizer with OAUTHBEARER mechanism over a TLS listener, validating topic-level access control policies for different client teams.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator, Keycloak, and necessary OAuth secrets. | Cluster Operator and Keycloak are deployed and ready. |
| 2. | Deploy Kafka cluster with custom OAuth authentication and Keycloak authorization over TLS listener. | Kafka cluster is deployed and ready with OAuth authentication and Keycloak authorization configured. |
| 3. | Create TLS users for team-a-client and team-b-client. | TLS users are created and ready. |

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## smokeTestForClients

**Description:** Test verifying that team A OAuth client can produce and consume messages on topics starting with 'a-' using OAUTHBEARER mechanism over TLS listener.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a topic starting with 'a-' prefix. | Topic is created. |
| 2. | Deploy team A OAuth producer and consumer over TLS. | Producer and consumer successfully authenticate and exchange messages. |

**Labels:**

* [security](labels/security.md)


## testTeamAReadFromTopic

**Description:** Test verifying that team A OAuth client can only consume messages using a consumer group starting with 'a-', and is denied when using an unauthorized consumer group.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Produce messages to a topic starting with 'a-' as team A. | Messages are produced successfully. |
| 2. | Attempt to consume messages using an unauthorized consumer group. | Consumer fails due to authorization denial. |
| 3. | Consume messages using a consumer group starting with 'a-'. | Consumer successfully receives messages. |

**Labels:**

* [security](labels/security.md)


## testTeamAWriteToTopic

**Description:** Test verifying that team A OAuth client is denied writing to unauthorized topics, can write to existing topics starting with 'x-', and can write to topics starting with 'a-'.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Attempt to produce messages to an unauthorized topic as team A. | Producer fails due to authorization denial. |
| 2. | Attempt to produce messages to a non-existent topic starting with 'x-' as team A. | Producer fails because team A cannot create 'x-' topics. |
| 3. | Create topic starting with 'x-' and produce messages as team A. | Producer successfully sends messages to the existing 'x-' topic. |
| 4. | Produce messages to a topic starting with 'a-' as team A. | Producer successfully sends messages to the 'a-' topic. |

**Labels:**

* [security](labels/security.md)


## testTeamAWriteToTopicStartingWithXAndTeamBReadFromTopicStartingWithX

**Description:** Test verifying that team A OAuth client can write to topics starting with 'x-' and team B OAuth client can read from those topics, validating cross-team topic access policies.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a topic starting with 'x-' and produce messages as team A. | Producer successfully sends messages to the 'x-' topic. |
| 2. | Consume messages from the 'x-' topic as team B. | Consumer successfully receives messages from the 'x-' topic. |

**Labels:**

* [security](labels/security.md)


## testTeamBWriteToTopic

**Description:** Test verifying that team B OAuth client is denied writing to unauthorized topics, and can write and read from topics starting with 'b-'.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Attempt to produce messages to an unauthorized topic as team B. | Producer fails due to authorization denial. |
| 2. | Produce and consume messages on a topic starting with 'b-' as team B. | Producer and consumer successfully exchange messages on the 'b-' topic. |

**Labels:**

* [security](labels/security.md)

