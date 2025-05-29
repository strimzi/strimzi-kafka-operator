# MirrorMaker2ST

**Description:** Tests MirrorMaker2 cross-cluster replication with various security (TLS, SCRAM), header and offset synchronisation, scaling and rolling update handling, and connector error state transitions.

**Labels:**

* `mirror-maker-2` (description file doesn't exist)

<hr style="border:1px solid">

## testIdentityReplicationPolicy

**Description:** Checks MM2 with IdentityReplicationPolicy, preserving topic names between Kafka clusters.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source and target Kafka clusters and a scraper pod. | Kafka clusters and scraper pod are ready. |
| 2. | Deploy MM2 with IdentityReplicationPolicy. | MM2 uses identity policy. |
| 3. | Produce and consume messages to/from the source Kafka cluster. | Messages are successfully produced and consumed in the source Kafka cluster. |
| 4. | Consume messages from the mirrored topic on the target Kafka cluster. | Mirrored topic has the same (unmodified) name as the source topic, and messages are consumed successfully. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testKafkaMirrorMaker2ConnectorsStateAndOffsetManagement

**Description:** Checks MM2 connector state transitions and offset management, including error handling.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka clusters and MM2 with wrong config to force connector failure. | MM2 shows NotReady with error. |
| 2. | Correct the MM2 configuration (fix bootstrap server address) to resolve connector failure. MM2 becomes Ready. | Connectors transition from FAILED to RUNNING state after the configuration fix. |
| 3. | Pause/resume connector and verify state transitions. | Connector state and message mirroring respond as expected. |
| 4. | Verify offsets using scraper and config maps. | Offset values are correct in external store. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testKMM2RollAfterSecretsCertsUpdateScramSha

**Description:** Validates rolling update of MM2 after changing SCRAM-SHA user secrets and certificates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka clusters and users with SCRAM-SHA. | SCRAM-SHA users and Kafka clusters are ready. |
| 2. | Deploy MM2 with SCRAM-SHA and CA credentials. | MM2 mirrors messages. |
| 3. | Update source and target user passwords, verify MM2 pod rolling update. | MM2 is rolled after secret update. |
| 4. | Produce and consume after rolling update. | Mirroring continues to work after secrets change. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testKMM2RollAfterSecretsCertsUpdateTLS

**Description:** Validates rolling update of MM2 after changing TLS user secrets and cluster certificates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka clusters and users with TLS. | TLS users and Kafka clusters are ready. |
| 2. | Deploy MM2 with TLS credentials and trusted certs. | MM2 mirrors messages. |
| 3. | Renew client and cluster CA secrets, verify MM2 and Kafka pods roll. | Pods are rolled after CA update. |
| 4. | Produce and consume after rolling update. | Mirroring continues to work after secret/cert change. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testMirrorMaker2

**Description:** Verifies MM2 mirrors messages between two clusters and handles rolling update events.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source and target Kafka clusters with default settings. | Kafka clusters are ready. |
| 2. | Deploy MirrorMaker 2 and a source topic. | MirrorMaker 2 is deployed and initial topic exists. |
| 3. | Produce and consume messages on the source cluster. | Clients successfully produce and consume messages. |
| 4. | Check MirrorMaker2 config map, pod labels, and other metadata. | Configuration and labels match expectations. |
| 5. | Verify messages are mirrored to the target cluster. | Target cluster consumer consumes mirrored messages. |
| 6. | Trigger a manual rolling update of MM2 via annotation. | MM2 pods roll and state is preserved. |
| 7. | Verify partition count propagation by updating topic. | Partition update is reflected on the mirrored topic. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testMirrorMaker2CorrectlyMirrorsHeaders

**Description:** Checks that Kafka headers are correctly mirrored by MM2.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka clusters, topic, and MM2. | Kafka clusters, topic and MM2 are ready. |
| 2. | Produce messages with specific headers to the source Kafka cluster. | Messages with headers are produced to source Kafka cluster. |
| 3. | Consume from mirrored topic on target Kafka cluster. | Headers are present in consumer log. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testMirrorMaker2TlsAndScramSha512Auth

**Description:** Checks message mirroring over TLS with SCRAM-SHA-512 authentication.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source and target Kafka clusters with TLS listeners and SCRAM-SHA-512 auth. | Kafka clusters and SCRAM users ready. |
| 2. | Produce/consume messages over TLS+SCRAM to the source Kafka cluster. | Producer and consumer clients successfully operate using SCRAM-SHA-512 authentication over TLS. |
| 3. | Deploy MM2 with SCRAM-SHA-512 credentials and trusted certs. | MM2 is running and ready with SCRAM-SHA-512 connections |
| 4. | Consume from mirrored topic on the target cluster. | Mirrored messages are successfully consumed using SCRAM-SHA-512 authentication. |
| 5. | Check the partition count of the mirrored topic in the target cluster. | Partition count matches the source topic’s partition count. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testMirrorMaker2TlsAndTlsClientAuth

**Description:** Checks message mirroring over TLS with mTLS authentication.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source and target Kafka clusters with TLS listeners and mTLS auth. | Kafka clusters and users are deployed with TLS/mTLS. |
| 2. | Deploy topic and TLS users. | Topic and users with TLS auth exist. |
| 3. | Produce/consume messages over TLS on the source. | TLS client operations succeed. |
| 4. | Deploy MM2 with TLS+mTLS configs and trusted certs. | MM2 is running and ready with mTLS connections. |
| 5. | Consume from mirrored topic on the target cluster. | Mirrored messages are successfully consumed using TLS. |
| 6. | Check the partition count of the mirrored topic in the target cluster. | Partition count matches the source topic’s partition count. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testRestoreOffsetsInConsumerGroup

**Description:** Tests offset checkpoint/restore in consumer groups with MM2 active-active mode.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka clusters and MM2 in active-active setup. | Active-active MM2 is ready. |
| 2. | Produce and consume messages across both Kafka clusters. | Messages were produced and consumed without issues. |
| 3. | Produce new messages, then consume a portion from the source cluster and a portion from the target cluster. | Offsets diverge between Kafka clusters and synchronization is tested. |
| 4. | Validate offset checkpoints prevent duplicate consumption. | Consumer jobs timeout as expected on empty offsets. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testScaleMirrorMaker2UpAndDownToZero

**Description:** Verifies scaling MM2 up and down (including scale-to-zero).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source and target Kafka clusters. | Kafka clusters are ready. |
| 2. | Deploy MM2 with initial replica count. | MM2 starts successfully. |
| 3. | Scale MM2 up and verify observedGeneration and pod names. | Pods increase and new pods are named correctly. |
| 4. | Scale MM2 down to zero replicas and wait until MM2 status URL is null. | All MM2 pods are removed (replicas=0), and status reflects shutdown (URL is null). |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)

