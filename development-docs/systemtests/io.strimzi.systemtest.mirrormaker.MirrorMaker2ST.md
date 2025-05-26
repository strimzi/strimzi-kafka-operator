# MirrorMaker2ST

**Description:** Tests message mirroring, TLS/SCRAM/mTLS security, offset and header replication, scaling, rolling updates (secrets/certs), and connector state management.

**Labels:**

* `mirror-maker-2` (description file doesn't exist)

<hr style="border:1px solid">

## testIdentityReplicationPolicy

**Description:** Checks MM2 with IdentityReplicationPolicy, preserving topic names between clusters.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source and target clusters and a scraper pod. | Test infra is ready. |
| 2. | Deploy MM2 with IdentityReplicationPolicy. | MM2 uses identity policy. |
| 3. | Produce and consume messages in source. | Source cluster works. |
| 4. | Consume mirrored messages in target. | Target cluster sees unmodified topic names. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testKafkaMirrorMaker2ConnectorsStateAndOffsetManagement

**Description:** Checks MM2 connector state transitions and offset management, including error handling.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy clusters and MM2 with wrong config to force connector failure. | MM2 shows NotReady with error. |
| 2. | Fix config, MM2 becomes Ready. | Connectors start working after fix. |
| 3. | Pause/resume connector and verify state transitions. | Connector state and message mirroring respond as expected. |
| 4. | Verify offsets using scraper and config maps. | Offset values are correct in external store. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testKMM2RollAfterSecretsCertsUpdateScramSha

**Description:** Validates rolling update of MM2 after changing SCRAM-SHA user secrets and certificates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy clusters and users with SCRAM-SHA. | SCRAM users and clusters are ready. |
| 2. | Deploy MM2 with SCRAM and CA credentials. | MM2 mirrors messages. |
| 3. | Update source and target user passwords, verify MM2 pod roll. | MM2 is rolled after secret update. |
| 4. | Produce and consume after rolling update. | Mirroring continues to work after secrets change. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testKMM2RollAfterSecretsCertsUpdateTLS

**Description:** Validates rolling update of MM2 after changing TLS user secrets and cluster certificates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy clusters and users with TLS. | TLS users and clusters are ready. |
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
| 3. | Produce and consume messages on the source cluster. | Clients successfully send and receive messages. |
| 4. | Check MirrorMaker2 config map, pod labels, and other metadata. | Configuration and labels match expectations. |
| 5. | Verify messages are mirrored to the target cluster. | Target cluster consumer receives mirrored messages. |
| 6. | Trigger a manual rolling update of MM2 via annotation. | MM2 pods roll and state is preserved. |
| 7. | Verify partition count propagation by updating topic. | Partition update is reflected on the mirrored topic. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testMirrorMaker2CorrectlyMirrorsHeaders

**Description:** Checks that Kafka headers are correctly mirrored by MM2.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy clusters, topic, and MM2. | Prerequisites are ready. |
| 2. | Produce messages with headers on source. | Headers are sent to Kafka. |
| 3. | Consume from mirrored topic on target. | Headers are present in consumer log. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testMirrorMaker2TlsAndScramSha512Auth

**Description:** Checks message mirroring over TLS with SCRAM-SHA-512 authentication.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source and target Kafka clusters with TLS listeners and SCRAM-SHA-512 auth. | Clusters and SCRAM users ready. |
| 2. | Produce/consume messages over TLS+SCRAM on the source. | SCRAM-SHA-512 client ops succeed. |
| 3. | Deploy MM2 with SCRAM-SHA-512 credentials and trusted certs. | MM2 connects using SCRAM-SHA-512. |
| 4. | Consume from mirrored topic on the target cluster. | Mirrored messages are readable via SCRAM-SHA-512. |
| 5. | Verify mirrored topic's partition count. | Partition counts match. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testMirrorMaker2TlsAndTlsClientAuth

**Description:** Checks message mirroring over TLS with mTLS authentication.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source and target Kafka clusters with TLS listeners and mTLS auth. | TLS clusters and users are ready. |
| 2. | Deploy topic and TLS users. | Topic and users with TLS auth exist. |
| 3. | Produce/consume messages over TLS on the source. | TLS client operations succeed. |
| 4. | Deploy MM2 with TLS+mTLS configs and trusted certs. | MM2 connects using mTLS. |
| 5. | Consume from mirrored topic on the target cluster. | Mirrored messages are readable via TLS. |
| 6. | Verify mirrored topic's partition count. | Partition counts match. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testRestoreOffsetsInConsumerGroup

**Description:** Tests offset checkpoint/restore in consumer groups with MM2 active-active mode.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy clusters and MM2 in active-active setup. | Active-active MM2 is ready. |
| 2. | Send and consume messages across both clusters. | Clients work on both sides. |
| 3. | Produce new messages, read part from each cluster. | Offsets diverge and sync is tested. |
| 4. | Validate offset checkpoints prevent duplicate consumption. | Consumer jobs timeout as expected on empty offsets. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)


## testScaleMirrorMaker2UpAndDownToZero

**Description:** Verifies scaling MM2 up and down (including scale-to-zero).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source and target Kafka clusters. | Clusters are ready. |
| 2. | Deploy MM2 with initial replica count. | MM2 starts successfully. |
| 3. | Scale MM2 up and verify observedGeneration and pod names. | Pods increase and new pods are named correctly. |
| 4. | Scale MM2 down to zero and verify pod removal. | All MM2 pods are removed, observedGeneration increases, status is Ready. |
| 5. | Wait until MM2 status URL is null. | Status reflects shutdown. |

**Labels:**

* `mirror-maker-2` (description file doesn't exist)

