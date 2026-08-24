# CertManagerST

**Description:** Test suite verifying cert-manager CA integration: The operators delegates issuing of end-entity certificate to an external cert-manager issuer while the CA public cert is provided by the user in a Kubernetes Secret.

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testCertManagerClusterCaAndRenewal

**Description:** Test verifying the cert-manager Cluster CA happy path and certificate renewal. A new Kafka cluster is deployed with clusterCa.type=cert-manager. cert-manager issues all component end-entity certificates. The cluster must come up healthy, Secrets and annotations are verified, and a TLS-authenticated producer/consumer must be able to send and receive messages. Then validityDays is updated to trigger certificates renewal. The Cluster Operator detects the cert change and rolls the broker pods. After the rolling update the cluster must remain healthy and a TLS-authenticated produce/consume must succeed.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create the CA cert Secret in the test namespace. | Secret is present in the test namespace. |
| 2. | Deploy Kafka with clusterCa.type=cert-manager, generateCertificateAuthority=false. | Kafka cluster reaches ready state without errors. |
| 3. | Assert cluster CA cert Secret has correct annotations. | ca-cert-generation=0, ca-key-generation=0, and cert-hash annotations are set. |
| 4. | Assert the cert-manager broker and cluster operator Secrets (-cm suffix) exist and their certificates match the corresponding Strimzi Secrets and are signed by the cert-manager CA. | cert-manager Secrets exist, their certificates match the Strimzi Secrets, and the issuer DNs match the CA subject DN. |
| 5. | Produce and consume messages over TLS using a KafkaUser. | Messages are successfully produced and consumed. |
| 6. | Snapshot broker pod UIDs before the change. | Snapshot captured. |
| 7. | Edit the Kafka CR to increase validityDays on clusterCa, causing cert-manager to re-issue broker certs with the new duration. | Kafka CR is accepted by the API server. |
| 8. | Wait for all broker pods to roll and become ready. | All broker pods have a new UID after the rolling update. |
| 9. | Produce and consume messages over TLS using a KafkaUser after renewal. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)


## testKafkaUserCertIssuedByCertManager

**Description:** Test verifying that when clientsCa.type=cert-manager is configured, the User Operator delegates issuing of KafkaUser TLS certificate to cert-manager. The cert-manager managed Secret (-cm suffix) must exist, and user.crt in the Strimzi user Secret must match its tls.crt.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create the CA cert Secret in the test namespace and deploy Kafka with both clusterCa.type=cert-manager and clientsCa.type=cert-manager. | Kafka cluster reaches ready state. |
| 2. | Create a KafkaUser with TLS authentication. | KafkaUser reaches ready state and its Secret is populated. |
| 3. | Assert that the cert-manager managed user Secret (<username>-cm) exists and its tls.crt matches user.crt in the Strimzi user Secret. | cert-manager Secret exists and certificates match. |
| 4. | Assert that user.crt is signed by the cert-manager clients CA (issuer DN matches clients CA subject DN). | User certificate issuer DN matches the clients CA subject DN. |
| 5. | Produce and consume messages over TLS using the KafkaUser. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)

