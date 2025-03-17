# CertManagerST

**Description:** Test suite verifying cert-manager CA integration: The operators delegates issuing of end-entity certificate to an external cert-manager issuer while the CA public cert is provided by the user in a Kubernetes Secret.

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testCertManagerClusterCaRenewal

**Description:** Test verifying that cert-manager renews end-entity certificates by updating validityDays on the Kafka cluster and that the Cluster Operator detects the cert change and rolls the broker pods. After the rolling update the cluster must remain healthy and a TLS-authenticated produce/consume must be able to send and receive messages.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create the CA cert Secret in the test namespace and deploy Kafka with clusterCa.type=cert-manager.io. | Kafka cluster reaches ready state. |
| 2. | Snapshot broker pod UIDs before the change. | Snapshot captured. |
| 3. | Edit the Kafka CR to increase validityDays on clusterCa, causing cert-manager to re-issue broker certs with the new duration. | Kafka CR is accepted by the API server. |
| 4. | Wait for all broker pods to roll and become ready. | All broker pods have a new UID after the rolling update. |
| 5. | Produce and consume messages over TLS using a KafkaUser. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)


## testKafkaUserCertIssuedByCertManager

**Description:** Test verifying that when clientsCa.type=cert-manager.io is configured, the User Operator delegates issuing of KafkaUser TLS certificate to cert-manager. The cert-manager managed Secret (-cm suffix) must exist, and user.crt in the Strimzi user Secret must match its tls.crt.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create the CA cert Secret in the test namespace and deploy Kafka with both clusterCa.type=cert-manager.io and clientsCa.type=cert-manager.io. | Kafka cluster reaches ready state. |
| 2. | Create a KafkaUser with TLS authentication. | KafkaUser reaches ready state and its Secret is populated. |
| 3. | Assert that the cert-manager managed user Secret (<username>-cm) exists and its tls.crt matches user.crt in the Strimzi user Secret. | cert-manager Secret exists and certificates match. |
| 4. | Assert that user.crt is signed by the cert-manager clients CA (issuer DN matches clients CA subject DN). | User certificate issuer DN matches the clients CA subject DN. |
| 5. | Produce and consume messages over TLS using the KafkaUser. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)


## testNewClusterWithCertManagerClusterCa

**Description:** Test verifying the cert-manager Cluster CA happy path: a new Kafka cluster is deployed with clusterCa.type=cert-manager.io. cert-manager issues all component end-entity certificates. The cluster must come up healthy and a TLS-authenticated producer/consumer must be able to send and receive messages.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create the CA cert Secret in the test namespace. | Secret is present in the test namespace. |
| 2. | Deploy Kafka with clusterCa.type=cert-manager.io, generateCertificateAuthority=false. | Kafka cluster reaches ready state without errors. |
| 3. | Assert cluster CA cert Secret has correct annotations. | ca-cert-generation=0, ca-key-generation=0, and cert-hash annotations are set. |
| 4. | Assert the cert-manager broker and cluster operator Secrets (-cm suffix) exist and their certificates match the corresponding Strimzi Secrets and are signed by the cert-manager CA. | cert-manager Secrets exist, their certificates match the Strimzi Secrets, and the issuer DNs match the CA subject DN. |
| 5. | Produce and consume messages over TLS using a KafkaUser. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)

