# CertManagerST

**Description:** Test suite verifying cert-manager CA integration: The operators delegates issuing of end-entity certificate to an external cert-manager issuer while the CA public cert is provided by the user in a Kubernetes Secret.

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testCertManagerClusterAndClientsCa

**Description:** Test verifying cert-manager CA integration for both cluster and clients CA, including KafkaUser certificate issuance and certificate renewal. A new Kafka cluster is deployed with clusterCa.type=cert-manager and clientsCa.type=cert-manager. cert-manager issues all component and user end-entity certificates. The cluster must come up healthy, Secrets and annotations are verified, KafkaUser cert is verified to be issued by cert-manager, and a TLS-authenticated producer/consumer must be able to send and receive messages. Then validityDays is updated to trigger certificate renewal and the cluster must remain healthy.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create the CA cert Secret in the test namespace. | Secret is present in the test namespace. |
| 2. | Deploy Kafka with clusterCa.type=cert-manager and clientsCa.type=cert-manager, generateCertificateAuthority=false. | Kafka cluster reaches ready state without errors. |
| 3. | Assert cluster CA cert Secret has correct annotations. | ca-cert-generation=0, ca-key-generation=0, and cert-hash annotations are set. |
| 4. | Assert the cert-manager broker and cluster operator Secrets (-cm suffix) exist and their certificates match the corresponding Strimzi Secrets and are signed by the cert-manager CA. | cert-manager Secrets exist, their certificates match the Strimzi Secrets, and the issuer DNs match the CA subject DN. |
| 5. | Create a KafkaUser and assert that the cert-manager managed user Secret (-cm suffix) exists, its tls.crt matches user.crt, and the user cert is signed by the cert-manager CA. | cert-manager user Secret exists, certificates match, and issuer DN matches cert-manager CA subject DN. |
| 6. | Produce and consume messages over TLS using the KafkaUser. | Messages are successfully produced and consumed. |
| 7. | Edit the Kafka CR to increase validityDays on clusterCa, causing cert-manager to re-issue broker certs with the new duration. | Kafka CR is accepted by the API server. |
| 8. | Wait for all broker pods to roll and become ready. | All broker pods have a new UID after the rolling update. |
| 9. | Produce and consume messages over TLS using a KafkaUser after renewal. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)

