# CertManagerST

**Description:** Test suite verifying cert-manager CA integration: The operators delegates issuing of end-entity certificate to an external cert-manager issuer while the CA public cert is provided by the user in a Kubernetes Secret.

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testCertManagerCaCertRenewal

**Description:** Test verifying CA certificate renewal when using cert-manager. A Kafka cluster is deployed with cert-manager. CA cert is renewed with the same key and then expects 1 rolling restart, ca-cert-generation incremented on pods but not on secrets, ca-key-generation unchanged.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with cert-manager cluster CA. | Kafka cluster reaches ready state. |
| 2. | Trigger cert renewal by updating the CA Certificate resource. | CA cert Secret is recreated with the same key but a new certificate. |
| 3. | Update the user-provided CA cert Secret with the renewed certificate. | Single rolling restart occurs. |
| 4. | Verify that ca-cert-generation is incremented on pods and ca-key-generation unchanged. | Generation annotations match expected values. |
| 5. | Verify cluster is functional after certificate renewal. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)


## testCertManagerCaKeyReplacement

**Description:** Test verifying CA key replacement when using cert-manager. A Kafka cluster is deployed with cert-manager. First, the CA key is replaced followed by reissuing end-entity certs by deleting -cm secrets, expects 3 rolling restarts with correct generation annotation progression. Then, the CA key is replaced without reissuing end-entity certificates,expects 2 rolling restarts and cluster stays stable. Then reissues end-entity certificates, expects another rolling restart.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with cert-manager cluster CA. | Kafka cluster reaches ready state. |
| 2. | Replace the CA key by deleting the CA cert Secret and waiting for cert-manager to regenerate it. | CA cert Secret is recreated with new key. |
| 3. | Trigger end-entity cert reissue by deleting -cm secrets. | cert-manager recreates -cm secrets signed by the new CA. |
| 4. | Verify that no rolling restart happens before updating the user-provided CA cert Secret. | Broker pods remain stable. |
| 5. | Update the user-provided CA cert Secret with the new CA certificate. | Cluster Operator detects the new CA and initiates rolling restarts. |
| 6. | Wait for 3 rolling restarts and verify generation annotations after each. | ca-key-generation incremented on pods after the first roll, ca-cert-generation incremented on both pods and secrets after the second roll, old CA cert removed in the third roll. |
| 7. | Verify cluster is functional after key replacement. | Messages are successfully produced and consumed. |
| 8. | Replace the CA key by deleting the CA cert Secret and waiting for cert-manager to regenerate it. | CA cert Secret is recreated with new key. |
| 9. | Update the user-provided CA cert Secret with the new CA certificate. | Cluster Operator detects the new CA and initiates rolling restarts. |
| 10. | Wait for 2 rolling restarts and verify generation annotations after each. | ca-key-generation incremented on pods after the first roll, ca-cert-generation incremented on only pods but not on secrets after the second roll |
| 11. | Verify that cluster is healthy and no further restarts happen after key replacement and not reissuing end-entity certificates | Broker pods remain stable. |
| 12. | Trigger end-entity cert reissue by deleting -cm secrets. | cert-manager recreates -cm secrets signed by the new CA. |
| 13. | Wait for the final rolling restart and verify generation annotations on Secrets. | ca-cert-generation incremented on secrets after the final roll. |
| 14. | Verify cluster is functional after reissuing end-entity certificates. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)


## testCertManagerClusterCa

**Description:** Test verifying cert-manager CA integration for both cluster and clients CA, including KafkaUser certificate issuance and certificate renewal. A new Kafka cluster is deployed with clusterCa.type=cert-manager. cert-manager issues all component end-entity certificates. The cluster must come up healthy, Secrets and annotations are verified.Then validityDays is updated to trigger certificate renewal and the cluster must remain healthy.and a TLS-authenticated producer/consumer must be able to send and receive messages.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create the CA cert Secret in the test namespace. | Secret is present in the test namespace. |
| 2. | Deploy Kafka with clusterCa.type=cert-manager and generateCertificateAuthority=false. | Kafka cluster reaches ready state without errors. |
| 3. | Verify that cluster CA cert Secret has correct annotations. | ca-cert-generation=0, ca-key-generation=0, and cert-hash annotations are set. |
| 4. | Verify that the cert-manager broker and cluster operator Secrets (-cm suffix) exist and their certificates match the corresponding Strimzi Secrets and are signed by the cert-manager CA. | cert-manager Secrets exist, their certificates match the Strimzi Secrets, and the issuer DNs match the CA subject DN. |
| 5. | Edit the Kafka CR to change validityDays on clusterCa, causing cert-manager to re-issue broker certificates. | Kafka CR is accepted by the API server. |
| 6. | Wait for all broker pods to roll and become ready. | All broker pods have a new UID after the rolling update. |
| 7. | Verify that broker certificate is updated. | Broker certificate does not match the certificate captured before renewal |
| 8. | Produce and consume messages over TLS using a KafkaUser after renewal. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)


## testSwitchBetweenCertManagerAndCustomCa

**Description:** Test verifying switch between cert-manager CA and custom CA. A Kafka cluster is deployed with cert-manager cluster CA, then switched to a user-provided custom CA and finally back to cert-manager. At each transition the cluster must remain operational and certificates must match the expected CA.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with cert-manager cluster CA. | Kafka cluster reaches ready state. |
| 2. | Pause reconciliation, replace cluster CA secrets with custom CA, edit Kafka CR, resume. | Kafka CR and secrets are updated atomically. |
| 3. | Wait for broker pods to roll twice (trust new CA, then re-issue certs). | All broker pods have new UIDs after both rolling updates. |
| 4. | Verify broker certificates are signed by the custom CA. | Broker certificate issuer DN matches custom CA subject DN. |
| 5. | Produce and consume messages over TLS after switching to custom CA. | Messages are successfully produced and consumed. |
| 6. | Wait for CO cert to be reissued with the custom CA. | CO cert secret generation matches cluster CA cert generation. |
| 7. | Edit the Kafka CR to switch cluster CA back to cert-manager. | Kafka CR is updated. |
| 8. | Wait for broker pods to roll twice (trust new CA, then re-issue certs). | All broker pods have new UIDs after both rolling updates. |
| 9. | Verify broker certificates are signed by the cert-manager CA. | Broker certificate issuer DN matches cert-manager CA subject DN. |
| 10. | Produce and consume messages over TLS after switching back to cert-manager. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)


## testSwitchFromStrimziToCertManager

**Description:** Test verifying switch from Strimzi-managed CA to cert-manager CA. A Kafka cluster is first deployed with the default Strimzi-managed CA, then switched to cert-manager by editing the Kafka CR. The cluster must remain operational and certificates must match the expected CA.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with default Strimzi-managed CA. | Kafka cluster reaches ready state. |
| 2. | Create the cert-manager CA cert Secret and edit the Kafka CR to switch cluster CA to cert-manager. | Kafka CR is updated. |
| 3. | Wait for broker pods to roll twice (trust new CA, then re-issue certs). | All broker pods have new UIDs after both rolling updates. |
| 4. | Wait for CO cert to be reissued with the new cert-manager CA. | CO cert secret generation matches cluster CA cert generation. |
| 5. | Verify broker certificates are signed by the cert-manager CA. | Broker certificate issuer DN matches cert-manager CA subject DN. |
| 6. | Produce and consume messages over TLS after switching to cert-manager. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)

