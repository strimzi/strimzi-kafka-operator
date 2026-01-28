# CustomCaST

**Description:** Test suite for verifying custom CA (Certificate Authority) key-pair manipulation, including replacing cluster and clients key-pairs to invoke renewal process.

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testCustomClusterCaAndClientsCaCertificates

**Description:** This test verifies the functionality of custom cluster and clients CAs. Custom CAs are created and deployed as secrets before Kafka deployment, forcing Kafka to use them instead of generating its own certificate authorities.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create custom cluster CA and clients CA and deploy them as secrets. | Both CA secrets are created. |
| 2. | Deploy Kafka cluster configured to use custom CAs. | Kafka cluster is deployed using the custom CAs. |
| 3. | Verify Kafka broker certificates are signed by the custom cluster CA. | Broker certificates have correct issuer. |
| 4. | Create KafkaUser and verify its certificate is signed by the custom clients CA. | User certificate has correct issuer. |
| 5. | Send and receive messages over TLS. | Messages are successfully produced and consumed. |

**Labels:**

* [security](labels/security.md)


## testReplaceCustomClientsCACertificateValidityToInvokeRenewalProcess

**Description:** This test verifies that changing certificate validity and renewal days triggers renewal of user certificates without renewing the clients CA itself.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create custom clients CA, deploy Kafka cluster, and create KafkaUser. | Kafka cluster and user are deployed with custom clients CA. |
| 2. | Record initial CA and user certificate dates. | Certificate dates are captured. |
| 3. | Pause Kafka reconciliation and update validity and renewal days for clients CA. | CA configuration is updated. |
| 4. | Resume reconciliation and wait for Entity Operator to roll. | Entity Operator rolls to apply new configuration. |
| 5. | Verify CA certificate dates remain unchanged. | Clients CA was not renewed. |
| 6. | Verify user certificates have been renewed with new dates. | User certificates have updated validity dates. |

**Labels:**

* [security](labels/security.md)


## testReplaceCustomClusterCACertificateValidityToInvokeRenewalProcess

**Description:** This test verifies that changing certificate validity and renewal days triggers renewal of cluster certificates without renewing the cluster CA itself.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create custom cluster CA and deploy Kafka cluster with it. | Kafka cluster is deployed with custom cluster CA. |
| 2. | Record initial CA and broker certificate dates. | Certificate dates are captured. |
| 3. | Pause Kafka reconciliation and update validity and renewal days for cluster CA. | CA configuration is updated. |
| 4. | Resume reconciliation and wait for components to roll. | Controllers, brokers, and Entity Operator roll. |
| 5. | Verify CA certificate dates remain unchanged. | Cluster CA was not renewed. |
| 6. | Verify broker certificates have been renewed with new dates. | Broker certificates have updated validity dates. |

**Labels:**

* [security](labels/security.md)


## testReplacingCustomClientsKeyPairToInvokeRenewalProcess

**Description:** This test verifies manual renewal of custom clients CA by replacing the clients CA key-pair and triggering certificate renewal through rolling updates. Only Kafka pods should roll, Entity Operator must not roll.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create custom clients CA and deploy Kafka cluster with it. | Kafka cluster is deployed with custom clients CA. |
| 2. | Generate a new clients CA key-pair. | New clients CA is created. |
| 3. | Replace the old clients CA key-pair with the new one while retaining the old certificate. | CA secrets are updated with new key-pair. |
| 4. | Resume reconciliation and wait for Kafka pods to roll. | Kafka pods roll to trust the new CA and use new certificates. |
| 5. | Verify Entity Operator does not roll. | Entity Operator pods remain unchanged. |
| 6. | Verify message production works with renewed certificates. | Producer successfully sends messages. |

**Labels:**

* [security](labels/security.md)


## testReplacingCustomClusterKeyPairToInvokeRenewalProcess

**Description:** This test verifies manual renewal of custom cluster CA by replacing the cluster CA key-pair and triggering certificate renewal through rolling updates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create custom cluster CA and deploy Kafka cluster with it. | Kafka cluster is deployed with custom cluster CA. |
| 2. | Generate a new cluster CA key-pair. | New cluster CA is created. |
| 3. | Replace the old cluster CA key-pair with the new one while retaining the old certificate. | CA secrets are updated with new key-pair. |
| 4. | Resume reconciliation and wait for rolling updates to complete. | All components roll to trust the new CA and use new certificates. |
| 5. | Verify message production works with renewed certificates. | Producer successfully sends messages. |
| 6. | Remove outdated certificate and trigger manual rolling update using 'strimzi.io/manual-rolling-update' annotation. | Cluster no longer trusts old certificates. |

**Labels:**

* [security](labels/security.md)

