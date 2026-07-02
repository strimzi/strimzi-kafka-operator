# SecurityST

**Description:** Test suite for verifying TLS certificate management, CA renewal and replacement, certificate maintenance windows, ACL authorization, and TLS configuration for Kafka and KafkaConnect components.

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testAclRuleReadAndWrite

**Description:** Test verifying Kafka ACL authorization with separate read and write users over a NodePort TLS listener, ensuring write-only users cannot consume and read-only users cannot produce.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with simple authorization and a NodePort TLS listener. | Kafka cluster is deployed with ACL authorization. |
| 2. | Create a KafkaUser with write-only ACL permissions and send messages. | Messages are sent successfully. |
| 3. | Attempt to receive messages as the write-only user. | Receive fails with GroupAuthorizationException. |
| 4. | Create a KafkaUser with read-only ACL permissions for a specific consumer group. | Read-only user is created. |
| 5. | Receive messages as the read-only user. | Messages are received successfully. |
| 6. | Attempt to send messages as the read-only user. | Send fails with authorization exception. |

**Labels:**

* [security](labels/security.md)


## testAclWithSuperUser

**Description:** Test verifying that a Kafka super user can both produce and consume messages regardless of ACL restrictions, while a non-super user with write-only ACLs cannot consume.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with simple authorization, a NodePort TLS listener, and a configured super user. | Kafka cluster is deployed with super user configured. |
| 2. | Create a KafkaUser configured as super user with write-only ACL and send messages. | Messages are sent successfully. |
| 3. | Receive messages as the super user despite having only write ACL. | Messages are received successfully due to super user privileges. |
| 4. | Create a non-super user with write-only ACL and send messages. | Messages are sent successfully. |
| 5. | Attempt to receive messages as the non-super user. | Receive fails with GroupAuthorizationException. |

**Labels:**

* [security](labels/security.md)


## testAutoRenewAllCaCertsTriggeredByAnno

**Description:** Test verifying that annotating both cluster and clients CA certificate secrets with strimzi.io/force-renew triggers automatic renewal of all CA certificates, causing rolling updates of all components.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter. | Kafka cluster is deployed. |
| 2. | Create KafkaUser and KafkaTopic, produce and consume messages over TLS. | Messages are exchanged successfully. |
| 3. | Annotate both cluster and clients CA certificate secrets with strimzi.io/force-renew. | CA cert renewal is triggered for both CAs. |
| 4. | Wait for rolling updates of all components. | All components roll. |
| 5. | Verify both CA certificates have changed and clients can still consume messages. | New certificates are in use and messaging works. |

**Labels:**

* [security](labels/security.md)


## testAutoRenewCaCertsTriggerByExpiredCertificate

**Description:** Test verifying that a cluster deployed with an already-expired cluster CA certificate automatically triggers certificate renewal, and that messaging works after renewal completes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a Kubernetes secret with an already-expired cluster CA certificate. | Secret with expired certificate is created. |
| 2. | Deploy Kafka cluster which uses the expired certificate. | Kafka cluster is deployed. |
| 3. | Create KafkaUser and KafkaTopic, produce and consume messages over TLS. | Messages are exchanged successfully. |
| 4. | Wait for the expired certificate to be automatically renewed and cluster to stabilize. | Certificate is renewed and cluster is stable. |
| 5. | Produce and consume messages again after renewal. | Messages are exchanged successfully with renewed certificates. |

**Labels:**

* [security](labels/security.md)


## testAutoRenewClientsCaCertsTriggeredByAnno

**Description:** Test verifying that annotating the clients CA certificate secret with strimzi.io/force-renew triggers automatic renewal of the clients CA certificate, causing rolling updates of Kafka brokers without rolling Entity Operator, KafkaExporter, or Cruise Control.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter. | Kafka cluster is deployed. |
| 2. | Create KafkaUser and KafkaTopic, produce and consume messages over TLS. | Messages are exchanged successfully. |
| 3. | Annotate the clients CA certificate secret with strimzi.io/force-renew. | CA cert renewal is triggered. |
| 4. | Wait for Kafka rolling update; verify Entity Operator, KafkaExporter, and Cruise Control do not roll. | Only Kafka rolls. |
| 5. | Verify the CA certificate has changed and clients can still consume messages. | New certificate is in use and messaging works. |

**Labels:**

* [security](labels/security.md)


## testAutoRenewClusterCaCertsTriggeredByAnno

**Description:** Test verifying that annotating the cluster CA certificate secret with strimzi.io/force-renew triggers automatic renewal of the cluster CA certificate, causing rolling updates of Kafka brokers, controllers, Entity Operator, KafkaExporter, and Cruise Control.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter. | Kafka cluster is deployed. |
| 2. | Create KafkaUser and KafkaTopic, produce and consume messages over TLS. | Messages are exchanged successfully. |
| 3. | Annotate the cluster CA certificate secret with strimzi.io/force-renew. | CA cert renewal is triggered. |
| 4. | Wait for rolling updates of Kafka, Entity Operator, KafkaExporter, and Cruise Control. | All components roll. |
| 5. | Verify the CA certificate has changed and clients can still consume messages. | New certificate is in use and messaging works. |

**Labels:**

* [security](labels/security.md)


## testAutoReplaceAllCaKeysTriggeredByAnno

**Description:** Test verifying that annotating both cluster and clients CA key secrets with strimzi.io/force-replace triggers automatic replacement of all CA key pairs, causing multiple rolling updates of all components.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter. | Kafka cluster is deployed. |
| 2. | Create KafkaUser and KafkaTopic, produce and consume messages over TLS. | Messages are exchanged successfully. |
| 3. | Annotate both cluster and clients CA key secrets with strimzi.io/force-replace. | CA key replacement is triggered for both CAs. |
| 4. | Wait for multiple rolling updates of all components. | All components complete multiple rolling updates. |
| 5. | Verify both CA keys have changed and clients can still consume messages. | New keys are in use and messaging works. |

**Labels:**

* [security](labels/security.md)


## testAutoReplaceClientsCaKeysTriggeredByAnno

**Description:** Test verifying that annotating the clients CA key secret with strimzi.io/force-replace triggers automatic replacement of the clients CA key pair, causing a rolling update of Kafka brokers without rolling Entity Operator, KafkaExporter, or Cruise Control.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter. | Kafka cluster is deployed. |
| 2. | Create KafkaUser and KafkaTopic, produce and consume messages over TLS. | Messages are exchanged successfully. |
| 3. | Annotate the clients CA key secret with strimzi.io/force-replace. | CA key replacement is triggered. |
| 4. | Wait for Kafka rolling update; verify Entity Operator, KafkaExporter, and Cruise Control do not roll. | Only Kafka rolls. |
| 5. | Verify the CA key has changed and clients can still consume messages. | New key is in use and messaging works. |

**Labels:**

* [security](labels/security.md)


## testAutoReplaceClusterCaKeysTriggeredByAnno

**Description:** Test verifying that annotating the cluster CA key secret with strimzi.io/force-replace triggers automatic replacement of the cluster CA key pair, causing multiple rolling updates including removal of the old cluster CA certificate.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with TLS listener, Cruise Control, and KafkaExporter. | Kafka cluster is deployed. |
| 2. | Create KafkaUser and KafkaTopic, produce and consume messages over TLS. | Messages are exchanged successfully. |
| 3. | Annotate the cluster CA key secret with strimzi.io/force-replace. | CA key replacement is triggered. |
| 4. | Wait for multiple rolling updates of all components including an additional roll for old certificate removal. | All components complete multiple rolling updates. |
| 5. | Verify the CA key has changed and clients can still consume messages. | New key is in use and messaging works. |

**Labels:**

* [security](labels/security.md)


## testBrokerCertificatesIncludeFullCaChain

**Description:** Test verifying that broker certificate secrets contain the full CA chain (broker cert + CA cert), with correct subject and issuer fields, and that TLS messaging works with the full chain.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster. | Kafka cluster is deployed. |
| 2. | Retrieve the broker certificate chain from the broker secret. | Certificate chain is retrieved. |
| 3. | Verify the chain contains exactly 2 certificates (broker cert and CA cert) with correct subject and issuer fields. | Certificate chain is valid. |
| 4. | Create KafkaUser and KafkaTopic, produce and consume messages over TLS. | Messages are exchanged successfully. |

**Labels:**

* [security](labels/security.md)


## testCaRenewalBreakInMiddle

**Description:** Test verifying that CA certificate renewal completes successfully even when a broker Pod becomes stuck in Pending state during the rolling update, simulating a break-in-the-middle scenario.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with short CA validity and renewal days and replication factor of 3. | Kafka cluster is deployed with short-lived certificates. |
| 2. | Create KafkaUser and KafkaTopic, produce and consume messages over TLS. | Messages are exchanged successfully. |
| 3. | Set impossibly high CPU resource requirements on brokers and update CA configuration to trigger renewal. | Broker Pods enter Pending state during rolling update. |
| 4. | Verify a consumer can still read previously produced messages. | Consumer reads messages from available replicas. |
| 5. | Fix CPU resource requirements to allow Pods to schedule. | Pods become schedulable. |
| 6. | Wait for certificate renewal and rolling updates to complete. | All components roll and certificates are renewed. |
| 7. | Produce and consume messages on a new topic with renewed certificates. | Messages are exchanged successfully with new certificates. |

**Labels:**

* [security](labels/security.md)


## testCertRegeneratedAfterInternalCAisDeleted

**Description:** Test verifying that deleting internal CA certificate secrets triggers automatic regeneration of the CA certificates with new certificate data, followed by a rolling update.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster and create KafkaUser and KafkaTopic. | All resources are deployed. |
| 2. | Verify CA certificate secrets exist. | Secrets are present with certificate data. |
| 3. | Delete all CA certificate secrets. | Secrets are deleted. |
| 4. | Wait for Kafka rolling update and secret regeneration. | Pods roll and secrets are recreated. |
| 5. | Verify regenerated certificates have different data than the originals. | New certificates differ from deleted ones. |
| 6. | Produce and consume messages over TLS with regenerated certificates. | Messages are exchanged successfully. |

**Labels:**

* [security](labels/security.md)


## testCertRenewalInMaintenanceTimeWindow

**Description:** Test verifying that CA certificate renewal is deferred until a configured maintenance time window, and that rolling updates only occur within the window.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with a maintenance time window set 15 minutes in the future and short CA validity and renewal days. | Kafka cluster is deployed with maintenance window configured. |
| 2. | Create KafkaUser and KafkaTopic. | Resources are created. |
| 3. | Update CA validity and renewal days to trigger renewal. | CA configuration is updated. |
| 4. | Verify that CA certificate generation remains unchanged and no rolling update occurs outside the maintenance window. | No renewal or rolling update happens. |
| 5. | Add a new maintenance window starting at the current time. | Maintenance window is updated to start now. |
| 6. | Wait for rolling update to occur within the maintenance window. | Kafka rolls within the maintenance window. |
| 7. | Verify CA certificate generations have incremented and KafkaUser certificate has been renewed. | Certificates are renewed. |
| 8. | Produce and consume messages over TLS with renewed certificates. | Messages are exchanged successfully. |

**Labels:**

* [security](labels/security.md)


## testClientsCACertRenew

**Description:** Test verifying that changing clients CA validity and renewal days triggers certificate renewal via a rolling update, resulting in updated certificate dates for both the CA and KafkaUser certificates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with short clients CA validity and renewal periods. | Kafka cluster is deployed. |
| 2. | Create a KafkaUser with TLS authentication. | KafkaUser is created. |
| 3. | Record initial CA and user certificate start and end dates. | Certificate dates are captured. |
| 4. | Update clients CA validity to 200 days and renewal to 150 days. | CA configuration is updated. |
| 5. | Wait for rolling updates of brokers and Entity Operator. | Components roll. |
| 6. | Verify CA and user certificate end dates have been extended. | Certificate dates are renewed with longer validity. |

**Labels:**

* [security](labels/security.md)


## testClusterCACertRenew

**Description:** Test verifying that changing cluster CA validity and renewal days triggers certificate renewal via a rolling update, resulting in updated certificate dates for both the CA and broker certificates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with short cluster CA validity and renewal periods. | Kafka cluster is deployed. |
| 2. | Record initial CA and broker certificate start and end dates. | Certificate dates are captured. |
| 3. | Update cluster CA validity to 200 days and renewal to 150 days. | CA configuration is updated. |
| 4. | Wait for rolling updates of controllers, brokers, and Entity Operator. | All components roll. |
| 5. | Verify CA and broker certificate end dates have been extended. | Certificate dates are renewed with longer validity. |

**Labels:**

* [security](labels/security.md)


## testKafkaAndKafkaConnectCipherSuites

**Description:** Test verifying that KafkaConnect cannot connect to a Kafka cluster when configured with an incompatible cipher suite, and recovers when updated to use a matching cipher suite.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster configured with TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 cipher suite. | Kafka cluster is deployed with specific cipher suite. |
| 2. | Deploy KafkaConnect configured with TLS_DHE_RSA_WITH_AES_128_GCM_SHA256 cipher suite. | KafkaConnect becomes NotReady due to cipher suite mismatch. |
| 3. | Update KafkaConnect configuration to use the same cipher suite as Kafka. | KafkaConnect configuration is updated. |
| 4. | Verify KafkaConnect becomes Ready and is stable. | KafkaConnect is Ready and stable. |

**Labels:**

* [security](labels/security.md)


## testKafkaAndKafkaConnectTlsVersion

**Description:** Test verifying that KafkaConnect cannot connect to a Kafka cluster when configured with an incompatible TLS version, and recovers when updated to use a matching TLS version.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster configured to support only TLSv1.2. | Kafka cluster is deployed with TLSv1.2. |
| 2. | Deploy KafkaConnect configured with TLSv1. | KafkaConnect becomes NotReady due to TLS version mismatch. |
| 3. | Update KafkaConnect configuration to use TLSv1.2. | KafkaConnect configuration is updated. |
| 4. | Verify KafkaConnect becomes Ready and is stable. | KafkaConnect is Ready and stable. |

**Labels:**

* [security](labels/security.md)


## testOwnerReferenceOfCASecrets

**Description:** Test verifying that CA secrets with generateSecretOwnerReference set to false persist after Kafka deletion, and CA secrets with generateSecretOwnerReference set to true are automatically deleted with the Kafka cluster.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with generateSecretOwnerReference set to false for both cluster and clients CAs. | Kafka cluster is deployed. |
| 2. | Delete the Kafka cluster. | Kafka cluster is deleted. |
| 3. | Verify that CA secrets still exist after Kafka deletion. | CA secrets persist. |
| 4. | Delete the persisted CA secrets manually. | Secrets are deleted. |
| 5. | Deploy a new Kafka cluster with generateSecretOwnerReference set to true. | New Kafka cluster is deployed. |
| 6. | Delete the Kafka cluster. | Kafka cluster is deleted. |
| 7. | Verify that CA secrets are automatically deleted with the Kafka cluster. | CA secrets are deleted. |

**Labels:**

* [security](labels/security.md)


## testTlsHostnameVerificationWithKafkaConnect

**Description:** Test verifying TLS hostname verification behavior with KafkaConnect, where connecting via IP address fails with default hostname verification and succeeds when ssl.endpoint.identification.algorithm is set to empty.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster and get the bootstrap service IP address. | Kafka is deployed and IP is retrieved. |
| 2. | Deploy KafkaConnect configured to connect via IP address without disabling hostname verification. | KafkaConnect enters CrashLoopBackOff due to hostname verification failure. |
| 3. | Update KafkaConnect configuration to set ssl.endpoint.identification.algorithm to empty. | KafkaConnect configuration is updated. |
| 4. | Verify KafkaConnect recovers and becomes Ready. | KafkaConnect is in Ready state. |

**Labels:**

* [security](labels/security.md)

