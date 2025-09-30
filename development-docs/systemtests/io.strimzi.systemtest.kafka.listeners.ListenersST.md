# ListenersST

**Description:** This class demonstrates various tests for Kafka listeners using different authentication mechanisms.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Install the Cluster Operator with default settings. | Cluster Operator is installed successfully. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testAdvertisedHostNamesAppearsInBrokerCerts

**Description:** Verify that advertised hostnames appear correctly in broker certificates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Define internal and external advertised hostnames and ports | Hostnames and ports are defined and listed |
| 2. | Create broker configurations with advertised hostnames and ports | Broker configurations are created |
| 3. | Deploy resources with Wait function and create Kafka instance | Resources and Kafka instance are successfully created |
| 4. | Retrieve broker certificates from Kubernetes secrets | Certificates are retrieved correctly from secrets |
| 5. | Validate that each broker's certificate contains the expected internal and external advertised hostnames | Certificates contain the correct advertised hostnames |

**Labels:**

* [kafka](labels/kafka.md)


## testCertificateWithNonExistingDataCrt

**Description:** Test checking behavior when Kafka is configured with a non-existing certificate in the TLS listener.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Define non-existing certificate name. | Non-existing certificate name is defined. |
| 2. | Create a custom secret for Kafka with the defined certificate. | Custom secret created successfully. |
| 3. | Create KafkaNodePool resources. | KafkaNodePool resources created. |
| 4. | Create Kafka cluster with ephemeral storage and the non-existing certificate. | Kafka cluster creation initiated. |
| 5. | Wait for controller pods to be ready if in non-KRaft mode. | Controller pods are ready. |
| 6. | Wait until Kafka status message indicates missing certificate. | Error message about missing certificate is found in Kafka status condition. |

**Labels:**

* [kafka](labels/kafka.md)


## testCertificateWithNonExistingDataKey

**Description:** Test verifies that a Kafka cluster correctly identifies and reports the absence of a specified custom certificate private key.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Define the non-existing certificate key. | The non-existing certificate key string is defined. |
| 2. | Create a custom secret with a certificate for Kafka server. | Custom secret is created in the namespace. |
| 3. | Create broker and controller KafkaNodePools. | Resources are created and ready. |
| 4. | Deploy a Kafka cluster with a listener using the custom secret and non-existing key. | Deployment initiated without waiting for the resources to be ready. |
| 5. | If not in KRaft mode, wait for controller pods to be ready. | Controller pods are in ready state (if applicable). |
| 6. | Check Kafka status condition for custom certificate error message. | Error message indicating the missing custom certificate private key is present in Kafka status conditions. |

**Labels:**

* [kafka](labels/kafka.md)


## testClusterIp

**Description:** Test verifies the functionality of Kafka with a cluster IP listener.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create the Kafka broker and controller pools | Kafka broker and controller pools are created |
| 2. | Create the Kafka cluster with a cluster IP listener | Kafka cluster with cluster IP listener is created |
| 3. | Retrieve the cluster IP bootstrap address | Cluster IP bootstrap address is correctly retrieved |
| 4. | Deploy Kafka clients | Kafka clients are deployed successfully |
| 5. | Wait for Kafka clients to succeed | Kafka clients successfully produce and consume messages |

**Labels:**

* [kafka](labels/kafka.md)


## testClusterIpTls

**Description:** This test validates the creation of Kafka resources with TLS authentication, ensuring proper setup and functionality of the Kafka cluster in a parallel namespace.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create ephemeral Kafka cluster with TLS enabled on ClusterIP listener | Kafka cluster is created with TLS enabled listener on port 9103 |
| 2. | Create Kafka user with TLS authentication | Kafka user is created successfully |
| 3. | Retrieve the ClusterIP bootstrap address for the Kafka cluster | Bootstrap address for the Kafka cluster is retrieved |
| 4. | Instantiate TLS Kafka Clients (producer and consumer) | TLS Kafka clients are instantiated successfully |
| 5. | Wait for the Kafka Clients to complete their tasks and verify success | Kafka Clients complete their tasks successfully |

**Labels:**

* [kafka](labels/kafka.md)


## testCustomCertLoadBalancerAndTlsRollingUpdate

**Description:** This test verifies the behavior of Kafka with custom certificates for load balancer and TLS rolling updates.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create custom secrets for Kafka clusters | Secrets created and available in namespace |
| 2. | Deploy Kafka resources with load balancer and internal TLS listener | Kafka resources deployed with respective configurations |
| 3. | Create Kafka user and retrieve certificates | Kafka user created and certificates retrieved from Kafka status and secrets |
| 4. | Compare Kafka certificates with secret certificates | Certificates from Kafka status and secrets match |
| 5. | Verify message production and consumption using an external Kafka client | Messages successfully produced and consumed over SSL |
| 6. | Trigger and verify TLS rolling update | TLS rolling update completed successfully |
| 7. | Repeat certificate verification steps after rolling update | Certificates from Kafka status and secrets match post update |
| 8. | Repeatedly produce and consume messages to ensure Kafka stability | Messages successfully produced and consumed, ensuring stability |
| 9. | Revert the certificate updates and verify Kafka status | Certificates reverted and verified, Kafka operates normally |

**Labels:**

* [kafka](labels/kafka.md)


## testCustomCertNodePortAndTlsRollingUpdate

**Description:** Test verifies custom certificates with NodePort and rolling update in Kafka.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate root and intermediate certificates | Certificates are generated successfully |
| 2. | Generate end-entity certificates | End-entity certificates are generated successfully |
| 3. | Create custom secrets with generated certificates | Secrets are created in Kubernetes |
| 4. | Deploy Kafka cluster with custom NodePort and TLS settings | Kafka cluster is deployed and running |
| 5. | Verify messages sent and received through external Kafka client | Messages are produced and consumed successfully |
| 6. | Perform rolling update and update certificates in custom secrets | Rolling update is performed and certificates are updated |
| 7. | Verify messages sent and received after rolling update | Messages are produced and consumed successfully after update |
| 8. | Restore default certificate configuration and perform rolling update | Default certificates are restored and rolling update is completed |
| 9. | Verify messages sent and received with restored configuration | Messages are produced and consumed successfully with restored configuration |

**Labels:**

* [kafka](labels/kafka.md)


## testCustomCertRouteAndTlsRollingUpdate

**Description:** This test verifies the custom certificate handling and TLS rolling update mechanisms for Kafka brokers using OpenShift-specific configurations.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create various certificate chains and export them to PEM files | Certificates are created and exported successfully |
| 2. | Create custom secrets with the generated certificates | Secrets are created in the specified namespace |
| 3. | Deploy Kafka cluster and TLS user with specified configurations | Kafka cluster and TLS user are deployed successfully |
| 4. | Verify certificates in KafkaStatus match those in the secrets | Certificates are verified to match |
| 5. | Use external Kafka client to produce and consume messages | Messages are produced and consumed successfully |
| 6. | Update Kafka listeners with new certificates and perform rolling update | Kafka cluster rolls out successfully with updated certificates |
| 7. | Verify certificates in KafkaStatus match after update | Certificates are verified to match after the update |
| 8. | Repeat message production and consumption with updated certificates | Messages are produced and consumed successfully with new certificates |

**Labels:**

* [kafka](labels/kafka.md)


## testCustomChainCertificatesForLoadBalancer

**Description:** Verifies custom certificate chain configuration for Kafka load balancer, ensuring proper secret creation, resource setup, and message sending/receiving functionality.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create custom secrets for certificate chains and root CA | Secrets are created successfully |
| 2. | Deploy Kafka broker and controller pools with custom certificates | Kafka pools are deployed without issues |
| 3. | Deploy Kafka cluster with custom listener configurations | Kafka cluster is deployed with custom listener configurations |
| 4. | Set up Kafka topic and user | Kafka topic and user are created successfully |
| 5. | Verify message production and consumption via external Kafka client with TLS | Messages are produced and consumed successfully |
| 6. | Set up Kafka clients for further messaging operations | Kafka clients are set up without issues |
| 7. | Produce messages using Kafka producer | Messages are produced successfully |
| 8. | Consume messages using Kafka consumer | Messages are consumed successfully |

**Labels:**

* [kafka](labels/kafka.md)


## testCustomChainCertificatesForNodePort

**Description:** Test verifies the custom chain certificates configuration for Kafka NodePort listener.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate custom root CA and intermediate certificates. | Root and intermediate certificates are generated. |
| 2. | Generate end entity certificates using intermediate CA. | End entity certificates are generated. |
| 3. | Export certificates to PEM files. | Certificates are exported to PEM files. |
| 4. | Create Kubernetes secrets with the custom certificates. | Custom certificate secrets are created. |
| 5. | Deploy Kafka cluster with NodePort listener using the custom certificates. | Kafka cluster is deployed successfully. |
| 6. | Create a Kafka user with TLS authentication. | Kafka user is created. |
| 7. | Verify message production and consumption with external Kafka client. | Messages are produced and consumed successfully. |
| 8. | Verify message production with internal Kafka client. | Messages are produced successfully. |
| 9. | Verify message consumption with internal Kafka client. | Messages are consumed successfully. |

**Labels:**

* [kafka](labels/kafka.md)


## testCustomChainCertificatesForRoute

**Description:** Test to verify custom chain certificates for a Kafka Route.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate root and intermediate certificates | Root and intermediate CA keys are generated |
| 2. | Create cluster custom certificate chain and root CA secrets | Custom certificate chain and root CA secrets are created in OpenShift |
| 3. | Create Kafka cluster with custom certificates | Kafka cluster is deployed with custom certificates for internal and external listeners |
| 4. | Create Kafka user | Kafka user with TLS authentication is created |
| 5. | Verify message production and consumption with external Kafka client | Messages are produced and consumed successfully using the external Kafka client |
| 6. | Create Kafka clients for internal message production and consumption | Internal Kafka clients are created and configured with TLS authentication |
| 7. | Verify internal message production with Kafka client | Messages are produced successfully using the internal Kafka client |
| 8. | Verify internal message consumption with Kafka client | Messages are consumed successfully using the internal Kafka client |

**Labels:**

* [kafka](labels/kafka.md)


## testCustomSoloCertificatesForLoadBalancer

**Description:** Test verifying custom solo certificates for load balancer in a Kafka cluster.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create custom secret | Custom secret is created with the specified certificate and key |
| 2. | Create Kafka resources with KafkaNodePools | Kafka brokers and controller pools are created and configured |
| 3. | Create Kafka cluster with listeners | Kafka cluster is created with internal and load balancer listeners using the custom certificates |
| 4. | Create TLS user | TLS user is created |
| 5. | Verify produced and consumed messages via external client | Messages are successfully produced and consumed using the custom certificates |
| 6. | Create and verify TLS producer client | TLS producer client is created and verified for success |
| 7. | Create and verify TLS consumer client | TLS consumer client is created and verified for success |

**Labels:**

* [kafka](labels/kafka.md)


## testCustomSoloCertificatesForNodePort

**Description:** Test custom certificates in Kafka listeners, specifically for the NodePort type.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate Root CA certificate and key | Root CA certificate and key are generated |
| 2. | Generate Intermediate CA certificate and key using Root CA | Intermediate CA certificate and key are generated |
| 3. | Generate Kafka Broker certificate and key using Intermediate CA | Broker certificate and key are generated |
| 4. | Export generated certificates and keys to PEM files | PEM files are created with certificates and keys |
| 5. | Create custom secret with the PEM files | Custom secret is created within the required namespace |
| 6. | Deploy and wait for Kafka cluster resources with custom certificates | Kafka cluster is deployed successfully with custom certificates |
| 7. | Create and wait for TLS KafkaUser | TLS KafkaUser is created successfully |
| 8. | Produce and consume messages using ExternalKafkaClient | Messages are successfully produced and consumed |
| 9. | Produce and consume messages using internal TLS client | Messages are successfully produced and consumed with internal TLS client |

**Labels:**

* [kafka](labels/kafka.md)


## testCustomSoloCertificatesForRoute

**Description:** Test custom solo certificates for Kafka route and client communication.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate root CA certificate and key | Root CA certificate and key are generated |
| 2. | Generate intermediate CA certificate and key | Intermediate CA certificate and key are generated |
| 3. | Generate end-entity certificate and key for Strimzi | End-entity certificate and key for Strimzi are generated |
| 4. | Export certificates and keys to PEM files | Certificates and keys are exported to PEM files |
| 5. | Create custom secret with certificates and keys | Custom secret is created in the namespace with certificates and keys |
| 6. | Deploy Kafka cluster with custom certificates | Kafka cluster is deployed with custom certificates |
| 7. | Create TLS Kafka user | TLS Kafka user is created |
| 8. | Verify client communication using external Kafka client | Messages are successfully produced and consumed using external Kafka client |
| 9. | Deploy Kafka clients with custom certificates | Kafka clients are deployed with custom certificates |
| 10. | Verify client communication using internal Kafka client | Messages are successfully produced and consumed using internal Kafka client |

**Labels:**

* [kafka](labels/kafka.md)


## testLoadBalancer

**Description:** Test verifying load balancer functionality with external clients.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka broker and controller KafkaNodePools. | Broker and controller KafkaNodePools are created |
| 2. | Create Kafka cluster with ephemeral storage and load balancer listener | Kafka cluster is created with the specified configuration |
| 3. | Wait until the load balancer address is reachable | Address is reachable |
| 4. | Configure external Kafka client and send messages | Messages are sent successfully |
| 5. | Verify that messages are correctly produced and consumed | Messages are produced and consumed as expected |

**Labels:**

* [kafka](labels/kafka.md)


## testLoadBalancerTls

**Description:** Test validating the TLS connection through a Kafka LoadBalancer.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create and configure KafkaNodePools | KafkaNodePools for brokers and controllers are created |
| 2. | Create and configure Kafka cluster with TLS listener | Kafka cluster with TLS enabled LoadBalancer listener is created |
| 3. | Create and configure Kafka user with TLS authentication | Kafka user with TLS authentication is created |
| 4. | Wait for the LoadBalancer address to be reachable | LoadBalancer address becomes reachable |
| 5. | Send and receive messages using external Kafka client | Messages are successfully produced and consumed over the TLS connection |

**Labels:**

* [kafka](labels/kafka.md)


## testMessagesTlsScramShaWithPredefinedPassword

**Description:** Validates that messages can be sent and received over TLS with SCRAM-SHA authentication using a predefined password, and that the password can be updated and still be functional.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create and encode the initial password | Initial password is encoded |
| 2. | Create and encode the secondary password | Secondary password is encoded |
| 3. | Create a secret in Kubernetes with the initial password | Secret is created and contains the initial password |
| 4. | Verify the password in the secret | Password in the secret is verified to be correct |
| 5. | Create a KafkaUser with SCRAM-SHA authentication using the secret | KafkaUser is created with correct authentication settings |
| 6. | Create Kafka cluster and topic with SCRAM-SHA authentication | Kafka cluster and topic are created correctly |
| 7. | Produce and consume messages using TLS and SCRAM-SHA | Messages are successfully transmitted and received |
| 8. | Update the secret with the secondary password | Secret is updated with the new password |
| 9. | Wait for the user password change to take effect | Password change is detected and applied |
| 10. | Produce and consume messages with the updated password | Messages are successfully transmitted and received with the new password |

**Labels:**

* [kafka](labels/kafka.md)


## testNodePort

**Description:** Test checking the functionality of Kafka cluster with NodePort external listener configurations.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create resource with Kafka broker pool and controller pool. | Resources with Kafka pools are created successfully. |
| 2. | Create Kafka cluster with NodePort and TLS listeners. | Kafka cluster is set up with the specified listeners. |
| 3. | Create ExternalKafkaClient and verify message production and consumption. | Messages are produced and consumed successfully. |
| 4. | Check Kafka status for proper listener addresses. | Listener addresses in Kafka status are validated successfully. |
| 5. | Check ClusterRoleBinding annotations and labels in Kafka cluster. | Annotations and labels match the expected values. |

**Labels:**

* [kafka](labels/kafka.md)


## testNodePortTls

**Description:** Test the NodePort TLS functionality for Kafka brokers in a Kubernetes environment.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka broker and controller KafkaNodePools. | Broker and controller KafkaNodePools are created |
| 2. | Deploy Kafka cluster with NodePort listener and TLS enabled | Kafka cluster is deployed with NodePort listener and TLS |
| 3. | Create a Kafka topic | Kafka topic is created |
| 4. | Create a Kafka user with TLS authentication | Kafka user with TLS authentication is created |
| 5. | Configure external Kafka client and send and receive messages using TLS | External Kafka client sends and receives messages using TLS successfully |

**Labels:**

* [kafka](labels/kafka.md)


## testNonExistingCustomCertificate

**Description:** Test for verifying non-existing custom certificate handling by creating necessary resources and ensuring correct error message check.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create necessary KafkaNodePools | KafkaNodePools are created and initialized |
| 2. | Create Kafka cluster with a listener using non-existing certificate | Kafka cluster resource is initialized with non-existing TLS certificate |
| 3. | Wait for pods to be ready if not in KRaft mode | Pods are ready |
| 4. | Wait for Kafka status condition message indicating the non-existing secret | Correct error message regarding the non-existing secret appears |

**Labels:**

* [kafka](labels/kafka.md)


## testOverrideNodePortConfiguration

**Description:** Test verifying that NodePort configuration can be overridden for Kafka brokers and bootstrap service.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka broker and controller pools using resource manager. | Kafka broker and controller pools are created successfully. |
| 2. | Deploy Kafka cluster with overridden NodePort configuration for brokers and bootstrap. | Kafka cluster is deployed with specified NodePort values. |
| 3. | Verify that the bootstrap service NodePort matches the configured value. | Bootstrap NodePort matches the configured value of 32100. |
| 4. | Verify that the broker service NodePort matches the configured value. | Broker NodePort matches the configured value of 32000. |
| 5. | Produce and consume messages using an external Kafka client. | Messages are produced and consumed successfully using the external client. |

**Labels:**

* [kafka](labels/kafka.md)


## testSendMessagesCustomListenerTlsCustomization

**Description:** Test custom listener configured with TLS and with user defined configuration for ssl.* fields.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate 2 custom root CA and 2 user certificates (each user cert signed by different root CA) for mTLS configured for custom listener. | Secrets with generated CA and user certs are available. |
| 2. | Create a broker and controller KafkaNodePools. | KafkaNodePools are created. |
| 3. | Create a Kafka cluster with custom listener using TLS authentication and both custom CA certs defined via 'ssl.truststore.location'. Kafka also contains simple authorization config with superuser 'pepa'. | Kafka cluster with custom listener is ready. |
| 4. | Create a Kafka topic and Kafka TLS users wit respective ACL configuration. | Kafka topic and users are created. |
| 5. | Transmit messages over TLS to custom listener with user-1 certs generated during the firs step. | Messages are transmitted successfully. |
| 6. | Transmit messages over TLS to custom listener with Strimzi certs generated for KafkaUser. | Producer/consumer time-outed due to wrong certificate used. |
| 7. | Transmit messages over TLS to custom listener with user-2 certs generated during the firs step. | Messages are transmitted successfully. |
| 8. | Remove 'ssl.principal.mapping.rules' configuration from Kafka's listener. | Rolling update of Kafka brokers is performed successfully. |
| 9. | Transmit messages over TLS to custom listener with user-1 certs generated during the firs step. | Producer/consumer time-outed due to not-authorized error as KafkaUser CN doesn't match. |

**Labels:**

* [kafka](labels/kafka.md)


## testSendMessagesCustomListenerTlsScramSha

**Description:** Test custom listener configured with scram SHA authentication and TLS.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a Kafka cluster with broker and controller KafkaNodePools. | Kafka cluster is created with KafkaNodePools. |
| 2. | Create a Kafka cluster with custom listener using TLS and SCRAM-SHA authentication. | Kafka cluster with custom listener is ready. |
| 3. | Create a Kafka topic and SCRAM-SHA user. | Kafka topic and user are created. |
| 4. | Transmit messages over TLS using SCRAM-SHA authentication. | Messages are transmitted successfully. |

**Labels:**

* [kafka](labels/kafka.md)


## testSendMessagesPlainAnonymous

**Description:** Test sending messages over plain transport, without auth

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka resources with wait. | Kafka broker, controller, and topic are created. |
| 2. | Log transmission message. | Transmission message is logged. |
| 3. | Produce and consume messages with plain clients. | Messages are successfully produced and consumed. |
| 4. | Validate Kafka service discovery annotation. | The discovery annotation is validated successfully. |

**Labels:**

* [kafka](labels/kafka.md)


## testSendMessagesPlainScramSha

**Description:** Test sending messages over plain transport using scram sha auth.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka brokers and controllers. | Kafka brokers and controllers are created. |
| 2. | Enable Kafka with plain listener disabled and scram sha auth. | Kafka instance with scram sha auth is enabled on a specified listener. |
| 3. | Set up topic and user. | Kafka topic and Kafka user are set up with scram sha auth credentials. |
| 4. | Check logs in broker pod for authentication. | Logs show that scram sha authentication succeeded. |
| 5. | Send messages over plain transport using scram sha authentication. | Messages are successfully sent over plain transport using scram sha auth. |
| 6. | Verify service discovery annotation. | Service discovery annotation is checked and validated. |

**Labels:**

* [kafka](labels/kafka.md)


## testSendMessagesTlsAuthenticated

**Description:** Test sending messages over tls transport using mutual tls auth.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | CreateKafkaNodePool resources. | Persistent storage KafkaNodePools are created. |
| 2. | Disable plain listener and enable tls listener in Kafka resource. | Kafka with plain listener disabled and tls listener enabled is created. |
| 3. | Create Kafka topic and user. | Kafka topic and tls user are created. |
| 4. | Configure and deploy Kafka clients. | Kafka clients producer and consumer with tls are deployed. |
| 5. | Wait for clients to successfully send and receive messages. | Clients successfully send and receive messages over tls. |
| 6. | Assert that the service discovery contains expected info. | Service discovery matches expected info. |

**Labels:**

* [kafka](labels/kafka.md)


## testSendMessagesTlsScramSha

**Description:** Test sending messages over TLS transport using SCRAM-SHA authentication.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create resources for KafkaNodePools. | KafkaNodePools are created. |
| 2. | Create Kafka cluster with SCRAM-SHA-512 authentication. | Kafka cluster is created with SCRAM-SHA authentication. |
| 3. | Create Kafka topic and user. | Kafka topic and user are created. |
| 4. | Transmit messages over TLS using SCRAM-SHA. | Messages are successfully transmitted. |
| 5. | Check if generated password has the expected length. | Password length is as expected. |
| 6. | Verify Kafka service discovery annotation. | Service discovery annotation is as expected. |

**Labels:**

* [kafka](labels/kafka.md)

