# CustomCaChainST

**Description:** Test suite for verifying custom CA chain trust establishment, user certificate authentication with multi-stage CA hierarchies, and KafkaConnect trust chain configurations.

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testCustomCaTrustChainOnInternalPort

**Description:** Verifies trust chain validation on the internal listener (port 9091) used for inter-broker communication. Clients connect directly to the internal port with different trust secrets to verify that PKIX path building works correctly.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate a custom CA chain: Root -> Intermediate -> Leaf. | CA chain is generated. |
| 2. | Deploy the full custom CA chain as Cluster CA and Clients CA secrets. | CA secrets are deployed. |
| 3. | Deploy Kafka cluster with custom CAs (generateCertificateAuthority: false) so that broker certificates are signed by the Leaf CA. | Kafka cluster is ready. |
| 4. | Create a NetworkPolicy allowing all ingress to Kafka broker pods so that test clients can reach port 9091. | NetworkPolicy is created. |
| 5. | Generate a client certificate signed by the Cluster CA Leaf and create a KafkaTopic. | Client certificate and KafkaTopic are created. |
| 6. | Create trust secrets with different chain levels: Root + Intermediate + Leaf, Root + Intermediate, Root only, Intermediate only, Leaf only. | Trust secrets are created. |
| 7. | For each trust secret, verify that clients can produce and consume messages on port 9091. | All five trust configurations succeed. |
| 8. | Create a trust secret with only a foreign Root CA and verify that clients cannot connect on port 9091. | Producer/consumer time out due to trust failure. |

**Labels:**

* [security](labels/security.md)


## testKafkaConnectTrustWithCustomCaChain

**Description:** Verifies that KafkaConnect properly establishes trust when connecting to Kafka using various custom CA configurations.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate a custom CA chain: Root -> Intermediate -> Leaf. | CA chain is generated. |
| 2. | Generate a Subleaf CA signed by Leaf (Root -> Intermediate -> Leaf -> Subleaf). | Subleaf CA is generated. |
| 3. | Deploy the full custom CA chain as Cluster CA secrets. | Cluster CA secrets are deployed. |
| 4. | Deploy Kafka cluster with custom Cluster CA so that broker certificates are signed by the Leaf CA. | Kafka cluster is ready. |
| 5. | Create six trust secrets for KafkaConnect: Root + Intermediate + Leaf, Root + Intermediate, Root only, Intermediate only, Leaf only, and Subleaf chain. | Trust secrets are created. |
| 6. | For each valid trust secret (Root + Intermediate + Leaf, Root + Intermediate, Root only, Intermediate only, Leaf only), deploy KafkaConnect and verify it becomes ready. | KafkaConnect connects successfully. |
| 7. | Deploy KafkaConnect with the Subleaf trust secret and verify it does not become ready. | KafkaConnect fails to connect. |

**Labels:**

* [security](labels/security.md)


## testMultistageCustomCaTrustChainEstablishment

**Description:** Verifies that clients can establish trust based on any issuer from the custom CA chain the Leaf CA, Intermediate CA, or the Root CA when the broker presents the full certificate chain. A foreign CA that is not part of the chain should fail to establish trust.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate a custom CA chain: Root -> Intermediate -> Leaf. | CA chain is generated. |
| 2. | Deploy the full chain as Cluster CA and Clients CA secrets. | CA secrets are deployed. |
| 3. | Deploy Kafka cluster with custom CAs. | Kafka cluster is ready. |
| 4. | Verify the broker certificate chain contains 4 certificates and validate the issuer chain: broker cert -> Leaf CA -> Intermediate CA -> Root CA (self-signed). | Chain contains 4 certs with correct issuer relationships and CA basic constraints. |
| 5. | Create five trust secrets with different levels: Root + Intermediate + Leaf, Root + Intermediate, Root only, Intermediate only, Leaf only. | Trust secrets are created. |
| 6. | For each trust secret, verify that clients can successfully produce and consume messages. | All five trust configurations succeed. |
| 7. | Create a trust secret with only a foreign Root CA. | Foreign trust secret is created. |
| 8. | Verify that clients using the foreign CA trust secret cannot connect. | Producer/consumer time out due to trust failure. |

**Labels:**

* [security](labels/security.md)


## testMultistageCustomCaUserCertificateAuthentication

**Description:** Verifies that only users with certificates signed by the designated Leaf CA can connect to Kafka. Users with certificates signed by Intermediate CA, Root CA, or a foreign CA are rejected.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Generate a custom CA chain: Root -> Intermediate -> Leaf. | CA chain is generated. |
| 2. | Generate a separate foreign Root CA. | Foreign CA is generated. |
| 3. | Generate four user certificates signed by Leaf CA, Intermediate CA, Root CA, and foreign CA respectively. | User certificates are generated. |
| 4. | Deploy external client certificate secrets (each signed by a different CA) and a broker-side CA trust secret containing only the Leaf CA cert. | Secrets are created in the namespace. |
| 5. | Deploy Kafka with an internal TLS listener using Custom TLS client authentication, with the broker truststore containing only the Leaf CA cert. | Kafka cluster is ready with the configured listener. |
| 6. | Verify that the user with a Leaf-CA-signed cert can produce and consume messages. | Messages are transmitted successfully. |
| 7. | Verify that users with Intermediate-CA, Root-CA, and foreign-CA-signed certs are rejected. | Producer/consumer time out due to TLS handshake failure. |

**Labels:**

* [security](labels/security.md)

