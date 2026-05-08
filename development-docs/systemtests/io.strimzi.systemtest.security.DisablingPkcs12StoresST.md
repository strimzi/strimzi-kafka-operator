# DisablingPkcs12StoresST

**Description:** Test suite for verifying that users can disable PKCS12 stores in CA and User secrets.

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testDisabledPKCS12Stores

**Description:** This test verifies that PKCS12 stores are not generated in CA and User secrets when it is disabled in the Cluster Operator configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a Kafka cluster. | Kafka cluster is deployed. |
| 2. | Create a KafkaUser with TLS authentication. | New KafkaUser is created. |
| 3. | Verify that Clients and Cluster CA secrets have no PKCS12 store and no password for it. | PKCS12 store and password are not present. |
| 4. | Verify that Kafka User secret has no PKCS12 store and no password for it. | PKCS12 store and password are not present. |

**Labels:**

* [security](labels/security.md)

