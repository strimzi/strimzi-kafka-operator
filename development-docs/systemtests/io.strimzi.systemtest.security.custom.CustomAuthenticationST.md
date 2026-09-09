# CustomAuthenticationST

**Description:** Test suite for verifying the custom authentication based on the Kubernetes Service Account tokens.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the Cluster Operator. | Cluster Operator is deployed and ready. |

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testServiceAccountAuthentication

**Description:** This test case verifies the authentication based on the Kubernetes Service Account tokens. The Kafka listener validates the tokens against the JWKS endpoint of the Kubernetes API server and requires a custom audience. Kafka Connect and the Kafka clients authenticate with tokens obtained through projected Service Account token volumes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create the broker and controller KafkaNodePools. | KafkaNodePools are created. |
| 2. | Deploy a Kafka cluster with a custom listener that authenticates clients with Kubernetes Service Account tokens and requires a custom audience. | Kafka cluster is deployed and ready. |
| 3. | Create a KafkaTopic. | KafkaTopic is ready. |
| 4. | Deploy Kafka Connect with custom authentication and a projected Service Account token volume mounted into the Connect container. | Kafka Connect connects to the Kafka cluster and becomes ready. |
| 5. | Create a FileStreamSink KafkaConnector consuming from the KafkaTopic. | KafkaConnector is ready. |
| 6. | Produce messages with a Kafka client using a projected Service Account token with the expected audience. | The producer finishes successfully. |
| 7. | Check the file sink of the KafkaConnector. | All produced messages are present in the file sink, so the connector consumed them over the authenticated listener. |

**Labels:**

* [security](labels/security.md)

