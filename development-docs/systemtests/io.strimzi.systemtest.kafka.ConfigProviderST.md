# ConfigProviderST

**Description:** This test suite verifies Kafka Connect using ConfigMap and EnvVar configuration.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy cluster operator across all namespaces, with custom configuration. | Cluster operator is deployed. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testConnectWithConnectorUsingConfigAndEnvProvider

**Description:** Test to ensure Kafka Connect functions correctly using ConfigMap and EnvVar configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create broker and controller KafkaNodePools. | Resources are created and are in ready state. |
| 2. | Create Kafka cluster. | Kafka cluster is ready |
| 3. | Create ConfigMap for connector configuration. | ConfigMap with connector configuration is created. |
| 4. | Deploy Kafka Connect with external configuration from ConfigMap. | Kafka Connect is deployed with proper configuration. |
| 5. | Create necessary Role and RoleBinding for connector. | Role and RoleBinding are created and applied. |
| 6. | Deploy Kafka connector. | Kafka connector is successfully deployed. |
| 7. | Deploy Kafka clients. | Kafka clients are deployed and ready. |
| 8. | Send messages and verify they are written to sink file. | Messages are successfully written to the specified sink file. |

**Labels:**

* [kafka](labels/kafka.md)

