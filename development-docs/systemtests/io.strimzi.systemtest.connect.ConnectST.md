# ConnectST

**Description:** Verifies the deployment, manual rolling update, and undeployment of Kafka Connect components.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy scraper Pod for accessing all other Pods | Scraper Pod is deployed |

**Labels:**

* [connect](labels/connect.md)

<hr style="border:1px solid">

## testConnectScramShaAuthWithWeirdUserName

**Description:** Test verifying that Kafka Connect can authenticate with SCRAM-SHA-512 using a username with special characters and length exceeding typical constraints.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create resource with Node Pools | Node Pools created successfully |
| 2. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 3. | Deploy Kafka cluster with SCRAM-SHA-512 authentication | Kafka cluster deployed with specified authentications |
| 4. | Create Kafka Topic | Topic created successfully |
| 5. | Create Kafka SCRAM-SHA-512 user with a weird username | User created successfully with SCRAM-SHA-512 credentials |
| 6. | Deploy Kafka Connect with SCRAM-SHA-512 authentication | Kafka Connect instance deployed and configured with user credentials |
| 7. | Deploy Kafka Connector | Kafka Connector deployed and configured successfully |
| 8. | Send messages using the configured client | Messages sent successfully |
| 9. | Verify that connector receives messages | Messages consumed by the connector and written to the specified sink |

**Labels:**

* [connect](labels/connect.md)


## testConnectTlsAuthWithWeirdUserName

**Description:** Test verifying Kafka connect TLS authentication with a username containing unusual characters.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set up a name of username containing dots and 64 characters |  |
| 2. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 3. | Create Kafka broker, controller, topic, and Kafka user with the specified username | Resources are created with the expected configurations |
| 4. | Setup Kafka Connect with the created Kafka instance and TLS authentication | Kafka Connect is set up with the expected configurations |
| 5. | Check if the user can produce messages to Kafka | Messages are produced successfully |
| 6. | Verify that Kafka Connect can consume messages | Messages are consumed successfully by Kafka Connect |

**Labels:**

* [connect](labels/connect.md)


## testConnectorTaskAutoRestart

**Description:** Test the automatic restart functionality of Kafka Connect tasks when they fail.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create test storage instance | Test storage instance is created |
| 2. | Create node pool resources | Node pool resources are created and waited for readiness |
| 3. | Create Kafka cluster | Kafka cluster is created and waited for readiness |
| 4. | Deploy EchoSink Kafka Connector with autor restart enabled | Kafka Connector is created with auto-restart enabled |
| 5. | Send first batch of messages | First batch of messages is sent to the topic |
| 6. | Ensure connection success for the first batch | Successfully produce the first batch of messages |
| 7. | Send second batch of messages | Second batch of messages is sent to the topic |
| 8. | Ensure connection success for the second batch | Successfully produce the second batch of messages |
| 9. | Verify task failure and auto-restart | Connector task fails and is automatically restarted |
| 10. | Wait for task to reach running state | Connector task returns to running state after recovery |
| 11. | Verify auto-restart count reset | Auto-restart count is reset to zero after task stability |

**Labels:**

* [connect](labels/connect.md)


## testCustomAndUpdatedValues

**Description:** Test that verifies custom and updated environment variables and readiness/liveness probes for Kafka Connect.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create and configure Kafka Connect with initial values | Kafka Connect is created and configured with initial environment variables and readiness/liveness probes |
| 2. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 3. | Verify initial configuration and environment variables | Initial configuration and environment variables are as expected |
| 4. | Update Kafka Connect configuration and environment variables | Kafka Connect configuration and environment variables are updated |
| 5. | Verify updated configuration and environment variables | Updated configuration and environment variables are as expected |

**Labels:**

* [connect](labels/connect.md)


## testDeployRollUndeploy

**Description:** Verifies the deployment, manual rolling update, and undeployment of Kafka Connect components.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize Test Storage | Test storage instance is created with required context |
| 2. | Define expected configurations | Configurations are loaded from properties file |
| 3. | Create and wait for resources | Kafka resources, including NodePools and KafkaConnect instances, are created and become ready |
| 4. | Annotate for manual rolling update | KafkaConnect components are annotated for a manual rolling update |
| 5. | Perform and wait for rolling update | KafkaConnect components roll and new pods are deployed |
| 6. | Kafka Connect pod | Pod configurations and annotations are verified |
| 7. | Kafka Connectors | Various Kafka Connect resource labels and configurations are verified to ensure correct deployment |

**Labels:**

* [connect](labels/connect.md)


## testJvmAndResources

**Description:** Test ensuring the JVM options and resource requests/limits are correctly applied to Kafka Connect components.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TestStorage instance | TestStorage instance is created |
| 2. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 3. | Create broker and controller node pools | Node pools are created and ready |
| 4. | Create Kafka cluster | Kafka cluster is created and operational |
| 5. | Setup JVM options and resource requirements for Kafka Connect | Kafka Connect is configured with specified JVM options and resources |
| 6. | Verify JVM options and resource requirements | JVM options and resource requests/limits are correctly applied to the Kafka Connect pod |

**Labels:**

* [connect](labels/connect.md)


## testKafkaConnectAndConnectorFileSinkPlugin

**Description:** Test the functionality of Kafka Connect with a File Sink Plugin in a parallel namespace setup.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create and configure test storage | Test storage is set up with necessary configurations. |
| 2. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 3. | Create and wait for the broker and controller pools | Broker and controller pools are created and running. |
| 4. | Deploy and configure Kafka Connect with File Sink Plugin | Kafka Connect with File Sink Plugin is deployed and configured. |
| 5. | Deploy Network Policies for Kafka Connect | Network Policies are successfully deployed for Kafka Connect. |
| 6. | Create and wait for Kafka Connector | Kafka Connector is created and running. |
| 7. | Deploy and configure scraper pod | Scraper pod is deployed and configured. |
| 8. | Deploy and configure Kafka clients | Kafka clients are deployed and configured. |
| 9. | Execute assertions to verify the Kafka Connector configuration and status | Assertions confirm the Kafka Connector is successfully deployed, has the correct configuration, and is running. |

**Labels:**

* [connect](labels/connect.md)


## testKafkaConnectAndConnectorStateWithFileSinkPlugin

**Description:** This test case verifies pausing, stopping and running of connector via 'spec.pause' or 'spec.state' specification.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy prerequisites for running FileSink KafkaConnector, that is KafkaTopic, Kafka cluster, KafkaConnect, and FileSink KafkaConnector. | All resources are deployed and ready. |
| 2. | Pause and run connector by modifying 'spec.pause' property of Connector, while also producing messages when connector pauses. | Connector is paused and resumed as expected, after connector is resumed, produced messages are present in destination file, indicating connector resumed correctly. |
| 3. | Stop and run connector by modifying 'spec.state' property of Connector (with priority over now set 'spec.pause=false'), while also producing messages when connector stops. | Connector stops and resumes as expected, after resuming, produced messages are present in destination file, indicating connector resumed correctly. |
| 4. | Pause and run connector by modifying 'spec.state' property of Connector (with priority over now set 'spec.pause=false'), while also producing messages when connector pauses. | Connector pauses and resumes as expected, after resuming, produced messages are present in destination file, indicating connector resumed correctly. |

**Labels:**

* [connect](labels/connect.md)


## testKafkaConnectScaleUpScaleDown

**Description:** Test verifying the scaling up and down functionality of Kafka Connect in a Kubernetes environment.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TestStorage object instance | Instance of TestStorage is created |
| 2. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 3. | Create resources for KafkaNodePools and KafkaCluster | Resources are created and ready |
| 4. | Deploy Kafka Connect with file plugin | Kafka Connect is deployed with 1 initial replica |
| 5. | Verify the initial replica count | Initial replica count is verified to be 1 |
| 6. | Scale Kafka Connect up to a higher number of replicas | Kafka Connect is scaled up successfully |
| 7. | Verify the new replica count after scaling up | New replica count is verified to be the scaled up count |
| 8. | Scale Kafka Connect down to the initial number of replicas | Kafka Connect is scaled down successfully |
| 9. | Verify the replica count after scaling down | Replica count is verified to be the initial count |

**Labels:**

* [connect](labels/connect.md)


## testKafkaConnectWithPlainAndScramShaAuthentication

**Description:** Test verifying Kafka Connect functionalities with Plain and SCRAM-SHA authentication.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create object instance of TestStorage | Instance of TestStorage is created |
| 2. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 3. | Deploy Kafka with SCRAM-SHA-512 listener | Kafka is deployed with the specified listener authentication |
| 4. | Create KafkaUser with SCRAM-SHA authentication | KafkaUser is created using SCRAM-SHA authentication with the given credentials |
| 5. | Create KafkaTopic | KafkaTopic is created |
| 6. | Deploy KafkaConnect with SCRAM-SHA-512 authentication | KafkaConnect instance is deployed and connected to Kafka |
| 7. | Deploy required resources for NetworkPolicy, KafkaConnect, and ScraperPod | Resources are successfully deployed with NetworkPolicy applied |
| 8. | Create FileStreamSink connector | FileStreamSink connector is created successfully |
| 9. | Create Kafka client with SCRAM-SHA-PLAIN authentication and send messages | Messages are produced and consumed successfully using Kafka client with SCRAM-SHA-PLAIN authentication |
| 10. | Verify messages in KafkaConnect file sink | FileSink contains the expected number of messages |

**Labels:**

* [connect](labels/connect.md)


## testKafkaConnectWithScramShaAuthenticationRolledAfterPasswordChanged

**Description:** Verifies Kafka Connect functionality when SCRAM-SHA authentication password is changed and the component is rolled.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 2. | Create Kafka cluster with SCRAM-SHA authentication | Kafka cluster is created with SCRAM-SHA authentication enabled |
| 3. | Create a Kafka user with SCRAM-SHA authentication | Kafka user with SCRAM-SHA authentication is created |
| 4. | Deploy Kafka Connect with the created user credentials | Kafka Connect is deployed successfully |
| 5. | Update the SCRAM-SHA user password and reconfigure Kafka Connect | Kafka Connect is reconfigured with the new password |
| 6. | Verify Kafka Connect continues to function after rolling update | Kafka Connect remains functional and REST API is available |

**Labels:**

* [connect](labels/connect.md)


## testMountingSecretAndConfigMapAsVolumesAndEnvVars

**Description:** This test verifies that Secrets and ConfigMaps can be mounted as volumes and environment variables in Kafka Connect.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Secrets and ConfigMaps | Secrets and ConfigMaps are created successfully. |
| 2. | Create Kafka environment | Kafka broker, Kafka Connect, and other resources are deployed successfully. |
| 3. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 4. | Bind Secrets and ConfigMaps to Kafka Connect | Secrets and ConfigMaps are bound to Kafka Connect as volumes and environment variables. |
| 5. | Verify environment variables | Kafka Connect environment variables contain expected values from Secrets and ConfigMaps. |
| 6. | Verify mounted volumes | Kafka Connect mounted volumes contain expected values from Secrets and ConfigMaps. |

**Labels:**

* [connect](labels/connect.md)


## testMultiNodeKafkaConnectWithConnectorCreation

**Description:** Test validating multi-node Kafka Connect cluster creation, connector deployment, and message processing.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage and determine connect cluster name | Test storage and cluster name properly initialized |
| 2. | Create broker and controller node pools | Broker and controller node pools created successfully |
| 3. | Deploy Kafka cluster in ephemeral mode | Kafka cluster deployed successfully |
| 4. | Create Kafka Connect cluster with default image | Kafka Connect cluster created with appropriate configuration |
| 5. | Create and configure Kafka Connector | Kafka Connector deployed and configured with correct settings |
| 6. | Verify the status of the Kafka Connector | Kafka Connector status retrieved and worker node identified |
| 7. | Deploy Kafka clients for producer and consumer | Kafka producer and consumer clients deployed |
| 8. | Verify that Kafka Connect writes messages to the specified file sink | Messages successfully written to the file sink by Kafka Connect |

**Labels:**

* [connect](labels/connect.md)


## testScaleConnectAndConnectorSubresource

**Description:** This test verifies the scaling functionality of Kafka Connect and Kafka Connector subresources.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the test storage and create broker and controller pools | Broker and controller pools are created successfully |
| 2. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 3. | Deploy Kafka, Kafka Connect and Kafka Connector resources | Kafka, Kafka Connect and Kafka Connector resources are deployed successfully |
| 4. | Scale Kafka Connect subresource | Kafka Connect subresource is scaled successfully |
| 5. | Verify Kafka Connect subresource scaling | Kafka Connect replicas and observed generation are as expected |
| 6. | Scale Kafka Connector subresource | Kafka Connector subresource task max is set correctly |
| 7. | Verify Kafka Connector subresource scaling | Kafka Connector task max in spec, status and Connect Pods API are as expected |

**Labels:**

* [connect](labels/connect.md)


## testScaleConnectWithConnectorToZero

**Description:** Test scaling Kafka Connect with a connector to zero replicas.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TestStorage instance | TestStorage instance is created with context |
| 2. | Create broker and controller node pools | Broker and Controller node pools are created |
| 3. | Create ephemeral Kafka cluster | Kafka cluster with 3 replicas is created |
| 4. | Create Kafka Connect with file plugin | Kafka Connect is created with 2 replicas and file plugin |
| 5. | Create Kafka Connector | Kafka Connector is created with necessary configurations |
| 6. | Check Kafka Connect pods | There are 2 Kafka Connect pods |
| 7. | Scale down Kafka Connect to zero | Kafka Connect is scaled down to 0 replicas |
| 8. | Wait for Kafka Connect to be ready | Kafka Connect readiness is verified |
| 9. | Wait for Kafka Connector to not be ready | Kafka Connector readiness is verified |
| 10. | Verify conditions | Pod size is 0, Kafka Connect is ready, Kafka Connector is not ready due to zero replicas |

**Labels:**

* [connect](labels/connect.md)


## testScaleConnectWithoutConnectorToZero

**Description:** Test to validate scaling KafkaConnect without a connector to zero replicas.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage and create namespace | Namespace and storage initialized |
| 2. | Create broker and controller node pools | Node pools created with 3 replicas. |
| 3. | Create ephemeral Kafka cluster | Kafka cluster created with 3 replicas. |
| 4. | Create KafkaConnect resource with 2 replicas | KafkaConnect resource created with 2 replicas. |
| 5. | Verify that KafkaConnect has 2 pods | 2 KafkaConnect pods are running. |
| 6. | Scale down KafkaConnect to zero replicas | KafkaConnect scaled to zero replicas. |
| 7. | Wait for KafkaConnect to be ready | KafkaConnect is ready with 0 replicas. |
| 8. | Verify that KafkaConnect has 0 pods | No KafkaConnect pods are running and status is ready. |

**Labels:**

* [connect](labels/connect.md)


## testSecretsWithKafkaConnectWithTlsAndScramShaAuthentication

**Description:** Test validating Kafka Connect with TLS and SCRAM-SHA authentication along with associated resources setup and verification.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage | Instances created successfully |
| 2. | Create Kafka node pools (broker and controller) | Node pools created and ready |
| 3. | Deploy Kafka cluster with TLS and SCRAM-SHA-512 authentication | Kafka cluster deployed with listeners configured |
| 4. | Create Kafka user with SCRAM-SHA-512 | User created successfully |
| 5. | Deploy Kafka topic | Topic created successfully |
| 6. | Deploy Kafka Connect with TLS and SCRAM-SHA-512 authentication | Kafka Connect deployed with plugins and configuration |
| 7. | Deploy scraper pod for testing Kafka Connect | Scraper pod deployed successfully |
| 8. | Deploy NetworkPolicies for Kafka Connect | NetworkPolicies applied successfully |
| 9. | Create and configure FileStreamSink KafkaConnector | FileStreamSink KafkaConnector created and configured |
| 10. | Create Kafka clients for SCRAM-SHA-512 over TLS | Kafka clients (producer and consumer) created successfully |
| 11. | Wait for client operations to succeed | Message production and consumption verified |
| 12. | Verify messages in Kafka Connect file sink | Messages found in the specified file path |

**Labels:**

* [connect](labels/connect.md)


## testSecretsWithKafkaConnectWithTlsAndTlsClientAuthentication

**Description:** This test verifies that Kafka Connect works with TLS and TLS client authentication.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create test storage instance | Test storage instance is created |
| 2. | Create NodePools using resourceManager based on the configuration | NodePools for broker and controller are created or not based on configuration |
| 3. | Create resources for Kafka broker and Kafka Connect components | Resources are created and ready |
| 4. | Configure Kafka broker with TLS listener and client authentication | Kafka broker is configured correctly |
| 5. | Deploy Kafka user with TLS authentication | Kafka user is deployed with TLS authentication |
| 6. | Deploy Kafka topic | Kafka topic is deployed |
| 7. | Configure and deploy Kafka Connect with TLS and TLS client authentication | Kafka Connect is configured and deployed correctly |
| 8. | Deploy Network Policies for Kafka Connect | Network Policies are deployed |
| 9. | Create FileStreamSink KafkaConnector via scraper pod | KafkaConnector is created correctly |
| 10. | Deploy TLS clients and produce/consume messages | Messages are produced and consumed successfully |
| 11. | Verify messages in Kafka Connect FileSink | Messages are verified in Kafka Connect FileSink |

**Labels:**

* [connect](labels/connect.md)

