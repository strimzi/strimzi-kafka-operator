# HttpBridgeScramShaST

**Description:** Test suite for validating Kafka Bridge functionality with TLS and SCRAM-SHA authentication

**Contact:** `Jakub Stejskal <xstejs24@gmail.com>`

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TestStorage instance | TestStorage instance is created |
| 2. | Create BridgeClients instance | BridgeClients instance is created |
| 3. | Deploy Kafka and KafkaBridge | Kafka and KafkaBridge are deployed successfully |
| 4. | Create Kafka topic | Kafka topic is created with the given configuration |
| 5. | Create Kafka user with SCRAM-SHA authentication | Kafka user is created and configured with SCRAM-SHA authentication |
| 6. | Deploy HTTP bridge | HTTP bridge is deployed |

**Use-cases:**

* `tls-scram-authentication`
* `message-production`
* `message-consumption`

**Tags:**

* `internalclients`
* `bridge`
* `regression`

<hr style="border:1px solid">

## testReceiveSimpleMessageTlsScramSha

**Description:** Test to check the reception of a simple message via Kafka Bridge using TLS and SCRAM-SHA encryption.

**Contact:** `Lukas Kral <lukywill16@gmail.com>`

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage and BridgeClientsBuilder instances | Instances are successfully initialized |
| 2. | Create Kafka topic using ResourceManager | Kafka topic is created and available |
| 3. | Create Bridge consumer using ResourceManager | Bridge consumer is successfully created |
| 4. | Send messages to Kafka using KafkaClients | Messages are successfully sent to the Kafka topic |
| 5. | Wait for clients' success validation | Messages are successfully consumed from the Kafka topic |

**Use-cases:**

* `tls-scram-authentication`

**Tags:**

* `internalclients`
* `bridge`
* `regression`


## testSendSimpleMessageTlsScramSha

**Description:** Test ensuring that sending a simple message using TLS and SCRAM-SHA authentication via Kafka Bridge works as expected.

**Contact:** `Lukas Kral <lukywill16@gmail.com>`

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TestStorage and BridgeClients objects | Instances of TestStorage and BridgeClients are created |
| 2. | Create topic using the resource manager | Topic is created successfully with the specified configuration |
| 3. | Start producing messages via Kafka Bridge | Messages are produced successfully to the topic |
| 4. | Wait for producer success | Producer finishes sending messages without errors |
| 5. | Create KafkaClients and configure with TLS and SCRAM-SHA | Kafka client is configured with appropriate security settings |
| 6. | Start consuming messages via Kafka client | Messages are consumed successfully from the topic |
| 7. | Wait for consumer success | Consumer finishes receiving messages without errors |

**Use-cases:**

* `tls-scram-authentication`
* `message-production`
* `message-consumption`

**Tags:**

* `internalclients`
* `bridge`
* `regression`

