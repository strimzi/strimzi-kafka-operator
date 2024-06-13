# HttpBridgeKafkaExternalListenersST

**Description:** Test suite ensures secure SCRAM-SHA and TLS authentication for Kafka HTTP Bridge with unusual usernames.

**Contact:** `Lukas Kral <lukywill16@gmail.com>`

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy default cluster operator installation | Cluster operator is deployed |

**Use-cases:**

* `auth-weird-username`
* `scram-sha-auth`
* `Avoiding 409 error`

**Tags:**

* `regression`
* `bridge`
* `nodeport`
* `externalclients`

<hr style="border:1px solid">

## testScramShaAuthWithWeirdUsername

**Description:** Test verifies SCRAM-SHA authentication with a username containing special characters and length constraints.

**Contact:** `Lukas Kral <lukywill16@gmail.com>`

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create object instance | Instance of an object is created |
| 2. | Create a weird named user with special characters | User with a specified name is created |
| 3. | Initialize PasswordSecret for authentication | PasswordSecret is initialized with the predefined username and password |
| 4. | Initialize CertSecretSource for TLS configuration | CertSecretSource is set up with the proper certificate and secret names |
| 5. | Configure KafkaBridgeSpec with SCRAM-SHA authentication and TLS settings | KafkaBridgeSpec is built with the provided authentication and TLS settings |
| 6. | Invoke test method with weird username and bridge specification | Test runs successfully with no 409 error |

**Use-cases:**

* `auth-weird-username`
* `scram-sha-auth`

**Tags:**

* `regression`
* `bridge`
* `nodeport`
* `externalclients`


## testTlsAuthWithWeirdUsername

**Description:** Test ensuring that a node port service is created and 409 error is avoided when using a TLS authentication with a username that has unusual characters.

**Contact:** `Lukas Kral <lukywill16@gmail.com>`

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage and generate a weird username with dots and 64 characters | Weird username is generated successfully |
| 2. | Create and configure CertSecretSource with certificate and secret names for the consumer | CertSecretSource is configured with proper certificate and secret name |
| 3. | Build KafkaBridgeSpec with the TLS authentication using the weird username | KafkaBridgeSpec is created with the given username and TLS configuration |
| 4. | Invoke testWeirdUsername method with created configurations | The method runs without any 409 error |

**Use-cases:**

* `Creating a node port service`
* `Avoiding 409 error`

**Tags:**

* `regression`
* `bridge`
* `nodeport`
* `externalclients`

