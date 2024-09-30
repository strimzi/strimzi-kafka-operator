# QuotasST

**Description:** NOTE: STs in this class will not properly work on `minikube` clusters (and maybe not on other clusters that use local storage), because the calculation of currently used storage is based on the local storage, which can be shared across multiple Docker containers. To properly run this suite, you should use cluster with proper storage.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy default cluster operator with the required configurations | Cluster operator is deployed |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testKafkaQuotasPluginIntegration

**Description:** Test to check Kafka Quotas Plugin for disk space.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Assume the cluster is not Minikube or MicroShift | Cluster is appropriate for the test |
| 2. | Create necessary resources for Kafka and nodes | Resources are created and Kafka is set up with quotas plugin |
| 3. | Send messages without any user; observe quota enforcement | Producer stops after reaching the minimum available bytes |
| 4. | Check Kafka logs for quota enforcement message | Kafka logs contain the expected quota enforcement message |
| 5. | Send messages with excluded user and observe the behavior | Messages are sent successfully without hitting the quota |
| 6. | Clean up resources | Resources are deleted successfully |

**Labels:**

* [kafka](labels/kafka.md)


## testKafkaQuotasPluginWithBandwidthLimitation

**Description:** Test verifying bandwidth limitations with Kafka quotas plugin.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set excluded principal | Principal is set |
| 2. | Create Kafka resources including node pools and persistent Kafka with quotas enabled | Kafka resources are created successfully with quotas setup |
| 3. | Create Kafka topic and user with SCRAM-SHA authentication | Kafka topic and SCRAM-SHA user are created successfully |
| 4. | Send messages with normal user | Messages are sent and duration is measured |
| 5. | Send messages with excluded user | Messages are sent and duration is measured |
| 6. | Assert that time taken for normal user is greater than for excluded user | Assertion is successful |

**Labels:**

* [kafka](labels/kafka.md)

