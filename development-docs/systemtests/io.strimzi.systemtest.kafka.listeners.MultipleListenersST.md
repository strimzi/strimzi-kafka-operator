# MultipleListenersST

**Description:** Test to verify the functionality of using multiple NodePort listeners in a Kafka cluster within the same namespace.

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testCombinationOfEveryKindOfListener

**Description:** Verifies the combination of every kind of Kafka listener: Internal, NodePort, Route, and LOADBALANCER.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve different types of Kafka listeners. | Lists of Internal, NodePort, Route, and LOADBALANCER listeners are retrieved. |
| 2. | Combine all different listener lists. | A combined list of all Kafka listener types is created. |
| 3. | Run listeners test with combined listener list. | Listeners test runs with all types of Kafka listeners in the combined list. |

**Labels:**

* [kafka](labels/kafka.md)


## testCombinationOfInternalAndExternalListeners

**Description:** Test verifying the combination of Internal and Enternal Kafka listeners.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Check if the environment supports cluster-wide NodePort rights. | Test is skipped if the environment is not suitable. |
| 2. | Retrieve and combine Internal and NodePort listeners. | Listeners are successfully retrieved and combined. |
| 3. | Run listeners test with combined listeners. | Listeners test is executed successfully. |

**Labels:**

* [kafka](labels/kafka.md)


## testMixtureOfExternalListeners

**Description:** Test ensuring that different types of Enternal Kafka listeners (Route and NodePort) work correctly when mixed.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve route listeners. | Route listeners are retrieved from test cases. |
| 2. | Retrieve NodePort listeners. | Nodeport listeners are retrieved from test cases. |
| 3. | Combine route and NodePort listeners. | Multiple different listeners list is populated. |
| 4. | Run listeners test. | Listeners test runs using the combined list. |

**Labels:**

* [kafka](labels/kafka.md)


## testMultipleInternal

**Description:** Test to verify the usage of more than one Kafka cluster within a single namespace.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Run the Internal Kafka listeners test. | Listeners test runs successfully on the specified cluster. |

**Labels:**

* [kafka](labels/kafka.md)


## testMultipleLoadBalancers

**Description:** Test verifying the behavior of multiple LoadBalancers in a single namespace using more than one Kafka cluster.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Run listeners test with LoadBalancer type. | Listeners test executes successfully with LoadBalancers. |
| 2. | Validate the results. | Results match the expected outcomes for multiple LoadBalancers. |

**Labels:**

* [kafka](labels/kafka.md)


## testMultipleNodePorts

**Description:** Test verifying the functionality of using multiple NodePort listeners in a Kafka cluster within the same namespace.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Execute listener tests with NodePort configuration. | Listener tests run without issues using NodePort. |

**Labels:**

* [kafka](labels/kafka.md)


## testMultipleRoutes

**Description:** Test to verify the functionality of multiple Kafka route listeners in a single namespace.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve test cases for Kafka Listener Type Route. | Test cases for Route are retrieved. |
| 2. | Run listener tests using the retrieved test cases and cluster name. | Listener tests run successfully with no errors. |

**Labels:**

* [kafka](labels/kafka.md)

