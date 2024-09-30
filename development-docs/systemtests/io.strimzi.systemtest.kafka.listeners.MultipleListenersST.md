# MultipleListenersST

**Description:** Test to verify the functionality of using multiple NodePorts in a Kafka cluster within the same namespace.

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testCombinationOfEveryKindOfListener

**Description:** Verifies the combination of every kind of Kafka listener: INTERNAL, NODEPORT, ROUTE, and LOADBALANCER.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve different types of Kafka listeners | Lists of INTERNAL, NODEPORT, ROUTE, and LOADBALANCER listeners are retrieved |
| 2. | Combine all different listener lists | A combined list of all Kafka listener types is created |
| 3. | Run listeners test with combined listener list | Listeners test runs with all types of Kafka listeners in the combined list |

**Labels:**

* [kafka](labels/kafka.md)


## testCombinationOfInternalAndExternalListeners

**Description:** Test verifying the combination of internal and external Kafka listeners.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Check if the environment supports cluster-wide NodePort rights | Test is skipped if the environment is not suitable |
| 2. | Retrieve and combine internal and NodePort listeners | Listeners are successfully retrieved and combined |
| 3. | Run listeners test with combined listeners | Listeners test is executed successfully |

**Labels:**

* [kafka](labels/kafka.md)


## testMixtureOfExternalListeners

**Description:** Test ensuring that different types of external Kafka listeners (ROUTE and NODEPORT) work correctly when mixed.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve route listeners | Route listeners are retrieved from test cases |
| 2. | Retrieve nodeport listeners | Nodeport listeners are retrieved from test cases |
| 3. | Combine route and nodeport listeners | Multiple different listeners list is populated |
| 4. | Run listeners test | Listeners test runs using the combined list |

**Labels:**

* [kafka](labels/kafka.md)


## testMultipleInternal

**Description:** Test to verify the usage of more than one Kafka cluster within a single namespace.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Run the internal Kafka listeners test | Listeners test runs successfully on the specified cluster |

**Labels:**

* [kafka](labels/kafka.md)


## testMultipleLoadBalancers

**Description:** Test verifying the behavior of multiple load balancers in a single namespace using more than one Kafka cluster.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Run listeners test with LOADBALANCER type | Listeners test executes successfully with load balancers |
| 2. | Validate the results | Results match the expected outcomes for multiple load balancers |

**Labels:**

* [kafka](labels/kafka.md)


## testMultipleNodePorts

**Description:** Test verifying the functionality of using multiple NodePorts in a Kafka cluster within the same namespace.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Execute listener tests with NodePort configuration | Listener tests run without issues using NodePort |

**Labels:**

* [kafka](labels/kafka.md)


## testMultipleRoutes

**Description:** Test to verify the functionality of multiple Kafka route listeners in a single namespace.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve test cases for Kafka Listener Type ROUTE | Test cases for ROUTE are retrieved |
| 2. | Run listener tests using the retrieved test cases and cluster name | Listener tests run successfully with no errors |

**Labels:**

* [kafka](labels/kafka.md)

