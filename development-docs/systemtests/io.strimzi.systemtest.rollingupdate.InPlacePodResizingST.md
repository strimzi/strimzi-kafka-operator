# InPlacePodResizingST

**Description:** Test suite for testing Kubernetes in-place resource updates (changing of Pod resources dynamically, without restart). In-place resource updates require Kubernetes 1.35 and newer.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default installation. | Cluster Operator is deployed. |
| 2. | Deploy Kafka with single mixed-role node pool and enabled in-place resizing. | Kafka cluster is deployed without any issue. |
| 3. | Deploy Connect cluster with single node and enabled in-place resizing. | Connect cluster is deployed without any issue. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testDeferredInPlaceConnectResourceUpdates

**Description:** Checks the in-place resource updates of a Connect cluster with amount of resources equal to the node capacity to get deferred resizing.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Update resource request and limits in the `KafkaConnect` resource to values exactly at the total capacity of the node. | Dynamic resource update is deferred and the Cluster Operator rolls the first broker Pod that becomes `Pending`. |
| 2. | Update the resources again back to the original value. | The Cluster Operator recovers the `Pending` Connect node. |

**Labels:**

* [kafka](labels/kafka.md)


## testDeferredInPlaceKafkaResourceUpdates

**Description:** Checks the in-place resource updates of a Kafka cluster with amount of resources equal to the node capacity to get deferred resizing.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Update resource request and limits in the `KafkaNodePool` to values exactly at the total capacity of the node. | Dynamic resource update is deferred and the Cluster Operator rolls the first broker Pod that becomes `Pending`. |
| 2. | Update the resources again back to the original value. | The Cluster Operator recovers the `Pending` Kafka node. |

**Labels:**

* [kafka](labels/kafka.md)


## testDynamicInPlaceConnectResourceUpdates

**Description:** Checks the in-place resource updates for Connect nodes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Slightly increase the resource request and limits in the `KafkaConnect` resource. | The resources are updated dynamically without any rolling updates. |

**Labels:**

* [kafka](labels/kafka.md)


## testDynamicInPlaceKafkaResourceUpdates

**Description:** Checks the in-place resource updates for Kafka nodes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Slightly increase the resource request and limits in the `KafkaNodePool`. | The resources are updated dynamically without any rolling updates. |

**Labels:**

* [kafka](labels/kafka.md)


## testInfeasibleInPlaceConnectResourceUpdates

**Description:** Checks the in-place resource updates of a Connect cluster with infeasible amount of resources.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Update resource request and limits in the `KafkaConnect` resource to values higher then the total capacity of the node. | Dynamic resource update is infeasible and the Cluster Operator rolls the first broker Pod that becomes `Pending`. |
| 2. | Update the resources again back to the original value. | The Cluster Operator recovers the `Pending` Connect node. |

**Labels:**

* [kafka](labels/kafka.md)


## testInfeasibleInPlaceKafkaResourceUpdates

**Description:** Checks the in-place resource updates of a Kafka cluster with infeasible amount of resources.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Update resource request and limits in the `KafkaNodePool` to values higher then the total capacity of the node. | Dynamic resource update is infeasible and the Cluster Operator rolls the first broker Pod that becomes `Pending`. |
| 2. | Update the resources again back to the original value. | The Cluster Operator recovers the `Pending` Kafka node. |

**Labels:**

* [kafka](labels/kafka.md)

