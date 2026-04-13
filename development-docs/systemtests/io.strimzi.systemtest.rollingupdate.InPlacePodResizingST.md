# InPlacePodResizingST

**Description:** Test suite for testing Kubernetes in-place resource updates (changing of Pod resources dynamically, without restart). In-place resource updates require Kubernetes 1.35 and newer.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default installation. | Cluster Operator is deployed. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testInPlaceResourceUpdates

**Description:** Checks the in-place resource updates for Kafka nodes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with single mixed-role node pool and enabled in-place resizing. | Kafka cluster is deployed without any issue. |
| 2. | Slightly increase the resource request and limits in the `KafkaNodePool`. | The resources are updated dynamically without any rolling updates. |
| 3. | Update resource request and limits in the `KafkaNodePool` to values higher then the total capacity of the node. | Dynamic resource update is infeasible and the Cluster Operator rolls the first broker Pod that becomes `Pending`. |
| 4. | Update the resources again back to the original value. | The Cluster Operator recovers the `Pending` Kafka node. |

**Labels:**

* [kafka](labels/kafka.md)

