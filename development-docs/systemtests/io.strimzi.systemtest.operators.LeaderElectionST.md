# LeaderElectionST

**Description:** Suite for testing Leader Election feature which allows users to run Cluster Operator in more than one replica. There will always be one leader, other replicas will stay in standby mode.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Verify deployment files contain all needed environment variables for leader election. | Deployment files contain required leader election environment variables. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testLeaderElection

**Description:** This test verifies that leader election works correctly when running Cluster Operator with multiple replicas. It tests leader failover by causing the current leader to crash and verifying that a new leader is elected.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with 2 replicas and leader election enabled. | Cluster Operator is deployed with 2 replicas and leader election is active. |
| 2. | Identify the current leader pod from the lease. | Current leader pod is identified from the lease holder identity. |
| 3. | Cause the leader pod to crash by changing its image to invalid one. | Leader pod enters CrashLoopBackOff state. |
| 4. | Wait for a new leader to be elected. | A different pod becomes the new leader and lease is updated. |
| 5. | Verify new leader election logs. | New leader pod logs contain leader election message. |

**Labels:**

* [kafka](labels/kafka.md)


## testLeaderElectionDisabled

**Description:** This test verifies that when leader election is disabled, no lease is created and no leader election messages appear in the logs.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with leader election disabled. | Cluster Operator is deployed with STRIMZI_LEADER_ELECTION_ENABLED=false. |
| 2. | Verify no lease exists for the Cluster Operator. | No lease resource is created in the operator namespace. |
| 3. | Check Cluster Operator logs for absence of leader election messages. | Logs do not contain leader election messages. |

**Labels:**

* [kafka](labels/kafka.md)

