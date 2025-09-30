# CruiseControlApiST

**Description:** This test suite verifies that Cruise Control's basic API requests function correctly

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the Cluster Operator | Cluster Operator is deployed |

**Labels:**

* [cruise-control](labels/cruise-control.md)

<hr style="border:1px solid">

## testCruiseControlAPIUsers

**Description:** This test case verifies the creation and usage of Cruise Control's API users.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create broker and controller KafkaNodePools | Both KafkaNodePools are successfully created |
| 2. | Create Secret containing the `arnost: heslo, USER` in the `.key` field | Secret is correctly created |
| 3. | Deploy Kafka with Cruise Control containing configuration for the CC API users, with reference to the Secret (and its `.key` value) created in previous step | Kafka cluster with Cruise Control are deployed, the CC API users configuration is applied |
| 4. | Do request to Cruise Control's API, specifically to `/state` endpoint with `arnost:heslo` user | Request is successful and response contains information about state of the Cruise Control |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlBasicAPIRequestsWithSecurityDisabled

**Description:** Test that verifies Cruise Control's basic API requests function correctly with security features disabled.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create broker and controller KafkaNodePools | Both KafkaNodePools are successfully created |
| 2. | Disable Cruise Control security and SSL in configuration | Configuration map is set with security and SSL disabled |
| 3. | Create required Kafka and Cruise Control resources with disabled security | Kafka and Cruise Control resources are deployed without enabling security |
| 4. | Call the Cruise Control state endpoint using HTTP without credentials | Cruise Control state response is received with HTTP status code 200 |
| 5. | Verify the Cruise Control state response | Response indicates Cruise Control is RUNNING with NO_TASK_IN_PROGRESS |

**Labels:**

* [cruise-control](labels/cruise-control.md)

