# CruiseControlApiST

**Description:** This test case verifies that Cruise Control's basic API requests function correctly with security features disabled.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the cluster operator | Cluster operator is deployed |

**Labels:**

* [cruise-control](labels/cruise-control.md)

<hr style="border:1px solid">

## testCruiseControlAPIUsers

**Description:** This test case verifies the creation and usage of CruiseControl's API users.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the test storage. | Test storage object is created. |
| 2. | Create NodePools for the Kafka cluster | NodePools are created |
| 3. | Create Secret containing the `arnost: heslo, USER` in the `.key` field | Secret is correctly created |
| 4. | Deploy Kafka with CruiseControl containing configuration for the CC API users, with reference to the Secret (and its `.key` value) created in previous step | Kafka cluster with CruiseControl are deployed, the CC API users configuration is applied |
| 5. | Do request to CruiseControl's API, specifically to `/state` endpoint with `arnost:heslo` user | Request is successful and response contains information about state of the CruiseControl |

**Labels:**

* [cruise-control](labels/cruise-control.md)


## testCruiseControlBasicAPIRequestsWithSecurityDisabled

**Description:** Test that verifies Cruise Control's basic API requests function correctly with security features disabled.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the test storage. | Test storage object is created. |
| 2. | Disable Cruise Control security and SSL in configuration. | Configuration map is set with security and SSL disabled. |
| 3. | Create required Kafka and Cruise Control resources with disabled security. | Kafka and Cruise Control resources are deployed without enabling security. |
| 4. | Call the Cruise Control state endpoint using HTTP without credentials. | Cruise Control state response is received with HTTP status code 200. |
| 5. | Verify the Cruise Control state response. | Response indicates Cruise Control is RUNNING with NO_TASK_IN_PROGRESS. |

**Labels:**

* [cruise-control](labels/cruise-control.md)

