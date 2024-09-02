# HttpBridgeCorsST

**Description:** Test suite for HTTP Bridge CORS functionality, focusing on verifying correct handling of allowed and forbidden origins.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set up Kafka Bridge and its configuration including CORS settings | Kafka Bridge is set up with the correct configuration |
| 2. | Deploy required Kafka resources and scraper pod | Kafka resources and scraper pod are deployed and running |

**Labels:**

* [bridge](labels/bridge.md)

<hr style="border:1px solid">

## testCorsForbidden

**Description:** Test ensuring that CORS (Cross-Origin Resource Sharing) requests with forbidden origins are correctly rejected by the Bridge.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create Kafka Bridge user and consumer group | Kafka Bridge user and consumer group are created successfully |
| 2. | Set up headers with forbidden origin and pre-flight HTTP OPTIONS method | Headers and method are set correctly |
| 3. | Send HTTP OPTIONS request to the Bridge | HTTP OPTIONS request is sent to the Bridge and a response is received |
| 4. | Verify the response contains '403' and 'CORS Rejected - Invalid origin' | Response indicates the CORS request is rejected |
| 5. | Remove 'Access-Control-Request-Method' from headers and set HTTP POST method | Headers are updated and HTTP method is set correctly |
| 6. | Send HTTP POST request to the Bridge | HTTP POST request is sent to the Bridge and a response is received |
| 7. | Verify the response contains '403' and 'CORS Rejected - Invalid origin' | Response indicates the CORS request is rejected |

**Labels:**

* [bridge](labels/bridge.md)


## testCorsOriginAllowed

**Description:** This test checks if CORS handling for allowed origin works correctly in the Kafka Bridge.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Set up the Kafka Bridge user and configuration | Kafka Bridge user and configuration are set up |
| 2. | Construct the request URL and headers | URL and headers are constructed properly |
| 3. | Send OPTIONS request to Kafka Bridge and capture the response | Response is captured from Bridge |
| 4. | Validate the response contains expected status codes and headers | Response has correct status codes and headers for allowed origin |
| 5. | Send GET request to Kafka Bridge and capture the response | Response is captured from Bridge for GET request |
| 6. | Check if the GET request response is '404 Not Found' | Response for GET request is 404 Not Found |

**Labels:**

* [bridge](labels/bridge.md)

