# CORS handling

## Description
The `cors_handling` label encompasses tests aimed at ensuring proper Cross-Origin Resource Sharing (CORS) handling in the Kafka Bridge. These tests verify that the Kafka Bridge correctly processes CORS requests, validating both allowed and forbidden origins.

#### Label: `cors_handling`

This label is used for tests that focus on the CORS functionality in the Kafka Bridge. 
It ensures that the bridge correctly handles HTTP requests from different origins based on the CORS configuration. 
The tests included under this label check for the following:
1. Allowed Origins: Verifying that the Kafka Bridge correctly handles and permits requests from allowed origins.
2. Forbidden Origins: Ensuring that requests from forbidden origins are correctly rejected with appropriate status codes and messages.

## Key Aspects Covered
1. **Setup and Configuration**:
   1. Configuring Kafka Bridge with CORS settings.
   2. Deploying required Kafka resources and scraper pods.
2. **Allowed Origin Requests**:
   1. Sending HTTP OPTIONS requests from allowed origins.
   2. Validating that responses contain correct status codes and headers.
   3. Ensuring GET requests from allowed origins return expected results.
3. **Forbidden Origin Requests**:
   1. Sending HTTP OPTIONS and POST requests from forbidden origins.
   2. Confirming that responses correctly reject the requests with status code 403 and relevant error messages.



<!-- generated part -->
**Tests:**
- [testCorsOriginAllowed](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeCorsST.md)
- [testCorsForbidden](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeCorsST.md)
