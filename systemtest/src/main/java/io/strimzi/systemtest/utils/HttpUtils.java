/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static io.restassured.RestAssured.given;

public class HttpUtils {

    private static final Logger LOGGER = LogManager.getLogger(HttpUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(TestConstants.SERVICE);

    private HttpUtils() { }

    public static void waitUntilServiceWithNameIsReady(String baserURI, String serviceName) {

        LOGGER.info("Waiting for Service name: {} to be present in JSON", serviceName);
        TestUtils.waitFor("Service name: " + serviceName + " is present in JSON", TestConstants.GLOBAL_TRACING_POLL, READINESS_TIMEOUT,
            () -> {
                Response response = given()
                        .when()
                            .baseUri(baserURI)
                            .relaxedHTTPSValidation()
                            .contentType("application/json")
                            .get("/jaeger/api/services");

                return response.body().peek().print().contains(serviceName);
            });
        LOGGER.info("Service name: {} is present", serviceName);
    }

    public static void waitUntilServiceHasSomeTraces(String baseURI, String serviceName) {
        LOGGER.info("Waiting for Service: {} to contain some traces", serviceName);
        TestUtils.waitFor("Service: " + serviceName + " to contain some traces", TestConstants.GLOBAL_TRACING_POLL, READINESS_TIMEOUT,
            () -> {
                Response response = given()
                            .when()
                                .baseUri(baseURI)
                                .relaxedHTTPSValidation()
                                .contentType("application/json")
                                .get("/jaeger/api/traces?service=" + serviceName);

                JsonPath jsonPathValidator = response.jsonPath();
                Map<Object, Object> data = jsonPathValidator.getMap("$");
                return data.size() > 0;
            });
        LOGGER.info("Service: {} contains some traces", serviceName);
    }
}
