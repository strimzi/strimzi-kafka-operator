/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;


public class TracingUtils {

    private static final Logger LOGGER = LogManager.getLogger(TracingUtils.class);

    private static final String JAEGER_QUERY_SERVICE = "my-jaeger-query";
    private static final String JAEGER_QUERY_SERVICE_ENDPOINT = "/jaeger/api/services";
    private static final String JAEGER_QUERY_SERVICE_TRACES_ENDPOINT = "/jaeger/api/traces?service=";
    private static final int JAEGER_QUERY_PORT = 16686;

    private TracingUtils() {}

    public static void verify(String jaegerServiceName, String clientPodName) {
        verifyThatServiceIsPresent(jaegerServiceName, clientPodName);
        verifyThatServiceTracesArePresent(jaegerServiceName, clientPodName);
    }

    private static void verifyThatServiceIsPresent(String jaegerServiceName, String clientPodName) {
        TestUtils.waitFor("Wait until service" + jaegerServiceName + " is present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            JsonObject jaegerServices = new JsonObject(cmdKubeClient().execInPod(clientPodName, "/bin/bash", "-c", "curl " + JAEGER_QUERY_SERVICE + ":" + JAEGER_QUERY_PORT + JAEGER_QUERY_SERVICE_ENDPOINT).out());

            LOGGER.info("Jaeger services {}", jaegerServices.getJsonArray("data").contains(jaegerServiceName));
            return jaegerServices.getJsonArray("data").contains(jaegerServiceName);
        });
    }

    private static void verifyThatServiceTracesArePresent(String jaegerServiceName, String clientPodName) {
        TestUtils.waitFor("Wait until service" + jaegerServiceName + " has some traces", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            JsonObject jaegerServicesTraces = new JsonObject(cmdKubeClient().execInPod(clientPodName,
                "/bin/bash", "-c", "curl " + JAEGER_QUERY_SERVICE + ":" + JAEGER_QUERY_PORT + JAEGER_QUERY_SERVICE_TRACES_ENDPOINT + jaegerServiceName).out());
            JsonArray traces = jaegerServicesTraces.getJsonArray("data");

            if (!(jaegerServicesTraces.getJsonArray("data").size() > 0)) {
                LOGGER.error("Jaeger service {} does not contain data object", jaegerServiceName);
                return false;
            }

            for (Object trace : traces) {
                String traceId = ((JsonObject) trace).getString("traceID");

                if (!(traceId.matches("^[a-z0-9]+"))) {
                    LOGGER.error("Jaeger trace does not have correct trace Id {}", traceId);
                    return false;
                }

                JsonArray spans = ((JsonObject) trace).getJsonArray("spans");

                if (!(spans.size() > 0)) {
                    LOGGER.error("Jaeger trace {} does not have more than 0 spans inside trace specifically {} spans", traceId, spans.size());
                    return false;
                }

                JsonObject processes = ((JsonObject) trace).getJsonObject("processes");

                if (!(processes.size() > 0)) {
                    LOGGER.error("Jaeger trace {} does not have more that 0 processes specifically {} processes", traceId, processes);
                    return false;
                }

                JsonObject warnings = ((JsonObject) trace).getJsonObject("warnings");

                if (warnings != null) {
                    LOGGER.error("Jaeger trace {} contain some warnings {}", traceId, warnings.toString());
                    return false;
                }
            }
            return true;
        });
    }
}
