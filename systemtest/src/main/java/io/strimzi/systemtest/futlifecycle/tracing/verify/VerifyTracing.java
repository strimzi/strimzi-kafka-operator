/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.futlifecycle.tracing.verify;

import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.hamcrest.Matchers;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Class VerifyTracing for containing all the verification methods.
 */
public class VerifyTracing {

    private static final String JAEGER_QUERY_SERVICE = "my-jaeger-query";
    private static final String JAEGER_QUERY_SERVICE_ENDPOINT = "/jaeger/api/services";
    private static final String JAEGER_QUERY_SERVICE_TRACES_ENDPOINT = "/jaeger/api/traces?service=";
    private static final int JAEGER_QUERY_PORT = 16686;

    private VerifyTracing() {}

    public static void verify(String jaegerServiceName, String clientPodName) {
        verifyThatServiceIsPresent(jaegerServiceName, clientPodName);
        verifyThatServiceTracesArePresent(jaegerServiceName, clientPodName);
    }

    private static void verifyThatServiceIsPresent(String jaegerServiceName, String clientPodName) {
        TestUtils.waitFor("Wait until service" + jaegerServiceName + " is present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            JsonObject jaegerServices = new JsonObject(cmdKubeClient().execInPod(clientPodName, "/bin/bash", "-c", "curl " + JAEGER_QUERY_SERVICE + ":" + JAEGER_QUERY_PORT + JAEGER_QUERY_SERVICE_ENDPOINT).out());
            try {
                assertThat(jaegerServices.getJsonArray("data"), hasItem(jaegerServiceName));
                return true;
            } catch (AssertionError e) {
                e.printStackTrace();
                return false;
            }
        });
    }

    private static void verifyThatServiceTracesArePresent(String jaegerServiceName, String clientPodName) {
        TestUtils.waitFor("Wait until service" + jaegerServiceName + " has some traces", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            JsonObject jaegerServicesTraces = new JsonObject(cmdKubeClient().execInPod(clientPodName,
                "/bin/bash", "-c", "curl " + JAEGER_QUERY_SERVICE + ":" + JAEGER_QUERY_PORT + JAEGER_QUERY_SERVICE_TRACES_ENDPOINT + jaegerServiceName).out());
            JsonArray traces = jaegerServicesTraces.getJsonArray("data");

            try {
                assertThat(jaegerServicesTraces.getJsonArray("data").size(), greaterThan(0));
                for (Object trace : traces) {
                    assertThat(((JsonObject) trace).getString("traceID"), Matchers.matchesPattern("^[a-z0-9]+"));
                    assertThat(((JsonObject) trace).getJsonArray("spans").size(), greaterThan(0));
                    assertThat(((JsonObject) trace).getJsonObject("processes").size(), greaterThan(0));
                    assertThat(((JsonObject) trace).getJsonObject("warnings"), nullValue());
                }
                return true;
            } catch (AssertionError e) {
                e.printStackTrace();
                return false;
            }
        });
    }
}
