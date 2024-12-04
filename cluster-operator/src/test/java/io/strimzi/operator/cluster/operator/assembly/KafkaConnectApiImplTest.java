/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.model.HttpRequest.request;

public class KafkaConnectApiImplTest {

    private static ClientAndServer server;

    @BeforeAll
    public static void setupServer() {
        server = new ClientAndServer(TestUtils.getFreePort());
    }

    @BeforeEach
    public void resetServer() {
        if (server != null && server.isRunning()) {
            server.reset();
        }
    }

    @AfterAll
    public static void stopServer() {
        if (server != null && server.isRunning()) {
            server.stop();
        }
    }

    @Test
    public void testJsonDecoding()  {
        assertThat(KafkaConnectApiImpl.tryToExtractErrorMessage(Reconciliation.DUMMY_RECONCILIATION, "{\"message\": \"This is the error\"}"), is("This is the error"));
        assertThat(KafkaConnectApiImpl.tryToExtractErrorMessage(Reconciliation.DUMMY_RECONCILIATION, "{\"message\": \"This is the error\""), is("Unknown error message"));
        assertThat(KafkaConnectApiImpl.tryToExtractErrorMessage(Reconciliation.DUMMY_RECONCILIATION, "Not a JSON"), is("Unknown error message"));
    }

    @Test
    public void testFeatureCompletionWithBadlyFormattedError() {
        HttpRequest request = request().withMethod("PUT");
        server.when(request).respond(HttpResponse.response().withBody("Some error message").withStatusCode(500));

        KafkaConnectApi api = new KafkaConnectApiImpl();

        assertThrows(Exception.class, api.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.getPort(), "my-connector", new JsonObject())
                .whenComplete((res, ex) -> assertThat(ex.getMessage(), containsString("Unknown error message")))::join);
    }

    @Test
    public void testFeatureCompletionWithWellFormattedError() {
        HttpRequest request = request().withMethod("PUT");
        server.when(request).respond(HttpResponse.response().withBody("{\"message\": \"This is the error\"}").withStatusCode(500));

        KafkaConnectApi api = new KafkaConnectApiImpl();

        assertThrows(Exception.class, api.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.getPort(), "my-connector", new JsonObject())
                .whenComplete((res, ex) -> assertThat(ex.getMessage(), containsString("This is the error")))::join);
    }

    @Test
    public void testListConnectLoggersWithLevel() throws Exception {
        HttpRequest request = request().withMethod("GET");
        server.when(request).respond(HttpResponse.response().withBody(new ObjectMapper().writeValueAsString(
                Map.of(
                        "org.apache.kafka.connect",
                        Map.of(
                                "level", "INFO"
                        )
                )
        )).withStatusCode(200));

        KafkaConnectApi api = new KafkaConnectApiImpl();

        api.listConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.getPort())
                .whenComplete((res, ex) -> assertThat(res, allOf(
                        aMapWithSize(1),
                        hasEntry("org.apache.kafka.connect", "INFO")
                ))).join();
    }

    @Test
    public void testListConnectLoggersWithLevelAndLastModified() throws Exception {
        HttpRequest request = request().withMethod("GET");
        server.when(request).respond(HttpResponse.response().withBody(new ObjectMapper().writeValueAsString(
                Map.of(
                        "org.apache.kafka.connect",
                        Map.of(
                                "level", "WARN",
                                "last_modified", "2020-01-01T00:00:00.000Z"
                        )
                )
        )).withStatusCode(200));

        KafkaConnectApi api = new KafkaConnectApiImpl();

        api.listConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.getPort())
                .whenComplete((res, ex) -> assertThat(res, allOf(
                        aMapWithSize(1),
                        hasEntry("org.apache.kafka.connect", "WARN")))
                ).join();
    }
}