/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaConnectApiImplTest {

    private static WireMockServer server;

    @BeforeAll
    public static void setupServer() {
        server = new WireMockServer(WireMockConfiguration.options().port(TestUtils.getFreePort()));
        server.start();
    }

    @BeforeEach
    public void resetServer() {
        if (server != null && server.isRunning()) {
            server.resetAll();
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
        server.stubFor(put(urlPathMatching(".*"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("Some error message")));

        KafkaConnectApi api = new KafkaConnectApiImpl();

        assertThrows(Exception.class, api.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.port(), "my-connector", new JsonObject())
                .whenComplete((res, ex) -> assertThat(ex.getMessage(), containsString("Unknown error message")))::join);
    }

    @Test
    public void testFeatureCompletionWithWellFormattedError() {
        server.stubFor(put(urlPathMatching(".*"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("{\"message\": \"This is the error\"}")));

        KafkaConnectApi api = new KafkaConnectApiImpl();

        assertThrows(Exception.class, api.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.port(), "my-connector", new JsonObject())
                .whenComplete((res, ex) -> assertThat(ex.getMessage(), containsString("This is the error")))::join);
    }

    @Test
    public void testListConnectLoggersWithLevel() throws Exception {
        server.stubFor(get(urlPathMatching(".*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(new ObjectMapper().writeValueAsString(
                                Map.of(
                                        "org.apache.kafka.connect",
                                        Map.of(
                                                "level", "INFO"
                                        )
                                )
                        ))));

        KafkaConnectApi api = new KafkaConnectApiImpl();

        api.listConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.port())
                .whenComplete((res, ex) -> assertThat(res, allOf(
                        aMapWithSize(1),
                        hasEntry("org.apache.kafka.connect", "INFO")
                ))).join();
    }

    @Test
    public void testListConnectLoggersWithLevelAndLastModified() throws Exception {
        server.stubFor(get(urlPathMatching(".*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(new ObjectMapper().writeValueAsString(
                                Map.of(
                                        "org.apache.kafka.connect",
                                        Map.of(
                                                "level", "WARN",
                                                "last_modified", "2020-01-01T00:00:00.000Z"
                                        )
                                )
                        ))));

        KafkaConnectApi api = new KafkaConnectApiImpl();

        api.listConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.port())
                .whenComplete((res, ex) -> assertThat(res, allOf(
                        aMapWithSize(1),
                        hasEntry("org.apache.kafka.connect", "WARN")))
                ).join();
    }
}