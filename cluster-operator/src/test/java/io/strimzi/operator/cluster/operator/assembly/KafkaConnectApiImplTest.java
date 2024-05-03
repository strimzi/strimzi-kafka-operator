/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

@ExtendWith(VertxExtension.class)
public class KafkaConnectApiImplTest {
    @Test
    public void testJsonDecoding()  {
        assertThat(KafkaConnectApiImpl.tryToExtractErrorMessage(Reconciliation.DUMMY_RECONCILIATION, Buffer.buffer("{\"message\": \"This is the error\"}")), is("This is the error"));
        assertThat(KafkaConnectApiImpl.tryToExtractErrorMessage(Reconciliation.DUMMY_RECONCILIATION, Buffer.buffer("{\"message\": \"This is the error\"")), is("Unknown error message"));
        assertThat(KafkaConnectApiImpl.tryToExtractErrorMessage(Reconciliation.DUMMY_RECONCILIATION, Buffer.buffer("Not a JSON")), is("Unknown error message"));
    }

    @Test
    public void testFeatureCompletionWithBadlyFormattedError(Vertx vertx, VertxTestContext context) throws ExecutionException, InterruptedException {
        HttpServer server = mockApi(vertx, 500, "Some error message");

        KafkaConnectApi api = new KafkaConnectApiImpl(vertx);

        Checkpoint async = context.checkpoint();
        api.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.actualPort(), "my-connector", new JsonObject())
                        .onComplete(context.failing(res -> context.verify(() -> {
                            assertThat(res.getMessage(), containsString("Unknown error message"));

                            server.close();
                            async.flag();
                        })));
    }

    @Test
    public void testFeatureCompletionWithWellFormattedError(Vertx vertx, VertxTestContext context) throws ExecutionException, InterruptedException {
        HttpServer server = mockApi(vertx, 500, "{\"message\": \"This is the error\"}");

        KafkaConnectApi api = new KafkaConnectApiImpl(vertx);

        Checkpoint async = context.checkpoint();
        api.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.actualPort(), "my-connector", new JsonObject())
                .onComplete(context.failing(res -> context.verify(() -> {
                    assertThat(res.getMessage(), containsString("This is the error"));

                    server.close();
                    async.flag();
                })));
    }

    @Test
    public void testListConnectLoggersWithLevel(Vertx vertx, VertxTestContext context) throws Exception {
        final HttpServer server = mockApi(vertx, 200, new ObjectMapper().writeValueAsString(
            Map.of(
                "org.apache.kafka.connect",
                Map.of(
                    "level", "INFO"
                )
            )
        ));
        final KafkaConnectApi api = new KafkaConnectApiImpl(vertx);
        final Checkpoint async = context.checkpoint();
        api.listConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.actualPort())
                .onComplete(context.succeeding(res -> context.verify(() -> {
                    assertThat(res, allOf(
                        aMapWithSize(1),
                        hasEntry("org.apache.kafka.connect", "INFO")
                    ));
                    server.close();
                    async.flag();
                })));
    }

    @Test
    public void testListConnectLoggersWithLevelAndLastModified(Vertx vertx, VertxTestContext context) throws Exception {
        final HttpServer server = mockApi(vertx, 200, new ObjectMapper().writeValueAsString(
            Map.of(
                "org.apache.kafka.connect",
                Map.of(
                    "level", "WARN",
                    "last_modified", "2020-01-01T00:00:00.000Z"
                )
            )
        ));
        final KafkaConnectApi api = new KafkaConnectApiImpl(vertx);
        final Checkpoint async = context.checkpoint();
        api.listConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.actualPort())
                .onComplete(context.succeeding(res -> context.verify(() -> {
                    assertThat(res, allOf(
                        aMapWithSize(1),
                        hasEntry("org.apache.kafka.connect", "WARN")
                    ));
                    server.close();
                    async.flag();
                })));
    }

    HttpServer mockApi(Vertx vertx, int status, String body) throws InterruptedException, ExecutionException {
        HttpServer httpServer = vertx.createHttpServer().requestHandler(request -> request.response().setStatusCode(status).end(body));
        return httpServer.listen(0).toCompletionStage().toCompletableFuture().get();
    }
}