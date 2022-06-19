/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

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

import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
        HttpServer server = mockApi(vertx, "Some error message");

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
        HttpServer server = mockApi(vertx, "{\"message\": \"This is the error\"}");

        KafkaConnectApi api = new KafkaConnectApiImpl(vertx);

        Checkpoint async = context.checkpoint();
        api.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "127.0.0.1", server.actualPort(), "my-connector", new JsonObject())
                .onComplete(context.failing(res -> context.verify(() -> {
                    assertThat(res.getMessage(), containsString("This is the error"));

                    server.close();
                    async.flag();
                })));
    }

    HttpServer mockApi(Vertx vertx, String error) throws InterruptedException, ExecutionException {
        HttpServer httpServer = vertx.createHttpServer().requestHandler(request -> request.response().setStatusCode(500).end(error));

        return httpServer.listen(0).toCompletionStage().toCompletableFuture().get();
    }
}