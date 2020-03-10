/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly.cruisecontrol;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class MockCruiseControlTest {

    private static final int PORT = 1080;
    private static final String HOST = "localhost";

    private static ClientAndServer ccServer;

    private void runTest(Vertx vertx, VertxTestContext context, String userTaskID, int pendingCalls) throws IOException, URISyntaxException {

        ccServer = MockCruiseControl.getCCServer(PORT, pendingCalls);

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        Future<CruiseControlResponse> statusFuture = client.getUserTaskStatus(HOST, PORT, userTaskID);

        if (pendingCalls > 0) {
            for (int i = 1; i <= pendingCalls; i++) {
                statusFuture = statusFuture.compose(response -> {
                    context.verify(() -> assertThat(
                            response.getJson().getJsonArray("userTasks").getJsonObject(0).getString("Status"),
                            is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                    );
                    return client.getUserTaskStatus(HOST, PORT, userTaskID);
                });
            }
        }

        statusFuture.compose(response -> {
            context.verify(() -> assertThat(
                    response.getJson().getJsonArray("userTasks").getJsonObject(0).getString("Status"),
                    is(CruiseControlUserTaskStatus.COMPLETED.toString()))
            );
            return Future.succeededFuture(response);
        }).setHandler(context.succeeding(result -> {
            ccServer.stop();
            context.completeNow();
        }));
    }

    @Test
    public void testCCUserTaskNoDelay(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, 0);
    }

    @Test
    public void testCCUserTaskNoDelayVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, 0);
    }

    @Test
    public void testCCUserTaskDelay(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, 3);
    }

    @Test
    public void testCCUserTaskDelayVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, 3);
    }
}
