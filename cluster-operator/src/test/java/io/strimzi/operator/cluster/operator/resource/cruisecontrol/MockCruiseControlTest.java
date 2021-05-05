/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class MockCruiseControlTest extends CruiseControlBase {

    private void runTest(Vertx vertx, VertxTestContext context, String userTaskID, int pendingCalls) throws IOException, URISyntaxException {
        ClientAndServer ccServer = MockCruiseControlUtils.server(PORT_NUMBERS.getUniqueNumber());

        MockCruiseControlUtils.setupCCUserTasksResponseNoGoals(ccServer, 0, pendingCalls);

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        Future<CruiseControlResponse> statusFuture = client.getUserTaskStatus(HOST, ccServer.getPort(), userTaskID);

        Checkpoint checkpoint = context.checkpoint(pendingCalls + 1);

        for (int i = 1; i <= pendingCalls; i++) {
            statusFuture = statusFuture.compose(response -> {
                context.verify(() -> assertThat(
                        response.getJson().getString("Status"),
                        is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                );
                checkpoint.flag();
                return client.getUserTaskStatus(HOST, ccServer.getPort(), userTaskID);
            });
        }

        statusFuture.compose(response -> {
            context.verify(() -> assertThat(
                    response.getJson().getString("Status"),
                    is(CruiseControlUserTaskStatus.COMPLETED.toString()))
            );
            checkpoint.flag();
            ccServer.stop();
            return Future.succeededFuture(response);
        });
    }

    @ParallelTest
    public void testCCUserTaskNoDelay(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControlUtils.REBALANCE_NO_GOALS_RESPONSE_UTID, 0);
    }

    @ParallelTest
    public void testCCUserTaskNoDelayVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControlUtils.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, 0);
    }

    @ParallelTest
    public void testCCUserTaskDelay(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControlUtils.REBALANCE_NO_GOALS_RESPONSE_UTID, 3);
    }

    @ParallelTest
    public void testCCUserTaskDelayVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        runTest(vertx, context, MockCruiseControlUtils.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, 3);
    }

    @ParallelTest
    public void testMockCCServerPendingCallsOverride(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        ClientAndServer ccServer = MockCruiseControlUtils.server(PORT_NUMBERS.getUniqueNumber());
        CruiseControlApi client = new CruiseControlApiImpl(vertx);
        String userTaskID = MockCruiseControlUtils.REBALANCE_NO_GOALS_RESPONSE_UTID;

        int pendingCalls1 = 2;
        Checkpoint firstPending = context.checkpoint(pendingCalls1);
        int pendingCalls2 = 4;
        Checkpoint secondPending = context.checkpoint(pendingCalls2);

        MockCruiseControlUtils.setupCCUserTasksResponseNoGoals(ccServer, 0, pendingCalls1);

        Future<CruiseControlResponse> statusFuture = client.getUserTaskStatus(HOST, ccServer.getPort(), userTaskID);

        for (int i = 1; i <= pendingCalls1; i++) {
            statusFuture = statusFuture.compose(response -> {
                context.verify(() -> assertThat(
                        response.getJson().getString("Status"),
                        is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                );
                firstPending.flag();
                return client.getUserTaskStatus(HOST, ccServer.getPort(), userTaskID);
            });
        }

        statusFuture = statusFuture.compose(response -> {
            context.verify(() -> assertThat(
                    response.getJson().getString("Status"),
                    is(CruiseControlUserTaskStatus.COMPLETED.toString()))
            );
            return Future.succeededFuture(response);
        });

        statusFuture = statusFuture.compose(response -> {
            try {
                ccServer.reset();
                MockCruiseControlUtils.setupCCUserTasksResponseNoGoals(ccServer, 0, pendingCalls2);
            } catch (IOException e) {
                return Future.failedFuture(e);
            } catch (URISyntaxException e) {
                return Future.failedFuture(e);
            }
            return Future.succeededFuture();
        });

        statusFuture = statusFuture.compose(ignore -> client.getUserTaskStatus(HOST, ccServer.getPort(), userTaskID));

        for (int i = 1; i <= pendingCalls2; i++) {
            statusFuture = statusFuture.compose(response -> {
                context.verify(() -> assertThat(
                        response.getJson().getString("Status"),
                        is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                );
                secondPending.flag();
                return client.getUserTaskStatus(HOST, ccServer.getPort(), userTaskID);
            });
        }

        statusFuture.compose(response -> {
            context.verify(() -> assertThat(
                    response.getJson().getJsonArray("userTasks").getJsonObject(0).getString("Status"),
                    is(CruiseControlUserTaskStatus.COMPLETED.toString()))
            );
            return Future.succeededFuture(response);
        }).onComplete(context.succeeding(result -> {
            ccServer.stop();
            context.completeNow();
        }));
    }

}
