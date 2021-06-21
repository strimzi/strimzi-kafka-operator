/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;

import java.io.IOException;
import java.net.URISyntaxException;

import static io.strimzi.operator.cluster.JSONObjectMatchers.hasEntry;
import static io.strimzi.operator.cluster.JSONObjectMatchers.hasKeys;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class CruiseControlClientTest {

    private static final String HOST = "localhost";

    private static ClientAndServer ccServer;

    @BeforeAll
    public static void setupServer() throws IOException {
        ccServer = MockCruiseControl.server(0);
    }

    @AfterAll
    public static void stopServer() {
        ccServer.stop();
    }

    @BeforeEach
    public void resetServer() {
        ccServer.reset();
    }

    @Test
    public void testGetCCState(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCStateResponse(ccServer);

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        client.getCruiseControlState(HOST, ccServer.getPort(), false)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getJson().getJsonObject("ExecutorState"),
                        hasEntry("state", "NO_TASK_IN_PROGRESS"));
                context.completeNow();
            })));
    }

    @Test
    public void testCCRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        client.rebalance(HOST, ccServer.getPort(), rbOptions, null)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID));
                assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
                context.completeNow();
            })));
    }

    @Test
    public void testCCRebalanceVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().withVerboseResponse().build();

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        client.rebalance(HOST, ccServer.getPort(), rbOptions, null)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                assertThat(result.getJson(), hasKeys("summary", "goalSummary", "proposals", "loadAfterOptimization", "loadBeforeOptimization"));
                context.completeNow();
            })));
    }

    @Test
    public void testCCRebalanceNotEnoughValidWindowsException(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApiImpl client = new CruiseControlApiImpl(vertx);

        client.rebalance(HOST, ccServer.getPort(), rbOptions, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                .onComplete(context.succeeding(result -> {
                    context.verify(() -> assertThat(result.isNotEnoughDataForProposal(), is(true)));
                    context.completeNow();
                }));
    }

    @Test
    public void testCCRebalancePropsosalNotReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 1);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApiImpl client = new CruiseControlApiImpl(vertx);

        client.rebalance(HOST, ccServer.getPort(), rbOptions, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
            .onComplete(context.succeeding(result -> {
                context.verify(() -> assertThat(result.isProposalStillCalaculating(), is(true)));
                context.completeNow();
            }));
    }

    @Test
    public void testCCGetRebalanceUserTask(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);

        CruiseControlApi client = new CruiseControlApiImpl(vertx);
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID;

        client.getUserTaskStatus(HOST, ccServer.getPort(), userTaskID).onComplete(context.succeeding(result -> {
            context.verify(() -> assertThat(result.getUserTaskId(), is(MockCruiseControl.USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID)));
            context.verify(() -> assertThat(result.getJson().getJsonObject(CruiseControlRebalanceKeys.SUMMARY.getKey()), is(notNullValue())));
            context.completeNow();
        }));
    }
}
