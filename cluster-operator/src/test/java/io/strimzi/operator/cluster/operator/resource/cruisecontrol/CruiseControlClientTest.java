/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
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

import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi.CC_REST_API_SUMMARY;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class CruiseControlClientTest {

    private static final int PORT = 1080;
    private static final String HOST = "localhost";

    private static ClientAndServer ccServer;

    @BeforeAll
    public static void setupServer() throws IOException, URISyntaxException {
        ccServer = MockCruiseControl.getCCServer(PORT);
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

        Checkpoint checkpoint = context.checkpoint();
        client.getCruiseControlState(HOST, PORT, false).onComplete(context.succeeding(result -> {
            context.verify(() -> assertThat(
                    result.getJson().getJsonObject("ExecutorState").getString("state"),
                    is("NO_TASK_IN_PROGRESS")));
            checkpoint.flag();
        }));
    }

    @Test
    public void testCCRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, PORT, rbOptions, null).onComplete(context.succeeding(result -> {
            context.verify(() -> assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID)));
            context.verify(() -> assertThat(result.getJson().containsKey("summary"), is(true)));
            context.verify(() -> assertThat(result.getJson().containsKey("goalSummary"), is(true)));
            context.verify(() -> assertThat(result.getJson().containsKey("loadAfterOptimization"), is(true)));
            checkpoint.flag();
        }));
    }

    @Test
    public void testCCRebalanceVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().withVerboseResponse().build();

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, PORT, rbOptions, null).onComplete(context.succeeding(result -> {
            context.verify(() -> assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID)));
            context.verify(() -> assertThat(result.getJson().containsKey("summary"), is(true)));
            context.verify(() -> assertThat(result.getJson().containsKey("goalSummary"), is(true)));
            context.verify(() -> assertThat(result.getJson().containsKey("proposals"), is(true)));
            context.verify(() -> assertThat(result.getJson().containsKey("loadAfterOptimization"), is(true)));
            context.verify(() -> assertThat(result.getJson().containsKey("loadBeforeOptimization"), is(true)));
            checkpoint.flag();
        }));
    }

    @Test
    public void testCCRebalanceNotEnoughValidWindowsException(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApiImpl client = new CruiseControlApiImpl(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, PORT, rbOptions, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                .onComplete(context.succeeding(result -> {
                    context.verify(() -> assertTrue(result.thereIsNotEnoughDataForProposal()));
                    checkpoint.flag();
                }));
    }

    @Test
    public void testCCRebalancePropsosalNotReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCRebalanceResponse(ccServer, 1);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApiImpl client = new CruiseControlApiImpl(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, PORT, rbOptions, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                .onComplete(context.succeeding(result -> {
                    context.verify(() -> assertTrue(result.proposalIsStillCalculating()));
                    checkpoint.flag();
                }));
    }

    @Test
    public void testCCGetRebalanceUserTask(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);

        CruiseControlApi client = new CruiseControlApiImpl(vertx);
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID;

        Checkpoint checkpoint = context.checkpoint();
        client.getUserTaskStatus(HOST, PORT, userTaskID).onComplete(context.succeeding(result -> {
            context.verify(() -> assertThat(result.getUserTaskId(), is(MockCruiseControl.USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID)));
            context.verify(() -> assertThat(result.getJson().getJsonObject(CC_REST_API_SUMMARY), is(notNullValue())));
            checkpoint.flag();
        }));
    }

    @Test
    public void testCCGetRebalanceVerboseUserTask(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);

        CruiseControlApi client = new CruiseControlApiImpl(vertx);
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID;

        Checkpoint checkpoint = context.checkpoint();
        client.getUserTaskStatus(HOST, PORT, userTaskID).onComplete(context.succeeding(result -> {
            context.verify(() -> assertThat(result.getUserTaskId(), is(MockCruiseControl.USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID)));
            context.verify(() -> assertThat(result.getJson().getJsonObject(CC_REST_API_SUMMARY), is(notNullValue())));
            checkpoint.flag();
        }));
    }

}
