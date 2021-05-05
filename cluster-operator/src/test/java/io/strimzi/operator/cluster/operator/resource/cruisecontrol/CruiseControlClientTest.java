/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;

import java.io.IOException;
import java.net.URISyntaxException;

import static io.strimzi.operator.cluster.JSONObjectMatchers.hasEntry;
import static io.strimzi.operator.cluster.JSONObjectMatchers.hasKey;
import static io.strimzi.operator.cluster.JSONObjectMatchers.hasKeys;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi.CC_REST_API_SUMMARY;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class CruiseControlClientTest extends CruiseControlBase {

    private static final String HOST = "localhost";

    @ParallelTest
    public void testGetCCState(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        ClientAndServer ccServer = MockCruiseControlUtils.server(PORT_NUMBERS.getUniqueNumber());

        MockCruiseControlUtils.setupCCStateResponse(ccServer);

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.getCruiseControlState(HOST, ccServer.getPort(), false)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getJson().getJsonObject("ExecutorState"),
                        hasEntry("state", "NO_TASK_IN_PROGRESS"));
                checkpoint.flag();
                ccServer.stop();
            })));
    }

    @ParallelTest
    public void testCCRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        ClientAndServer ccServer = MockCruiseControlUtils.server(PORT_NUMBERS.getUniqueNumber());

        MockCruiseControlUtils.setupCCRebalanceResponse(ccServer, 0);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, ccServer.getPort(), rbOptions, null)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getUserTaskId(), is(MockCruiseControlUtils.REBALANCE_NO_GOALS_RESPONSE_UTID));
                assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
                checkpoint.flag();
                ccServer.stop();
            })));
    }

    @ParallelTest
    public void testCCRebalanceVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        ClientAndServer ccServer = MockCruiseControlUtils.server(PORT_NUMBERS.getUniqueNumber());

        MockCruiseControlUtils.setupCCRebalanceResponse(ccServer, 0);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().withVerboseResponse().build();

        CruiseControlApi client = new CruiseControlApiImpl(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, ccServer.getPort(), rbOptions, null)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getUserTaskId(), is(MockCruiseControlUtils.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                assertThat(result.getJson(), hasKeys("summary", "goalSummary", "proposals", "loadAfterOptimization", "loadBeforeOptimization"));
                checkpoint.flag();
                ccServer.stop();
            })));
    }

    @ParallelTest
    public void testCCRebalanceNotEnoughValidWindowsException(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        ClientAndServer ccServer = MockCruiseControlUtils.server(PORT_NUMBERS.getUniqueNumber());

        MockCruiseControlUtils.setupCCRebalanceNotEnoughDataError(ccServer);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApiImpl client = new CruiseControlApiImpl(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, ccServer.getPort(), rbOptions, MockCruiseControlUtils.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
            .onComplete(context.succeeding(result -> {
                context.verify(() -> assertThat(result.isNotEnoughDataForProposal(), is(true)));
                checkpoint.flag();
                ccServer.stop();
            }));
    }

    @ParallelTest
    public void testCCRebalancePropsosalNotReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        ClientAndServer ccServer = MockCruiseControlUtils.server(PORT_NUMBERS.getUniqueNumber());

        MockCruiseControlUtils.setupCCRebalanceResponse(ccServer, 1);

        RebalanceOptions rbOptions = new RebalanceOptions.RebalanceOptionsBuilder().build();

        CruiseControlApiImpl client = new CruiseControlApiImpl(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.rebalance(HOST, ccServer.getPort(), rbOptions, MockCruiseControlUtils.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
            .onComplete(context.succeeding(result -> {
                context.verify(() -> assertThat(result.isProposalStillCalaculating(), is(true)));
                checkpoint.flag();
                ccServer.stop();
            }));
    }

    @ParallelTest
    public void testCCGetRebalanceUserTask(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        ClientAndServer ccServer = MockCruiseControlUtils.server(PORT_NUMBERS.getUniqueNumber());

        MockCruiseControlUtils.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);

        CruiseControlApi client = new CruiseControlApiImpl(vertx);
        String userTaskID = MockCruiseControlUtils.REBALANCE_NO_GOALS_RESPONSE_UTID;

        Checkpoint checkpoint = context.checkpoint();
        client.getUserTaskStatus(HOST, ccServer.getPort(), userTaskID)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getUserTaskId(), is(MockCruiseControlUtils.USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID));
                assertThat(result.getJson().getJsonObject(CC_REST_API_SUMMARY), is(notNullValue()));
                checkpoint.flag();
                ccServer.stop();
            })));
    }

    @ParallelTest
    public void testCCGetRebalanceVerboseUserTask(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        ClientAndServer ccServer = MockCruiseControlUtils.server(PORT_NUMBERS.getUniqueNumber());

        MockCruiseControlUtils.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);

        CruiseControlApi client = new CruiseControlApiImpl(vertx);
        String userTaskID = MockCruiseControlUtils.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID;

        Checkpoint checkpoint = context.checkpoint();
        client.getUserTaskStatus(HOST, ccServer.getPort(), userTaskID)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getUserTaskId(), is(MockCruiseControlUtils.USER_TASK_REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                assertThat(result.getJson(), hasKey(CC_REST_API_SUMMARY));
                assertThat(result.getJson().getJsonObject(CC_REST_API_SUMMARY), is(notNullValue()));
                checkpoint.flag();
                ccServer.stop();
            })));
    }
}
