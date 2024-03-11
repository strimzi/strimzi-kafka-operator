/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlRebalanceKeys;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.function.Consumer;

import static io.strimzi.operator.cluster.JSONObjectMatchers.hasEntry;
import static io.strimzi.operator.cluster.JSONObjectMatchers.hasKeys;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl.HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class CruiseControlClientTest {

    private static final int PORT = 1080;
    private static final String HOST = "localhost";

    private static final boolean API_AUTH_ENABLED = true;
    private static final boolean API_SSL_ENABLED = true;

    private static MockCruiseControl ccServer;

    @BeforeAll
    public static void setupServer() throws IOException {
        ccServer = new MockCruiseControl(PORT);
    }

    @AfterAll
    public static void stopServer() {
        ccServer.stop();
    }

    @BeforeEach
    public void resetServer() {
        ccServer.reset();
    }

    private CruiseControlApi cruiseControlClientProvider(Vertx vertx) {
        return new CruiseControlApiImpl(vertx, HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS, MockCruiseControl.CC_SECRET, MockCruiseControl.CC_API_SECRET, API_AUTH_ENABLED, API_SSL_ENABLED);
    }

    @Test
    public void testGetCCState(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        ccServer.setupCCStateResponse();

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.getCruiseControlState(HOST, PORT, false)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat(result.getJson().getJsonObject("ExecutorState"),
                        hasEntry("state", "NO_TASK_IN_PROGRESS"));
                checkpoint.flag();
            })));
    }

    @Test
    public void testCCRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder().build();
        this.ccRebalance(vertx, context, 0, options, CruiseControlEndpoints.REBALANCE,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
                });
    }

    @Test
    public void testCCRebalanceVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder().withVerboseResponse().build();
        this.ccRebalanceVerbose(vertx, context, 0, options, CruiseControlEndpoints.REBALANCE,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "proposals", "loadAfterOptimization", "loadBeforeOptimization"));
                });
    }

    @Test
    public void testCCRebalanceNotEnoughValidWindowsException(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder().build();
        this.ccRebalanceNotEnoughValidWindowsException(vertx, context, options, CruiseControlEndpoints.REBALANCE,
                result -> assertThat(result.isNotEnoughDataForProposal(), is(true))
        );
    }

    @Test
    public void testCCRebalanceProposalNotReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder().build();
        this.ccRebalanceProposalNotReady(vertx, context, 1, options, CruiseControlEndpoints.REBALANCE,
                result ->  assertThat(result.isProposalStillCalculating(), is(true))
        );
    }

    @Test
    public void testCCGetRebalanceUserTask(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        ccServer.setupCCUserTasksResponseNoGoals(0, 0);

        CruiseControlApi client = cruiseControlClientProvider(vertx);
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID;

        Checkpoint checkpoint = context.checkpoint();
        client.getUserTaskStatus(HOST, PORT, userTaskID).onComplete(context.succeeding(result -> {
            context.verify(() -> assertThat(result.getUserTaskId(), is(MockCruiseControl.USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID)));
            context.verify(() -> assertThat(result.getJson().getJsonObject(CruiseControlRebalanceKeys.SUMMARY.getKey()), is(notNullValue())));
            checkpoint.flag();
        }));
    }

    @Test
    public void testCCAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalance(vertx, context, 0, options, CruiseControlEndpoints.ADD_BROKER,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
                });
    }

    @Test
    public void testCCAddBrokerVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withVerboseResponse()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceVerbose(vertx, context, 0, options, CruiseControlEndpoints.ADD_BROKER,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "proposals", "loadAfterOptimization", "loadBeforeOptimization"));
                });
    }

    @Test
    public void testCCAddBrokerNotEnoughValidWindowsException(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceNotEnoughValidWindowsException(vertx, context, options, CruiseControlEndpoints.ADD_BROKER,
                result -> assertThat(result.isNotEnoughDataForProposal(), is(true))
        );
    }

    @Test
    public void testCCAddBrokerProposalNotReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceProposalNotReady(vertx, context, 1, options, CruiseControlEndpoints.ADD_BROKER,
                result -> assertThat(result.isProposalStillCalculating(), is(true))
        );
    }

    @Test
    public void testCCAddBrokerDoesNotExist(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccBrokerDoesNotExist(vertx, context, options, CruiseControlEndpoints.ADD_BROKER,
                result -> {
                    assertThat(result, instanceOf(IllegalArgumentException.class));
                    assertTrue(result.getMessage().contains("Some/all brokers specified don't exist"));
                });
    }

    @Test
    public void testCCRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalance(vertx, context, 0, options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
                });
    }

    @Test
    public void testCCRemoveBrokerVerbose(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withVerboseResponse()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceVerbose(vertx, context, 0, options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "proposals", "loadAfterOptimization", "loadBeforeOptimization"));
                });
    }

    @Test
    public void testCCRemoveBrokerNotEnoughValidWindowsException(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceNotEnoughValidWindowsException(vertx, context, options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> assertThat(result.isNotEnoughDataForProposal(), is(true))
        );
    }

    @Test
    public void testCCRemoveBrokerProposalNotReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceProposalNotReady(vertx, context, 1, options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> assertThat(result.isProposalStillCalculating(), is(true))
        );
    }

    @Test
    public void testCCRemoveBrokerDoesNotExist(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccBrokerDoesNotExist(vertx, context, options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> {
                    assertThat(result, instanceOf(IllegalArgumentException.class));
                    assertTrue(result.getMessage().contains("Some/all brokers specified don't exist"));
                });
    }

    private void ccRebalance(Vertx vertx, VertxTestContext context, int pendingCalls, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(pendingCalls, endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(HOST, PORT, (RebalanceOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case ADD_BROKER:
                client.addBroker(HOST, PORT, (AddBrokerOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(HOST, PORT, (RemoveBrokerOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
        }
    }

    private void ccRebalanceVerbose(Vertx vertx, VertxTestContext context, int pendingCalls, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(pendingCalls, endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(HOST, PORT, (RebalanceOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case ADD_BROKER:
                client.addBroker(HOST, PORT, (AddBrokerOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(HOST, PORT, (RemoveBrokerOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
        }
    }

    private void ccRebalanceNotEnoughValidWindowsException(Vertx vertx, VertxTestContext context, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceNotEnoughDataError(endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(HOST, PORT, (RebalanceOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case ADD_BROKER:
                client.addBroker(HOST, PORT, (AddBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(HOST, PORT, (RemoveBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
        }
    }

    private void ccRebalanceProposalNotReady(Vertx vertx, VertxTestContext context, int pendingCalls, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(pendingCalls, endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(HOST, PORT, (RebalanceOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case ADD_BROKER:
                client.addBroker(HOST, PORT, (AddBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(HOST, PORT, (RemoveBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
        }
    }

    private void ccBrokerDoesNotExist(Vertx vertx, VertxTestContext context, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<Throwable> assertion) throws IOException, URISyntaxException {
        ccServer.setupCCBrokerDoesNotExist(endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case ADD_BROKER:
                client.addBroker(HOST, PORT, (AddBrokerOptions) options, MockCruiseControl.BROKERS_NOT_EXIST_ERROR)
                        .onComplete(context.failing(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(HOST, PORT, (RemoveBrokerOptions) options, MockCruiseControl.BROKERS_NOT_EXIST_ERROR)
                        .onComplete(context.failing(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            default:
                throw new IllegalArgumentException("The " + endpoint + " endpoint is invalid for this test");
        }
    }
}
