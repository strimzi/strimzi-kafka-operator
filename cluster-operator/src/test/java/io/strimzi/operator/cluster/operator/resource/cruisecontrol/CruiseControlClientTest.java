/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlRebalanceKeys;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
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

    private static final String HOST = "localhost";

    private static final boolean API_AUTH_ENABLED = true;
    private static final boolean API_SSL_ENABLED = true;

    private static int cruiseControlPort;
    private static MockCruiseControl cruiseControlServer;

    @BeforeAll
    public static void setupServer() throws IOException {
        cruiseControlPort = TestUtils.getFreePort();
        File tlsKeyFile = TestUtils.tempFile(CruiseControlClientTest.class.getSimpleName(), ".key");
        File tlsCrtFile = TestUtils.tempFile(CruiseControlClientTest.class.getSimpleName(), ".crt");
        
        new MockCertManager().generateSelfSignedCert(tlsKeyFile, tlsCrtFile,
            new Subject.Builder().withCommonName("Trusted Test CA").build(), 365);

        cruiseControlServer = new MockCruiseControl(cruiseControlPort, tlsKeyFile, tlsCrtFile);
    }

    @AfterAll
    public static void stopServer() {
        if (cruiseControlServer != null && cruiseControlServer.isRunning()) {
            cruiseControlServer.stop();
        }
    }

    @BeforeEach
    public void resetServer() {
        if (cruiseControlServer != null && cruiseControlServer.isRunning()) {
            cruiseControlServer.reset();
        }
    }

    private CruiseControlApi cruiseControlClientProvider(Vertx vertx) {
        return new CruiseControlApiImpl(vertx, HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS, MockCruiseControl.CC_SECRET, MockCruiseControl.CC_API_SECRET, API_AUTH_ENABLED, API_SSL_ENABLED);
    }

    @Test
    public void testGetCCState(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        cruiseControlServer.setupCCStateResponse();

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        client.getCruiseControlState(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, false)
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

        cruiseControlServer.setupCCUserTasksResponseNoGoals(0, 0);

        CruiseControlApi client = cruiseControlClientProvider(vertx);
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID;

        Checkpoint checkpoint = context.checkpoint();
        client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID).onComplete(context.succeeding(result -> {
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
        cruiseControlServer.setupCCRebalanceResponse(pendingCalls, endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RebalanceOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case ADD_BROKER:
                client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
        }
    }

    private void ccRebalanceVerbose(Vertx vertx, VertxTestContext context, int pendingCalls, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion) throws IOException, URISyntaxException {
        cruiseControlServer.setupCCRebalanceResponse(pendingCalls, endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RebalanceOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case ADD_BROKER:
                client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, null)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
        }
    }

    private void ccRebalanceNotEnoughValidWindowsException(Vertx vertx, VertxTestContext context, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion) throws IOException, URISyntaxException {
        cruiseControlServer.setupCCRebalanceNotEnoughDataError(endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RebalanceOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case ADD_BROKER:
                client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
        }
    }

    private void ccRebalanceProposalNotReady(Vertx vertx, VertxTestContext context, int pendingCalls, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion) throws IOException, URISyntaxException {
        cruiseControlServer.setupCCRebalanceResponse(pendingCalls, endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RebalanceOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case ADD_BROKER:
                client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .onComplete(context.succeeding(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
        }
    }

    private void ccBrokerDoesNotExist(Vertx vertx, VertxTestContext context, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<Throwable> assertion) throws IOException, URISyntaxException {
        cruiseControlServer.setupCCBrokerDoesNotExist(endpoint);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Checkpoint checkpoint = context.checkpoint();
        switch (endpoint) {
            case ADD_BROKER:
                client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, MockCruiseControl.BROKERS_NOT_EXIST_ERROR)
                        .onComplete(context.failing(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            case REMOVE_BROKER:
                client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, MockCruiseControl.BROKERS_NOT_EXIST_ERROR)
                        .onComplete(context.failing(result -> context.verify(() -> {
                            assertion.accept(result);
                            checkpoint.flag();
                        })));
                break;
            default:
                throw new IllegalArgumentException("The " + endpoint + " endpoint is invalid for this test");
        }
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

    @Test
    public void testMockCCServerPendingCallsOverride(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        CruiseControlApi client = cruiseControlClientProvider(vertx);
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID;

        int pendingCalls1 = 2;
        Checkpoint firstPending = context.checkpoint(pendingCalls1);
        int pendingCalls2 = 4;
        Checkpoint secondPending = context.checkpoint(pendingCalls2);

        //When last checkpoint is flagged, then test is marked as completed, so create another checkpoint
        //for test end to prevent premature test success
        Checkpoint completeTest = context.checkpoint();

        cruiseControlServer.setupCCUserTasksResponseNoGoals(0, pendingCalls1);

        Future<CruiseControlResponse> statusFuture = client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);

        for (int i = 1; i <= pendingCalls1; i++) {
            statusFuture = statusFuture.compose(response -> {
                context.verify(() -> assertThat(
                    response.getJson().getString("Status"),
                    is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                );
                firstPending.flag();
                return client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);
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
                cruiseControlServer.reset();
                cruiseControlServer.setupCCUserTasksResponseNoGoals(0, pendingCalls2);
            } catch (IOException | URISyntaxException e) {
                return Future.failedFuture(e);
            }
            return Future.succeededFuture();
        });

        statusFuture = statusFuture.compose(ignore -> client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID));

        for (int i = 1; i <= pendingCalls2; i++) {
            statusFuture = statusFuture.compose(response -> {
                context.verify(() -> assertThat(
                    response.getJson().getString("Status"),
                    is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                );
                secondPending.flag();
                return client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);
            });
        }

        statusFuture.compose(response -> {
            context.verify(() -> assertThat(
                response.getJson().getString("Status"),
                is(CruiseControlUserTaskStatus.COMPLETED.toString()))
            );
            return Future.succeededFuture(response);
        }).onComplete(context.succeeding(result -> completeTest.flag()));
    }

    private void runTest(Vertx vertx, VertxTestContext context, String userTaskID, int pendingCalls) throws IOException, URISyntaxException {
        cruiseControlServer.setupCCUserTasksResponseNoGoals(0, pendingCalls);

        CruiseControlApi client = cruiseControlClientProvider(vertx);

        Future<CruiseControlResponse> statusFuture = client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);

        // One interaction is always expected at the end of the test, hence the +1
        Checkpoint expectedInteractions = context.checkpoint(pendingCalls + 1);

        for (int i = 1; i <= pendingCalls; i++) {
            statusFuture = statusFuture.compose(response -> {
                context.verify(() -> assertThat(
                    response.getJson().getString("Status"),
                    is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()))
                );
                expectedInteractions.flag();
                return client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);
            });
        }

        statusFuture.compose(response -> {
            context.verify(() -> assertThat(
                response.getJson().getString("Status"),
                is(CruiseControlUserTaskStatus.COMPLETED.toString()))
            );
            expectedInteractions.flag();
            return Future.succeededFuture(response);
        });
    }
}
