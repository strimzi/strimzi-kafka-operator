/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;
import io.strimzi.api.kafka.model.rebalance.BrokerAndVolumeIds;
import io.strimzi.api.kafka.model.rebalance.BrokerAndVolumeIdsBuilder;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlRebalanceKeys;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.strimzi.operator.cluster.JSONObjectMatchers.hasEntry;
import static io.strimzi.operator.cluster.JSONObjectMatchers.hasKeys;
import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl.HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CruiseControlClientTest {

    private static final String HOST = "localhost";

    private static final boolean API_AUTH_ENABLED = true;
    private static final boolean API_SSL_ENABLED = true;

    private static int cruiseControlPort;
    private static MockCruiseControl cruiseControlServer;

    @BeforeAll
    public static void setupServer() throws IOException {
        cruiseControlPort = TestUtils.getFreePort();
        File tlsKeyFile = ReadWriteUtils.tempFile(CruiseControlClientTest.class.getSimpleName(), ".key");
        File tlsCrtFile = ReadWriteUtils.tempFile(CruiseControlClientTest.class.getSimpleName(), ".crt");
        
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

    private CruiseControlApi cruiseControlClientProvider() {
        return new CruiseControlApiImpl(HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS, MockCruiseControl.CC_SECRET, MockCruiseControl.CC_API_SECRET, API_AUTH_ENABLED, API_SSL_ENABLED);
    }

    @Test
    public void testGetCCState() {
        cruiseControlServer.mockStateEndpoint(null, false);

        CruiseControlApi client = cruiseControlClientProvider();
        client.getCruiseControlState(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, false)
                .whenComplete((result, ex) -> assertThat(result.getExecutorStatus().getJson(),
                        hasEntry("state", "NO_TASK_IN_PROGRESS"))).join();
    }

    @Test
    public void testCCRebalance()  {
        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder().build();
        this.ccRebalance(0, options, CruiseControlEndpoints.REBALANCE, result -> {
            assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID));
            assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
        });
    }
    @Test
    public void testCCRebalanceVerbose()  {
        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder().withVerboseResponse().build();
        this.ccRebalanceVerbose(0, options, CruiseControlEndpoints.REBALANCE,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "proposals", "loadAfterOptimization", "loadBeforeOptimization"));
                });
    }

    @Test
    public void testCCRebalanceNotEnoughValidWindowsException()  {
        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder().build();
        this.ccRebalanceNotEnoughValidWindowsException(options, CruiseControlEndpoints.REBALANCE,
                result -> assertThat(result.isNotEnoughDataForProposal(), is(true))
        );
    }

    @Test
    public void testCCRebalanceProposalNotReady()  {
        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder().build();
        this.ccRebalanceProposalNotReady(1, options, CruiseControlEndpoints.REBALANCE,
                result ->  assertThat(result.isProposalStillCalculating(), is(true))
        );
    }

    @Test
    public void testCCGetRebalanceUserTask()  {

        cruiseControlServer.setupCCUserTasksResponseNoGoals(0, 0);

        CruiseControlApi client = cruiseControlClientProvider();
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID;

        client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID).whenComplete((result, ex) -> {
            assertThat(result.getUserTaskId(), is(MockCruiseControl.USER_TASK_REBALANCE_NO_GOALS_RESPONSE_UTID));
            assertThat(result.getJson().get(CruiseControlRebalanceKeys.SUMMARY.getKey()), is(notNullValue()));
        }).join();
    }

    @Test
    public void testCCAddBroker()  {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalance(0, options, CruiseControlEndpoints.ADD_BROKER,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
                });
    }

    @Test
    public void testCCAddBrokerVerbose()  {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withVerboseResponse()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceVerbose(0, options, CruiseControlEndpoints.ADD_BROKER,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "proposals", "loadAfterOptimization", "loadBeforeOptimization"));
                });
    }

    @Test
    public void testCCAddBrokerNotEnoughValidWindowsException()  {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceNotEnoughValidWindowsException(options, CruiseControlEndpoints.ADD_BROKER,
                result -> assertThat(result.isNotEnoughDataForProposal(), is(true))
        );
    }

    @Test
    public void testCCAddBrokerProposalNotReady()  {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceProposalNotReady(1, options, CruiseControlEndpoints.ADD_BROKER,
                result -> assertThat(result.isProposalStillCalculating(), is(true))
        );
    }

    @Test
    public void testCCAddBrokerDoesNotExist()  {
        AddBrokerOptions options = new AddBrokerOptions.AddBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccBrokerDoesNotExist(options, CruiseControlEndpoints.ADD_BROKER,
                result -> {
                    assertThat(result, instanceOf(IllegalArgumentException.class));
                    assertTrue(result.getMessage().contains("Some/all brokers specified don't exist"));
                });
    }

    @Test
    public void testCCRemoveBroker()  {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalance(0, options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
                });
    }

    @Test
    public void testCCRemoveBrokerDisks()  {
        BrokerAndVolumeIds brokerAndVolumeIds = new BrokerAndVolumeIdsBuilder()
                .withBrokerId(0)
                .withVolumeIds(1, 2, 3)
                .build();

        RemoveDisksOptions options = new RemoveDisksOptions.RemoveDisksOptionsBuilder()
                .withBrokersandVolumeIds(List.of(brokerAndVolumeIds))
                .build();
        this.ccRebalance(0, options, CruiseControlEndpoints.REMOVE_DISKS,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "loadAfterOptimization"));
                });
    }

    @Test
    public void testCCMoveReplicasOffVolumesProposalNotReady()  {
        BrokerAndVolumeIds brokerAndVolumeIds = new BrokerAndVolumeIdsBuilder()
                .withVolumeIds(1)
                .withBrokerId(0)
                .build();

        RemoveDisksOptions options = new RemoveDisksOptions.RemoveDisksOptionsBuilder()
                .withBrokersandVolumeIds(List.of(brokerAndVolumeIds))
                .build();
        this.ccRebalanceProposalNotReady(1, options, CruiseControlEndpoints.REMOVE_DISKS,
                result -> assertThat(result.isProposalStillCalculating(), is(true))
        );
    }

    @Test
    public void testCCMoveReplicasOffVolumesNotEnoughValidWindowsException()  {
        BrokerAndVolumeIds brokerAndVolumeIds = new BrokerAndVolumeIdsBuilder()
                .withVolumeIds(1)
                .withBrokerId(0)
                .build();

        RemoveDisksOptions options = new RemoveDisksOptions.RemoveDisksOptionsBuilder()
                .withBrokersandVolumeIds(List.of(brokerAndVolumeIds))
                .build();
        this.ccRebalanceNotEnoughValidWindowsException(options, CruiseControlEndpoints.REMOVE_DISKS,
                result -> assertThat(result.isNotEnoughDataForProposal(), is(true))
        );
    }

    @Test
    public void testCCMoveReplicasOffVolumesBrokerDoesNotExist()  {
        BrokerAndVolumeIds brokerAndVolumeIds = new BrokerAndVolumeIdsBuilder()
                .withVolumeIds(1)
                .withBrokerId(0)
                .build();

        RemoveDisksOptions options = new RemoveDisksOptions.RemoveDisksOptionsBuilder()
                .withBrokersandVolumeIds(List.of(brokerAndVolumeIds))
                .build();
        this.ccBrokerDoesNotExist(options, CruiseControlEndpoints.REMOVE_DISKS,
                result -> {
                    assertThat(result, instanceOf(IllegalArgumentException.class));
                    assertTrue(result.getMessage().contains("Some/all brokers specified don't exist"));
                });
    }

    @Test
    public void testCCRemoveBrokerVerbose()  {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withVerboseResponse()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceVerbose(0, options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> {
                    assertThat(result.getUserTaskId(), is(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID));
                    assertThat(result.getJson(), hasKeys("summary", "goalSummary", "proposals", "loadAfterOptimization", "loadBeforeOptimization"));
                });
    }

    @Test
    public void testCCRemoveBrokerNotEnoughValidWindowsException()  {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceNotEnoughValidWindowsException(options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> assertThat(result.isNotEnoughDataForProposal(), is(true))
        );
    }

    @Test
    public void testCCRemoveBrokerProposalNotReady()  {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccRebalanceProposalNotReady(1, options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> assertThat(result.isProposalStillCalculating(), is(true))
        );
    }

    @Test
    public void testCCRemoveBrokerDoesNotExist()  {
        RemoveBrokerOptions options = new RemoveBrokerOptions.RemoveBrokerOptionsBuilder()
                .withBrokers(List.of(3))
                .build();
        this.ccBrokerDoesNotExist(options, CruiseControlEndpoints.REMOVE_BROKER,
                result -> {
                    assertThat(result.getCause(), instanceOf(IllegalArgumentException.class));
                    assertTrue(result.getCause().getMessage().contains("Some/all brokers specified don't exist"));
                });
    }

    private void ccRebalance(int pendingCalls, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion)  {
        if (endpoint == CruiseControlEndpoints.REMOVE_DISKS) {
            cruiseControlServer.setupCCRebalanceResponse(pendingCalls, endpoint, null);
        } else {
            cruiseControlServer.setupCCRebalanceResponse(pendingCalls, endpoint, "false");
        }

        CruiseControlApi client = cruiseControlClientProvider();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RebalanceOptions) options, null)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
            case ADD_BROKER:
                client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, null)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
            case REMOVE_BROKER:
                client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, null)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
            case REMOVE_DISKS:
                client.removeDisks(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveDisksOptions) options, null)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
        }
    }

    private void ccRebalanceVerbose(int pendingCalls, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion)  {
        cruiseControlServer.setupCCRebalanceResponse(pendingCalls, endpoint, "true");

        CruiseControlApi client = cruiseControlClientProvider();
        switch (endpoint) {
            case REBALANCE:
                client.rebalance(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RebalanceOptions) options, null)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
            case ADD_BROKER:
                client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, null)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
            case REMOVE_BROKER:
                client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, null)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
        }
    }

    private void ccRebalanceNotEnoughValidWindowsException(AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion)  {
        if (endpoint == CruiseControlEndpoints.REMOVE_DISKS) {
            cruiseControlServer.setupCCRebalanceNotEnoughDataError(endpoint, null);
        } else {
            cruiseControlServer.setupCCRebalanceNotEnoughDataError(endpoint, "true|false");
        }

        CruiseControlApi client = cruiseControlClientProvider();

        switch (endpoint) {
            case REBALANCE:
                client.rebalance(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RebalanceOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
            case ADD_BROKER:
                client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
            case REMOVE_BROKER:
                client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
            case REMOVE_DISKS:
                client.removeDisks(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveDisksOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
        }
    }

    private void ccRebalanceProposalNotReady(int pendingCalls, AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<CruiseControlRebalanceResponse> assertion)  {
        if (endpoint == CruiseControlEndpoints.REMOVE_DISKS) {
            cruiseControlServer.setupCCRebalanceResponse(pendingCalls, endpoint, null);
        } else {
            cruiseControlServer.setupCCRebalanceResponse(pendingCalls, endpoint, "true|false");
        }

        CruiseControlApi client = cruiseControlClientProvider();

        switch (endpoint) {
            case REBALANCE:
                client.rebalance(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RebalanceOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .whenComplete((result, ex) -> assertion.accept(result)).join();
                break;
            case ADD_BROKER:
                client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .whenComplete((result, ex) -> assertion.accept(result)).join();
                break;
            case REMOVE_BROKER:
                client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .whenComplete((result, ex) -> assertion.accept(result)).join();
                break;
            case REMOVE_DISKS:
                client.removeDisks(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveDisksOptions) options, MockCruiseControl.REBALANCE_NOT_ENOUGH_VALID_WINDOWS_ERROR)
                        .whenComplete((result, ex) -> {
                            assertion.accept(result);
                        }).join();
                break;
        }
    }

    private void ccBrokerDoesNotExist(AbstractRebalanceOptions options, CruiseControlEndpoints endpoint, Consumer<Throwable> assertion)  {
        if (endpoint == CruiseControlEndpoints.REMOVE_DISKS) {
            cruiseControlServer.setupCCBrokerDoesNotExist(endpoint, null);
        } else {
            cruiseControlServer.setupCCBrokerDoesNotExist(endpoint, "true|false");
        }

        CruiseControlApi client = cruiseControlClientProvider();

        switch (endpoint) {
            case ADD_BROKER:
                assertThrows(Exception.class, client.addBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (AddBrokerOptions) options, MockCruiseControl.BROKERS_NOT_EXIST_ERROR)
                        .whenComplete((result, ex) -> assertion.accept(ex))::join);
                break;
            case REMOVE_BROKER:
                assertThrows(Exception.class, client.removeBroker(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveBrokerOptions) options, MockCruiseControl.BROKERS_NOT_EXIST_ERROR)
                        .whenComplete((result, ex) -> assertion.accept(ex))::join);
                break;
            case REMOVE_DISKS:
                assertThrows(Exception.class, client.removeDisks(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, (RemoveDisksOptions) options, MockCruiseControl.BROKERS_NOT_EXIST_ERROR)
                        .whenComplete((result, ex) -> assertion.accept(ex))::join);
                break;
            default:
                throw new IllegalArgumentException("The " + endpoint + " endpoint is invalid for this test");
        }
    }

    @Test
    public void testCCUserTaskNoDelay()  {
        runTest(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, 0);
    }

    @Test
    public void testCCUserTaskNoDelayVerbose()  {
        runTest(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, 0);
    }

    @Test
    public void testCCUserTaskDelay()  {
        runTest(MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, 3);
    }

    @Test
    public void testCCUserTaskDelayVerbose()  {
        runTest(MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, 3);
    }

    @Test
    public void testMockCCServerPendingCallsOverride()  {
        CruiseControlApi client = cruiseControlClientProvider();
        String userTaskID = MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID;

        int pendingCalls1 = 2;
        int pendingCalls2 = 4;

        cruiseControlServer.setupCCUserTasksResponseNoGoals(0, pendingCalls1);

        CompletableFuture<CruiseControlUserTasksResponse> statusFuture = client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);

        for (int i = 1; i <= pendingCalls1; i++) {
            statusFuture = statusFuture.thenCompose(response -> {
                assertThat(
                    response.getJson().get("Status").asText(),
                    is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()));
                return client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);
            });
        }

        statusFuture = statusFuture.thenCompose(response -> {
            assertThat(
                response.getJson().get("Status").asText(),
                is(CruiseControlUserTaskStatus.COMPLETED.toString()));
            return CompletableFuture.completedFuture(response);
        });

        statusFuture = statusFuture.thenCompose(response -> {
            cruiseControlServer.reset();
            cruiseControlServer.setupCCUserTasksResponseNoGoals(0, pendingCalls2);
            return CompletableFuture.completedFuture(null);
        });

        statusFuture = statusFuture.thenCompose(ignore -> client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID));

        for (int i = 1; i <= pendingCalls2; i++) {
            statusFuture = statusFuture.thenCompose(response -> {
                assertThat(
                    response.getJson().get("Status").asText(),
                    is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()));
                return client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);
            });
        }

        statusFuture.thenCompose(response -> {
            assertThat(
                response.getJson().get("Status").asText(),
                is(CruiseControlUserTaskStatus.COMPLETED.toString()));
            return CompletableFuture.completedFuture(response);
        }).join();
    }

    private void runTest(String userTaskID, int pendingCalls)  {
        cruiseControlServer.setupCCUserTasksResponseNoGoals(0, pendingCalls);

        CruiseControlApi client = cruiseControlClientProvider();

        CompletableFuture<CruiseControlUserTasksResponse> statusFuture = client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);

        for (int i = 1; i <= pendingCalls; i++) {
            statusFuture = statusFuture.thenCompose(response -> {
                assertThat(
                    response.getJson().get("Status").asText(),
                    is(CruiseControlUserTaskStatus.IN_EXECUTION.toString()));
                return client.getUserTaskStatus(Reconciliation.DUMMY_RECONCILIATION, HOST, cruiseControlPort, userTaskID);
            });
        }

        statusFuture.thenCompose(response -> {
            assertThat(
                    response.getJson().get("Status").asText(),
                    is(CruiseControlUserTaskStatus.COMPLETED.toString()));
            return CompletableFuture.completedFuture(response);
        }).join();
    }
}
