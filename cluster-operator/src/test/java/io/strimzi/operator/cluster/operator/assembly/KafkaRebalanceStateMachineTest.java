/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.KafkaRebalanceSpecBuilder;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaRebalanceStatus;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.AbstractRebalanceOptions;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.MockCruiseControl;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl.HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaRebalanceStateMachineTest {

    private static final String HOST = "localhost";
    private static final String RESOURCE_NAME = "my-rebalance";
    private static final String CLUSTER_NAMESPACE = "cruise-control-namespace";
    private static final String CLUSTER_NAME = "kafka-cruise-control-test-cluster";
    private static final String CRUISE_CONTROL_RETRIABLE_CONNECTION_EXCEPTION = "CruiseControlRetriableConnectionException";
    private static final KafkaRebalanceSpec EMPTY_KAFKA_REBALANCE_SPEC = new KafkaRebalanceSpecBuilder().build();
    private static final KafkaRebalanceSpec ADD_BROKER_KAFKA_REBALANCE_SPEC =
            new KafkaRebalanceSpecBuilder()
                    .withMode(KafkaRebalanceMode.ADD_BROKERS)
                    .withBrokers(3)
                    .build();
    private static final KafkaRebalanceSpec REMOVE_BROKER_KAFKA_REBALANCE_SPEC =
            new KafkaRebalanceSpecBuilder()
                    .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                    .withBrokers(3)
                    .build();

    private static ClientAndServer ccServer;

    @BeforeAll
    public static void before() throws IOException, URISyntaxException {
        ccServer = MockCruiseControl.server(CruiseControl.REST_API_PORT);
    }

    @AfterAll
    public static void after() {
        ccServer.stop();
    }

    @BeforeEach
    public void resetServer() {
        ccServer.reset();
    }

    /**
     * Creates an example {@link KafkaRebalanceBuilder} instance using the supplied state parameters.
     *
     * @param currentState The current state of the resource before being passed to computeNextStatus.
     * @param currentStatusSessionID The user task ID attached to the current KafkaRebalance resource. Can be null.
     * @param rebalanceAnnotation An annotation to be applied after the reconcile has started, for example "approve" or "stop".
     * @param rebalanceSpec A custom rebalance specification. If null a blank spec will be used.
     * @param reason Reason for the condition
     * @param isAutoApproval If the KafkaRebalance resource has to be auto-approved when in ProposalReady state.
     * @return A KafkaRebalance instance configured with the supplied parameters.
     */
    private KafkaRebalance createKafkaRebalance(KafkaRebalanceState currentState,
                                                String currentStatusSessionID,
                                                String rebalanceAnnotation,
                                                KafkaRebalanceSpec rebalanceSpec,
                                                String reason,
                                                boolean isAutoApproval) {

        Map<String, String> annotations = new HashMap<>();
        annotations.put(Annotations.ANNO_STRIMZI_IO_REBALANCE, rebalanceAnnotation == null ? "none" : rebalanceAnnotation);
        if (isAutoApproval) {
            annotations.put(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true");
        }

        KafkaRebalanceBuilder kafkaRebalanceBuilder =
                new KafkaRebalanceBuilder()
                        .editOrNewMetadata()
                            .withName(RESOURCE_NAME)
                            .withNamespace(CLUSTER_NAMESPACE)
                            .withLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                            .withAnnotations(annotations)
                        .endMetadata()
                        .withSpec(rebalanceSpec);

        // there is no actual status and related condition when a KafkaRebalance is just created
        if (currentState != KafkaRebalanceState.New) {
            Condition currentRebalanceCondition = new Condition();
            currentRebalanceCondition.setType(currentState.toString());
            currentRebalanceCondition.setStatus("True");
            if (reason != null) {
                currentRebalanceCondition.setReason(reason);
            }
            KafkaRebalanceStatus currentStatus = new KafkaRebalanceStatus();
            currentStatus.setConditions(Collections.singletonList(currentRebalanceCondition));
            currentStatus.setSessionId(currentStatusSessionID);

            kafkaRebalanceBuilder.withStatus(currentStatus);
        }
        return kafkaRebalanceBuilder.build();
    }

    /**
     *  Checks the expected transition between two states of the Kafka Rebalance operator.
     *
     * @param vertx The vertx test instance.
     * @param context The test context instance.
     * @param currentState The current state of the resource before being passed to computeNextStatus.
     * @param nextState The expected state of the resouce after computeNextStatus has been called.
     * @param initialAnnotation The initial annotation attached to the Kafka Rebalance resource. For example none or refresh.
     * @param kcRebalance The Kafka Rebalance instance that will be returned by the resourceSupplier.
     * @return A future for the {@link KafkaRebalanceStatus} returned by the {@link KafkaRebalanceAssemblyOperator#computeNextStatus} method
     */
    private Future<KafkaRebalanceStatus> checkTransition(Vertx vertx, VertxTestContext context,
                                                         KafkaRebalanceState currentState,
                                                         KafkaRebalanceState nextState,
                                                         KafkaRebalanceAnnotation initialAnnotation,
                                                         KafkaRebalance kcRebalance) {

        CruiseControlApi client = new CruiseControlApiImpl(vertx, HTTP_DEFAULT_IDLE_TIMEOUT_SECONDS, MockCruiseControl.CC_SECRET, MockCruiseControl.CC_API_SECRET, true, true);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        KafkaRebalanceAssemblyOperator kcrao = new KafkaRebalanceAssemblyOperator(vertx, supplier, ResourceUtils.dummyClusterOperatorConfig()) {
            @Override
            public String cruiseControlHost(String clusterName, String clusterNamespace) {
                return HOST;
            }
        };

        Reconciliation recon = new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME);

        AbstractRebalanceOptions.AbstractRebalanceOptionsBuilder<?, ?> rbOptions = kcrao.convertRebalanceSpecToRebalanceOptions(kcRebalance.getSpec());

        CrdOperator<KubernetesClient,
                KafkaRebalance,
                KafkaRebalanceList> mockRebalanceOps = supplier.kafkaRebalanceOperator;

        when(mockCmOps.getAsync(CLUSTER_NAMESPACE, RESOURCE_NAME)).thenReturn(Future.succeededFuture(new ConfigMap()));
        when(mockRebalanceOps.get(CLUSTER_NAMESPACE, RESOURCE_NAME)).thenReturn(kcRebalance);
        when(mockRebalanceOps.getAsync(CLUSTER_NAMESPACE, RESOURCE_NAME)).thenReturn(Future.succeededFuture(kcRebalance));

        return kcrao.computeNextStatus(recon, HOST, client, kcRebalance, currentState, initialAnnotation, rbOptions)
                .compose(result -> {
                    context.verify(() -> {
                        assertThat(result.getStatus().getConditions(), StateMatchers.hasStateInConditions(nextState));
                        if (initialAnnotation != KafkaRebalanceAnnotation.none && !currentState.isValidateAnnotation(initialAnnotation)) {
                            assertThat("InvalidAnnotation", is(result.status.getConditions().get(0).getReason()));
                        }
                    });
                    return Future.succeededFuture(result.getStatus());
                });
    }

    private static void defaultStatusHandler(AsyncResult<KafkaRebalanceStatus> result, VertxTestContext context) {
        if (result.succeeded()) {
            context.completeNow();
        } else {
            context.failNow(result.cause());
        }
    }

    private static void checkOptimizationResults(AsyncResult<KafkaRebalanceStatus> result, VertxTestContext context, boolean shouldBeEmpty) {
        if (result.succeeded()) {
            assertEquals(shouldBeEmpty, result.result().getOptimizationResult().isEmpty());
            context.completeNow();
        } else {
            context.failNow(result.cause());
        }
    }

    @Test
    public void testNewToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToProposalReady(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToProposalReady(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToProposalReady(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewToProposalReady(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testNewWithNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewWithNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewWithNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewWithNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewWithNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        // Test the case where the user asks for a rebalance but there is not enough data, the returned status should
        // not contain an optimisation result
        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testNewToProposalPending(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToProposalPending(vertx, context, 1, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToProposalPending(vertx, context, 1, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToProposalPending(vertx, context, 1, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewToProposalPending(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testNewBadGoalsError(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder().addAllToGoals(customGoals).build();
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsError(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);

        rebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                .withBrokers(3)
                .addAllToGoals(customGoals)
                .build();
        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsError(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        rebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                .withBrokers(3)
                .addAllToGoals(customGoals)
                .build();
        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsError(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewBadGoalsError(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        // Test the case where the user asks for a rebalance with custom goals which do not contain all the configured hard goals
        // In this case the computeNextStatus error will return a failed future with a message containing an illegal argument exception
        MockCruiseControl.setupCCRebalanceBadGoalsError(ccServer, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.NotReady,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(context.failing(throwable -> {
                    if (throwable.getMessage().contains("java.lang.IllegalArgumentException: Missing hard goals")) {
                        context.completeNow();
                    } else {
                        context.failNow(new RuntimeException("This operation failed with an unexpected error: " + throwable.getMessage(), throwable));
                    }
                }));
    }

    @Test
    public void testNewBadGoalsErrorWithSkipHGCheck(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder().addAllToGoals(customGoals).withSkipHardGoalCheck(true).build();
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsErrorWithSkipHGCheck(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);

        rebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                .withBrokers(3)
                .addAllToGoals(customGoals)
                .withSkipHardGoalCheck(true)
                .build();
        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsErrorWithSkipHGCheck(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        rebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                .withBrokers(3)
                .addAllToGoals(customGoals)
                .withSkipHardGoalCheck(true)
                .build();
        kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsErrorWithSkipHGCheck(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewBadGoalsErrorWithSkipHGCheck(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        // Test the case where the user asks for a rebalance with custom goals which do not contain all the configured hard goals
        // But we have set skip hard goals check to true
        MockCruiseControl.setupCCRebalanceBadGoalsError(ccServer, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testProposalPendingToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalPendingToProposalReady(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalPendingToProposalReady(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalPendingToProposalReady(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalPendingToProposalReady(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.PendingProposal, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testProposalPendingToProposalReadyWithDelay(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalPendingToProposalReadyWithDelay(vertx, context, 3, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalPendingToProposalReadyWithDelay(vertx, context, 3, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalPendingToProposalReadyWithDelay(vertx, context, 3, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalPendingToProposalReadyWithDelay(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.PendingProposal, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testProposalPendingToStopped(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, "stop", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalPendingToStopped(vertx, context, 3, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, "stop", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalPendingToStopped(vertx, context, 3, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, "stop", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalPendingToStopped(vertx, context, 3, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalPendingToStopped(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.PendingProposal, KafkaRebalanceState.Stopped,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testProposalReadyNoChange(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyNoChange(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyNoChange(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyNoChange(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyNoChange(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> defaultStatusHandler(result, context));
    }

    @Test
    public void testProposalReadyToRebalancingWithNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancingWithNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancingWithNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancingWithNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyToRebalancingWithNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.approve, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testProposalReadyToRebalancingWithPendingSummary(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancingWithPendingSummary(vertx, context, 1, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancingWithPendingSummary(vertx, context, 1, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancingWithPendingSummary(vertx, context, 1, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyToRebalancingWithPendingSummary(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.Rebalancing,
                KafkaRebalanceAnnotation.approve, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testProposalReadyToRebalancing(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancing(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancing(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancing(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyToRebalancing(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.Rebalancing,
                KafkaRebalanceAnnotation.approve, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testAutoApprovalProposalReadyToRebalancing(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, true);
        this.krAutoApprovalProposalReadyToRebalancing(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, true);
        this.krAutoApprovalProposalReadyToRebalancing(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, true);
        this.krAutoApprovalProposalReadyToRebalancing(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krAutoApprovalProposalReadyToRebalancing(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.Rebalancing,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testProposalReadyRefreshNoChange(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshNoChange(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshNoChange(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshNoChange(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyRefreshNoChange(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testProposalReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testProposalReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testRebalancingCompleted(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingCompleted(vertx, context, 0, 0, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingCompleted(vertx, context, 0, 0, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingCompleted(vertx, context, 0, 0, kcRebalance);
    }

    private void krRebalancingCompleted(Vertx vertx, VertxTestContext context, int activeCalls, int inExecutionCalls, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, activeCalls, inExecutionCalls);

        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Ready,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testRebalancingPendingThenExecution(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingPendingThenExecution(vertx, context, 1, 1, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingPendingThenExecution(vertx, context, 1, 1, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingPendingThenExecution(vertx, context, 1, 1, kcRebalance);
    }

    private void krRebalancingPendingThenExecution(Vertx vertx, VertxTestContext context, int activeCalls, int inExecutionCalls, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        // This tests that the optimization proposal is added correctly if it was not ready when the rebalance(dryrun=false) was called.
        // The first poll should see active and then the second should see in execution and add the optimization and cancel the timer
        // so that the status is updated.
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, activeCalls, inExecutionCalls);

        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Rebalancing,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testRebalancingToStopped(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, "stop", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingToStopped(vertx, context, 0, 0, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, "stop", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingToStopped(vertx, context, 0, 0, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, "stop", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingToStopped(vertx, context, 0, 0, kcRebalance);
    }

    private void krRebalancingToStopped(Vertx vertx, VertxTestContext context, int activeCalls, int inExecutionCalls, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, activeCalls, inExecutionCalls);
        MockCruiseControl.setupCCStopResponse(ccServer);

        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Stopped,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testRebalancingCompletedWithError(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingToStopped(vertx, context, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingToStopped(vertx, context, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingToStopped(vertx, context, kcRebalance);
    }

    private void krRebalancingToStopped(Vertx vertx, VertxTestContext context, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCUserTasksCompletedWithError(ccServer);

        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.NotReady,
                KafkaRebalanceAnnotation.none, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testStoppedRefreshToPendingProposal(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krStoppedRefreshToPendingProposal(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Stopped, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testStoppedRefreshToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToProposalReady(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToProposalReady(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToProposalReady(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krStoppedRefreshToProposalReady(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Stopped, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testStoppedRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krStoppedRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Stopped, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Ready, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testReadyRefreshToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToProposalReady(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToProposalReady(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToProposalReady(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krReadyRefreshToProposalReady(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Ready, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Ready, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testNotReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposal(vertx, context, 1, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNotReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.NotReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testNotReadyRefreshToProposalReady(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToProposalReady(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToProposalReady(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToProposalReady(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNotReadyRefreshToProposalReady(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.NotReady, KafkaRebalanceState.ProposalReady,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    @Test
    public void testNotReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNotReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceNotEnoughDataError(ccServer, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.NotReady, KafkaRebalanceState.PendingProposal,
                KafkaRebalanceAnnotation.refresh, kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    @Test
    public void testProposalReadyWithWrongAnnotation(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance, KafkaRebalanceState.ProposalReady,  KafkaRebalanceAnnotation.unknown);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance, KafkaRebalanceState.ProposalReady, KafkaRebalanceAnnotation.unknown);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance, KafkaRebalanceState.ProposalReady, KafkaRebalanceAnnotation.unknown);
    }

    @Test
    public void testReadyWithWrongAnnotation(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance, KafkaRebalanceState.Ready, KafkaRebalanceAnnotation.approve);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance, KafkaRebalanceState.Ready, KafkaRebalanceAnnotation.approve);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance, KafkaRebalanceState.Ready, KafkaRebalanceAnnotation.approve);
    }

    private void testStateWithWrongAnnotation(Vertx vertx, VertxTestContext context,
                                              int pendingCalls, CruiseControlEndpoints endpoint,
                                              KafkaRebalance kcRebalance, KafkaRebalanceState kafkaRebalanceState,
                                              KafkaRebalanceAnnotation kafkaRebalanceAnnotation) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                kafkaRebalanceState, kafkaRebalanceState,
                kafkaRebalanceAnnotation, kcRebalance)
                .onComplete(result -> defaultStatusHandler(result, context));
    }

    @Test
    public void testReadyAfterCruiseControlRetriableConnectionException(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, CRUISE_CONTROL_RETRIABLE_CONNECTION_EXCEPTION, false);
        this.testStateWithException(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance, KafkaRebalanceState.NotReady, KafkaRebalanceAnnotation.none);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, CRUISE_CONTROL_RETRIABLE_CONNECTION_EXCEPTION, false);
        this.testStateWithException(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance, KafkaRebalanceState.NotReady, KafkaRebalanceAnnotation.none);

        kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, CRUISE_CONTROL_RETRIABLE_CONNECTION_EXCEPTION, false);
        this.testStateWithException(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance, KafkaRebalanceState.NotReady, KafkaRebalanceAnnotation.none);
    }


    private void testStateWithException(Vertx vertx, VertxTestContext context,
                                              int pendingCalls, CruiseControlEndpoints endpoint,
                                              KafkaRebalance kcRebalance, KafkaRebalanceState kafkaRebalanceState,
                                              KafkaRebalanceAnnotation kafkaRebalanceAnnotation) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        checkTransition(vertx, context,
                kafkaRebalanceState, KafkaRebalanceState.ProposalReady,
                kafkaRebalanceAnnotation, kcRebalance)
                .onComplete(result -> defaultStatusHandler(result, context));
    }

    private static class StateMatchers extends AbstractResourceStateMatchers {

    }
}