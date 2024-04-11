/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpecBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.AbstractRebalanceOptions;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.MockCruiseControl;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
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

    private static MockCruiseControl ccServer;

    @BeforeAll
    public static void before() throws IOException, URISyntaxException {
        ccServer = new MockCruiseControl(CruiseControl.REST_API_PORT);
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
        annotations.put(Annotations.ANNO_STRIMZI_IO_REBALANCE, rebalanceAnnotation);
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
     * @param nextState The expected state of the resource after computeNextStatus has been called.
     * @param kcRebalance The Kafka Rebalance instance that will be returned by the resourceSupplier.
     * @return A future for the {@link KafkaRebalanceStatus} returned by the {@link KafkaRebalanceAssemblyOperator#computeNextStatus} method
     */
    private Future<KafkaRebalanceStatus> checkTransition(Vertx vertx, VertxTestContext context,
                                                         KafkaRebalanceState currentState,
                                                         KafkaRebalanceState nextState,
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

        KafkaRebalanceAnnotation initialAnnotation = kcrao.rebalanceAnnotation(kcRebalance);
        return kcrao.computeNextStatus(recon, HOST, client, kcRebalance, currentState, rbOptions)
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

    /**
     * Tests the transition from 'New' to 'ProposalReady'
     *
     * 1. A new KafkaRebalance resource is created; it is in the New state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource moves to the 'ProposalReady' state
     */
    @Test
    public void testNewToProposalReadyRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToProposalReady(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToProposalReady(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToProposalReady(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewToProposalReady(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(0, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.ProposalReady,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'New' to 'PendingProposal' because of not enough data to compute an optimization proposal
     *
     * 1. A new KafkaRebalance resource is created; it is in the New state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not enough data
     * 4. The KafkaRebalance resource moves to the 'PendingProposal' state
     */
    @Test
    public void testNewWithNotEnoughDataRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewWithNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewWithNotEnoughDataRebalance} for description
     */
    @Test
    public void testNewWithNotEnoughDataAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewWithNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewWithNotEnoughDataRebalance} for description
     */
    @Test
    public void testNewWithNotEnoughDataRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewWithNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewWithNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        // Test the case where the user asks for a rebalance but there is not enough data, the returned status should
        // not contain an optimisation result
        ccServer.setupCCRebalanceNotEnoughDataError(endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'New' to 'PendingProposal' because of the optimization proposal not available yet on Cruise Control
     *
     * 1. A new KafkaRebalance resource is created; it is in the New state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not ready yet
     * 4. The KafkaRebalance resource moves to the 'PendingProposal' state
     */
    @Test
    public void testNewToPendingProposalRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToPendingProposal(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewToPendingProposalRebalance} for description
     */
    @Test
    public void testNewToPendingProposalAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToPendingProposal(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewToPendingProposalRebalance} for description
     */
    @Test
    public void testNewToPendingProposalRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNewToPendingProposal(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewToPendingProposal(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(1, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'New' to 'NotReady' due to "missing hard goals" error
     *
     * 1. A new KafkaRebalance resource is created with some specified not hard goals; it is in the New state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is not ready because of a "missing hard goals" error
     * 4. The KafkaRebalance resource moves to the 'NotReady' state
     */
    @Test
    public void testNewBadGoalsErrorRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder().addAllToGoals(customGoals).build();
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsError(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewBadGoalsErrorRebalance} for description
     */
    @Test
    public void testNewBadGoalsErrorAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                .withBrokers(3)
                .addAllToGoals(customGoals)
                .build();
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsError(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewBadGoalsErrorRebalance} for description
     */
    @Test
    public void testNewBadGoalsErrorRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                .withBrokers(3)
                .addAllToGoals(customGoals)
                .build();
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsError(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewBadGoalsError(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        // Test the case where the user asks for a rebalance with custom goals which do not contain all the configured hard goals
        // In this case the computeNextStatus error will return a failed future with a message containing an illegal argument exception
        ccServer.setupCCRebalanceBadGoalsError(endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.NotReady,
                kcRebalance)
                .onComplete(context.failing(throwable -> {
                    if (throwable.getMessage().contains("java.lang.IllegalArgumentException: Missing hard goals")) {
                        context.completeNow();
                    } else {
                        context.failNow(new RuntimeException("This operation failed with an unexpected error: " + throwable.getMessage(), throwable));
                    }
                }));
    }

    /**
     * Tests the transition from 'New' to 'ProposalReady' skipping the hard goals check
     *
     * 1. A new KafkaRebalance resource is created with some specified not hard goals but with flag to skip the check; it is in the New state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource moves to the 'ProposalReady' state
     */
    @Test
    public void testNewBadGoalsErrorWithSkipHardGoalsRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder().addAllToGoals(customGoals).withSkipHardGoalCheck(true).build();
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsErrorWithSkipHardGoals(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewBadGoalsErrorWithSkipHardGoalsRebalance} for description
     */
    @Test
    public void testNewBadGoalsErrorWithSkipHardGoalsAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                .withBrokers(3)
                .addAllToGoals(customGoals)
                .withSkipHardGoalCheck(true)
                .build();
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsErrorWithSkipHardGoals(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNewBadGoalsErrorWithSkipHardGoalsRebalance} for description
     */
    @Test
    public void testNewBadGoalsErrorWithSkipHardGoalsRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        List<String> customGoals = new ArrayList<>();
        customGoals.add("Goal.one");
        customGoals.add("Goal.two");
        customGoals.add("Goal.three");

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                .withBrokers(3)
                .addAllToGoals(customGoals)
                .withSkipHardGoalCheck(true)
                .build();
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.New, null, null, rebalanceSpec, null, false);
        this.krNewBadGoalsErrorWithSkipHardGoals(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNewBadGoalsErrorWithSkipHardGoals(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        // Test the case where the user asks for a rebalance with custom goals which do not contain all the configured hard goals
        // But we have set skip hard goals check to true
        ccServer.setupCCRebalanceBadGoalsError(endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.New, KafkaRebalanceState.ProposalReady,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'PendingProposal' to 'ProposalReady'
     *
     * 1. A new KafkaRebalance resource is created; it is in the PendingProposal state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource moves to the 'ProposalReady' state
     */
    @Test
    public void testPendingProposalToProposalReadyRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krPendingProposalToProposalReady(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testPendingProposalToProposalReadyRebalance} for description
     */
    @Test
    public void testPendingProposalToProposalReadyAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krPendingProposalToProposalReady(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testPendingProposalToProposalReadyRebalance} for description
     */
    @Test
    public void testPendingProposalToProposalReadyRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krPendingProposalToProposalReady(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krPendingProposalToProposalReady(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(0, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.PendingProposal, KafkaRebalanceState.ProposalReady,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'PendingProposal' to 'ProposalReady' but after several pending calls
     *
     * 1. A new KafkaRebalance resource is created; it is in the PendingProposal state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is still pending for several calls (Cruise Control is computing it) and finally it's ready
     * 4. The KafkaRebalance resource moves to the 'ProposalReady' state
     */
    @Test
    public void testPendingProposalToProposalReadyWithDelayRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krPendingProposalToProposalReadyWithDelay(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testPendingProposalToProposalReadyWithDelayRebalance} for description
     */
    @Test
    public void testPendingProposalToProposalReadyWithDelayAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krPendingProposalToProposalReadyWithDelay(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testPendingProposalToProposalReadyWithDelayRebalance} for description
     */
    @Test
    public void testPendingProposalToProposalReadyWithDelayRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krPendingProposalToProposalReadyWithDelay(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krPendingProposalToProposalReadyWithDelay(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(3, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.PendingProposal, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .compose(v -> checkTransition(vertx, context,
                        KafkaRebalanceState.PendingProposal, KafkaRebalanceState.PendingProposal,
                        kcRebalance))
                .compose(v -> checkTransition(vertx, context,
                        KafkaRebalanceState.PendingProposal, KafkaRebalanceState.PendingProposal,
                        kcRebalance))
                .compose(v -> checkTransition(vertx, context,
                        KafkaRebalanceState.PendingProposal, KafkaRebalanceState.ProposalReady,
                        kcRebalance))
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'PendingProposal' to 'Stopped'
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=stop; it is in the PendingProposal state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The KafkaRebalance resource moves to the 'Stopped' state
     */
    @Test
    public void testPendingProposalToStoppedRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, "stop", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krPendingProposalToStopped(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testPendingProposalToStoppedRebalance} for description
     */
    @Test
    public void testPendingProposalToStoppedAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, "stop", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krPendingProposalToStopped(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testPendingProposalToStoppedRebalance} for description
     */
    @Test
    public void testPendingProposalToStoppedRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.PendingProposal, null, "stop", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krPendingProposalToStopped(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krPendingProposalToStopped(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        // the number of pending calls doesn't have a real impact
        // the stop annotation is evaluated on the next step computation so the rebalance request will be stopped immediately
        ccServer.setupCCRebalanceResponse(3, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.PendingProposal, KafkaRebalanceState.Stopped,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the stay on `ProposalReady` when there are no changes
     *
     * 1. A new KafkaRebalance resource is created; it is in the ProposalReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The KafkaRebalance resource stays in the 'ProposalReady' state
     */
    @Test
    public void testProposalReadyNoChangeRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyNoChange(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyNoChangeRebalance} for description
     */
    @Test
    public void testProposalReadyNoChangeAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyNoChange(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyNoChangeRebalance} for description
     */
    @Test
    public void testProposalReadyNoChangeRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyNoChange(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyNoChange(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(0, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.ProposalReady,
                kcRebalance)
                .onComplete(result -> defaultStatusHandler(result, context));
    }

    /**
     * Tests the transition from 'ProposalReady' to 'PendingProposal' because of not enough data
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=approve; it is in the ProposalReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not enough data
     * 4. The KafkaRebalance resource moves to the 'PendingProposal' state
     */
    @Test
    public void testProposalReadyToPendingProposalWithNotEnoughDataRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "approve", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToPendingProposalWithNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyToPendingProposalWithNotEnoughDataRebalance} for description
     */
    @Test
    public void testProposalReadyToPendingProposalWithNotEnoughDataAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "approve", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToPendingProposalWithNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyToPendingProposalWithNotEnoughDataRebalance} for description
     */
    @Test
    public void testProposalReadyToPendingProposalWithNotEnoughDataRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "approve", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToPendingProposalWithNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyToPendingProposalWithNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceNotEnoughDataError(endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'ProposalReady' to 'Rebalancing' because of the pending call
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=approve; it is in the ProposalReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not ready yet
     * 4. The KafkaRebalance resource moves to the 'Rebalancing' state
     */
    @Test
    public void testProposalReadyToRebalancingWithPendingSummaryRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "approve", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancingWithPendingSummary(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyToRebalancingWithPendingSummaryRebalance} for description
     */
    @Test
    public void testProposalReadyToRebalancingWithPendingSummaryAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "approve", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancingWithPendingSummary(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyToRebalancingWithPendingSummaryRebalance} for description
     */
    @Test
    public void testProposalReadyToRebalancingWithPendingSummaryRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "approve", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancingWithPendingSummary(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyToRebalancingWithPendingSummary(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(1, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.Rebalancing,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'ProposalReady' to 'Rebalancing' with manual approval
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=approve; it is in the ProposalReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource moves to the 'Rebalancing' state
     */
    @Test
    public void testProposalReadyToRebalancingRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "approve", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancing(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyToRebalancingRebalance} for description
     */
    @Test
    public void testProposalReadyToRebalancingAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "approve", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancing(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyToRebalancingRebalance} for description
     */
    @Test
    public void testProposalReadyToRebalancingRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "approve", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyToRebalancing(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyToRebalancing(Vertx vertx, VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(pendingCalls, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.Rebalancing,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'ProposalReady' to 'Rebalancing' with auto-approval
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance-auto-approval=true; it is in the ProposalReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource moves to the 'Rebalancing' state
     */
    @Test
    public void testAutoApprovalProposalReadyToRebalancingRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, null, true);
        this.krAutoApprovalProposalReadyToRebalancing(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testAutoApprovalProposalReadyToRebalancingRebalance} for description
     */
    @Test
    public void testAutoApprovalProposalReadyToRebalancingAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, true);
        this.krAutoApprovalProposalReadyToRebalancing(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testAutoApprovalProposalReadyToRebalancingRebalance} for description
     */
    @Test
    public void testAutoApprovalProposalReadyToRebalancingRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, true);
        this.krAutoApprovalProposalReadyToRebalancing(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krAutoApprovalProposalReadyToRebalancing(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(0, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.Rebalancing,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the stay on `ProposalReady` when there are no changes on refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the ProposalReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The KafkaRebalance resource stays in the 'ProposalReady' state
     */
    @Test
    public void testProposalReadyRefreshNoChangeRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshNoChange(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyRefreshNoChangeRebalance} for description
     */
    @Test
    public void testProposalReadyRefreshNoChangeAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshNoChange(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyRefreshNoChangeRebalance} for description
     */
    @Test
    public void testProposalReadyRefreshNoChangeRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshNoChange(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyRefreshNoChange(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(0, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.ProposalReady,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'ProposalReady' to 'PendingProposal' because of the pending call when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the ProposalReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not ready yet
     * 4. The KafkaRebalance resource moves to the 'PendingProposal' state
     */
    @Test
    public void testProposalReadyRefreshToPendingProposalRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyRefreshToPendingProposalRebalance} for description
     */
    @Test
    public void testProposalReadyRefreshToPendingProposalAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyRefreshToPendingProposalRebalance} for description
     */
    @Test
    public void testProposalReadyRefreshToPendingProposalRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(1, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'ProposalReady' to 'PendingProposal' because of not enough data when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the ProposalReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not enough data
     * 4. The KafkaRebalance resource moves to the 'PendingProposal' state
     */
    @Test
    public void testProposalReadyRefreshToPendingProposalNotEnoughDataRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyRefreshToPendingProposalNotEnoughDataRebalance} for description
     */
    @Test
    public void testProposalReadyRefreshToPendingProposalNotEnoughDataAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyRefreshToPendingProposalNotEnoughDataRebalance} for description
     */
    @Test
    public void testProposalReadyRefreshToPendingProposalNotEnoughDataRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krProposalReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krProposalReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceNotEnoughDataError(endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.ProposalReady, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'Rebalancing' to 'Ready'
     *
     * 1. A new KafkaRebalance resource is created; it is in the Rebalancing state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource moves to the 'Ready' state
     */
    @Test
    public void testRebalancingCompletedRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingCompleted(vertx, context, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testRebalancingCompletedRebalance} for description
     */
    @Test
    public void testRebalancingCompletedAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingCompleted(vertx, context, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testRebalancingCompletedRebalance} for description
     */
    @Test
    public void testRebalancingCompletedRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingCompleted(vertx, context, kcRebalance);
    }

    private void krRebalancingCompleted(Vertx vertx, VertxTestContext context, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCUserTasksResponseNoGoals(0, 0);

        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Ready,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the staying on 'Rebalancing' across multiple calls
     *
     * 1. A new KafkaRebalance resource is created; it is in the Rebalancing state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is rebalancing for several calls (Cruise Control is computing it)
     * 4. The KafkaRebalance stays in 'Rebalancing' state
     */
    @Test
    public void testRebalancingPendingThenExecutionRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingPendingThenExecution(vertx, context, 1, 1, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testRebalancingPendingThenExecutionRebalance} for description
     */
    @Test
    public void testRebalancingPendingThenExecutionAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingPendingThenExecution(vertx, context, 1, 1, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testRebalancingPendingThenExecutionRebalance} for description
     */
    @Test
    public void testRebalancingPendingThenExecutionRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_VERBOSE_RESPONSE_UTID, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingPendingThenExecution(vertx, context, 1, 1, kcRebalance);
    }

    private void krRebalancingPendingThenExecution(Vertx vertx, VertxTestContext context, int activeCalls, int inExecutionCalls, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        // This tests that the optimization proposal is added correctly if it was not ready when the rebalance(dryrun=false) was called.
        // The first poll should see active and then the second should see in execution and add the optimization and cancel the timer
        // so that the status is updated.
        ccServer.setupCCUserTasksResponseNoGoals(activeCalls, inExecutionCalls);

        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Rebalancing,
                kcRebalance)
                .compose(v -> checkTransition(vertx, context,
                        KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Rebalancing,
                        kcRebalance))
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'Rebalancing' to 'Stopped'
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=stop; it is in the Rebalancing state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The KafkaRebalance resource moves to the 'Stopped' state
     */
    @Test
    public void testRebalancingToStoppedRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, "stop", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingToStopped(vertx, context, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testRebalancingToStoppedRebalance} for description
     */
    @Test
    public void testRebalancingToStoppedAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, "stop", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingToStopped(vertx, context, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testRebalancingToStoppedRebalance} for description
     */
    @Test
    public void testRebalancingToStoppedRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, "stop", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingToStopped(vertx, context, kcRebalance);
    }

    private void krRebalancingToStopped(Vertx vertx, VertxTestContext context, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCUserTasksResponseNoGoals(0, 0);
        ccServer.setupCCStopResponse();

        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.Stopped,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'Rebalancing' to 'NotReady'
     *
     * 1. A new KafkaRebalance resource is created; it is in the Rebalancing state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal has errors
     * 4. The KafkaRebalance moves to the 'NotReady' state
     */
    @Test
    public void testRebalancingCompletedWithErrorRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, null, EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingCompletedWithError(vertx, context, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testRebalancingCompletedWithErrorRebalance} for description
     */
    @Test
    public void testRebalancingCompletedWithErrorAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingCompletedWithError(vertx, context, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testRebalancingCompletedWithErrorRebalance} for description
     */
    @Test
    public void testRebalancingCompletedWithErrorRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Rebalancing, MockCruiseControl.REBALANCE_NO_GOALS_RESPONSE_UTID, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krRebalancingCompletedWithError(vertx, context, kcRebalance);
    }

    private void krRebalancingCompletedWithError(Vertx vertx, VertxTestContext context, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCUserTasksCompletedWithError();

        checkTransition(vertx, context,
                KafkaRebalanceState.Rebalancing, KafkaRebalanceState.NotReady,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'Stopped' to 'PendingProposal' when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the Stopped state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not ready yet
     * 4. The KafkaRebalance moves to the 'PendingProposal' state
     */
    @Test
    public void testStoppedRefreshToPendingProposalRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testStoppedRefreshToPendingProposalRebalance} for description
     */
    @Test
    public void testStoppedRefreshToPendingProposalAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testStoppedRefreshToPendingProposalRebalance} for description
     */
    @Test
    public void testStoppedRefreshToPendingProposalRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krStoppedRefreshToPendingProposal(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(1, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Stopped, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'Stopped' to 'ProposalReady' when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the Stopped state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance moves to the 'ProposalReady' state
     */
    @Test
    public void testStoppedRefreshToProposalReadyRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToProposalReady(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testStoppedRefreshToProposalReadyRebalance} for description
     */
    @Test
    public void testStoppedRefreshToProposalReadyAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToProposalReady(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testStoppedRefreshToProposalReadyRebalance} for description
     */
    @Test
    public void testStoppedRefreshToProposalReadyRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToProposalReady(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krStoppedRefreshToProposalReady(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(0, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Stopped, KafkaRebalanceState.ProposalReady,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'Stopped' to 'PendingProposal' because of not enough data when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the Stopped state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not enough data
     * 4. The KafkaRebalance moves to the 'PendingProposal' state
     */
    @Test
    public void testStoppedRefreshToPendingProposalNotEnoughDataRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testStoppedRefreshToPendingProposalNotEnoughDataRebalance} for description
     */
    @Test
    public void testStoppedRefreshToPendingProposalNotEnoughDataAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testStoppedRefreshToPendingProposalNotEnoughDataRebalance} for description
     */
    @Test
    public void testStoppedRefreshToPendingProposalNotEnoughDataRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Stopped, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krStoppedRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krStoppedRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceNotEnoughDataError(endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Stopped, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'Ready' to 'PendingProposal' when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the Ready state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not ready yet
     * 4. The KafkaRebalance moves to the 'PendingProposal' state
     */
    @Test
    public void testReadyRefreshToPendingProposalRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyRefreshToPendingProposalRebalance} for description
     */
    @Test
    public void testReadyRefreshToPendingProposalAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyRefreshToPendingProposalRebalance} for description
     */
    @Test
    public void testReadyRefreshToPendingProposalRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(1, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Ready, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'Ready' to 'ProposalReady' when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the Ready state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance moves to the 'ProposalReady' state
     */
    @Test
    public void testReadyRefreshToProposalReadyRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToProposalReady(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyRefreshToProposalReadyRebalance} for description
     */
    @Test
    public void testReadyRefreshToProposalReadyAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToProposalReady(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyRefreshToProposalReadyRebalance} for description
     */
    @Test
    public void testReadyRefreshToProposalReadyRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToProposalReady(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krReadyRefreshToProposalReady(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(0, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Ready, KafkaRebalanceState.ProposalReady,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'Ready' to 'PendingProposal' because of not enough data when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the Ready state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not enough data
     * 4. The KafkaRebalance moves to the 'PendingProposal' state
     */
    @Test
    public void testReadyRefreshToPendingProposalNotEnoughDataRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyRefreshToPendingProposalNotEnoughDataRebalance} for description
     */
    @Test
    public void testReadyRefreshToPendingProposalNotEnoughDataAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyRefreshToPendingProposalNotEnoughDataRebalance} for description
     */
    @Test
    public void testReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceNotEnoughDataError(endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.Ready, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'NotReady' to 'PendingProposal' when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the NotReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not ready yet
     * 4. The KafkaRebalance moves to the 'PendingProposal' state
     */
    @Test
    public void testNotReadyRefreshToPendingProposalRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNotReadyRefreshToPendingProposalRebalance} for description
     */
    @Test
    public void testNotReadyRefreshToPendingProposalAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNotReadyRefreshToPendingProposalRebalance} for description
     */
    @Test
    public void testNotReadyRefreshToPendingProposalRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposal(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNotReadyRefreshToPendingProposal(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(1, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.NotReady, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the transition from 'NotReady' to 'ProposalReady' when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the NotReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance moves to the 'ProposalReady' state
     */
    @Test
    public void testNotReadyRefreshToProposalReadyRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToProposalReady(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNotReadyRefreshToProposalReadyRebalance} for description
     */
    @Test
    public void testNotReadyRefreshToProposalReadyAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToProposalReady(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNotReadyRefreshToProposalReadyRebalance} for description
     */
    @Test
    public void testNotReadyRefreshToProposalReadyRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToProposalReady(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNotReadyRefreshToProposalReady(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(0, endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.NotReady, KafkaRebalanceState.ProposalReady,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, false));
    }

    /**
     * Tests the transition from 'NotReady' to 'PendingProposal' because of not enough data when refresh
     *
     * 1. A new KafkaRebalance resource is created and annotated with strimzi.io/rebalance=refresh; it is in the NotReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is pending because not enough data
     * 4. The KafkaRebalance moves to the 'PendingProposal' state
     */
    @Test
    public void testNotReadyRefreshToPendingProposalNotEnoughDataRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, "refresh", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REBALANCE, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNotReadyRefreshToPendingProposalNotEnoughDataRebalance} for description
     */
    @Test
    public void testNotReadyRefreshToPendingProposalNotEnoughDataAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, "refresh", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.ADD_BROKER, kcRebalance);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testNotReadyRefreshToPendingProposalNotEnoughDataRebalance} for description
     */
    @Test
    public void testNotReadyRefreshToPendingProposalNotEnoughDataRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, "refresh", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.krNotReadyRefreshToPendingProposalNotEnoughData(vertx, context, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance);
    }

    private void krNotReadyRefreshToPendingProposalNotEnoughData(Vertx vertx, VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kcRebalance) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceNotEnoughDataError(endpoint);

        checkTransition(vertx, context,
                KafkaRebalanceState.NotReady, KafkaRebalanceState.PendingProposal,
                kcRebalance)
                .onComplete(result -> checkOptimizationResults(result, context, true));
    }

    /**
     * Tests the stay on `ProposalReady` when there is a wrong annotation
     *
     * 1. A new KafkaRebalance resource is created and annotated with an "unknown" annotation; it is in the ProposalReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The KafkaRebalance resource stays in the 'ProposalReady' state
     */
    @Test
    public void testProposalReadyWithWrongAnnotationRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "wrong", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance, KafkaRebalanceState.ProposalReady);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyWithWrongAnnotationRebalance} for description
     */
    @Test
    public void testProposalReadyWithWrongAnnotationAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "wrong", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance, KafkaRebalanceState.ProposalReady);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testProposalReadyWithWrongAnnotationRebalance} for description
     */
    @Test
    public void testProposalReadyWithWrongAnnotation(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.ProposalReady, null, "wrong", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance, KafkaRebalanceState.ProposalReady);
    }

    /**
     * Tests the stay on `Ready` when there is a wrong annotation
     *
     * 1. A new KafkaRebalance resource is created and annotated with an "unknown" annotation; it is in the Ready state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The KafkaRebalance resource stays in the 'Ready' state
     */
    @Test
    public void testReadyWithWrongAnnotationRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "approve", EMPTY_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance, KafkaRebalanceState.Ready);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyWithWrongAnnotationRebalance} for description
     */
    @Test
    public void testReadyWithWrongAnnotationAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "approve", ADD_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance, KafkaRebalanceState.Ready);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyWithWrongAnnotationRebalance} for description
     */
    @Test
    public void testReadyWithWrongAnnotationRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, "approve", REMOVE_BROKER_KAFKA_REBALANCE_SPEC, null, false);
        this.testStateWithWrongAnnotation(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance, KafkaRebalanceState.Ready);
    }

    private void testStateWithWrongAnnotation(Vertx vertx, VertxTestContext context,
                                              int pendingCalls, CruiseControlEndpoints endpoint,
                                              KafkaRebalance kcRebalance, KafkaRebalanceState kafkaRebalanceState) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(pendingCalls, endpoint);

        checkTransition(vertx, context,
                kafkaRebalanceState, kafkaRebalanceState,
                kcRebalance)
                .onComplete(result -> defaultStatusHandler(result, context));
    }

    /**
     * Tests the transition from 'NotReady' to 'ProposalReady' after recovering a retriable exception on Cruise Control
     *
     * 1. A new KafkaRebalance resource is created; it is in the NotReady state
     * 2. The operator computes the next state on the rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready after a retriable exception on Cruise Control
     * 4. The KafkaRebalance moves to the 'ProposalReady' state
     */
    @Test
    public void testReadyAfterCruiseControlRetriableConnectionExceptionRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.NotReady, null, null, EMPTY_KAFKA_REBALANCE_SPEC, CRUISE_CONTROL_RETRIABLE_CONNECTION_EXCEPTION, false);
        this.testStateWithException(vertx, context, 0, CruiseControlEndpoints.REBALANCE, kcRebalance, KafkaRebalanceState.NotReady);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyAfterCruiseControlRetriableConnectionExceptionRebalance} for description
     */
    @Test
    public void testReadyAfterCruiseControlRetriableConnectionExceptionAddBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, ADD_BROKER_KAFKA_REBALANCE_SPEC, CRUISE_CONTROL_RETRIABLE_CONNECTION_EXCEPTION, false);
        this.testStateWithException(vertx, context, 0, CruiseControlEndpoints.ADD_BROKER, kcRebalance, KafkaRebalanceState.NotReady);
    }

    /**
     * See the {@link KafkaRebalanceStateMachineTest#testReadyAfterCruiseControlRetriableConnectionExceptionRebalance} for description
     */
    @Test
    public void testReadyAfterCruiseControlRetriableConnectionExceptionRemoveBroker(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kcRebalance = createKafkaRebalance(KafkaRebalanceState.Ready, null, null, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, CRUISE_CONTROL_RETRIABLE_CONNECTION_EXCEPTION, false);
        this.testStateWithException(vertx, context, 0, CruiseControlEndpoints.REMOVE_BROKER, kcRebalance, KafkaRebalanceState.NotReady);
    }


    private void testStateWithException(Vertx vertx, VertxTestContext context,
                                              int pendingCalls, CruiseControlEndpoints endpoint,
                                              KafkaRebalance kcRebalance, KafkaRebalanceState kafkaRebalanceState) throws IOException, URISyntaxException {
        ccServer.setupCCRebalanceResponse(pendingCalls, endpoint);

        checkTransition(vertx, context,
                kafkaRebalanceState, KafkaRebalanceState.ProposalReady,
                kcRebalance)
                .onComplete(result -> defaultStatusHandler(result, context));
    }

    private static class StateMatchers extends AbstractResourceStateMatchers {

    }
}