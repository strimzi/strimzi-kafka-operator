/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaRebalance;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.KafkaRebalanceSpecBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.operator.assembly.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.operator.assembly.KafkaRebalanceState;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlRestException;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.MockCruiseControl;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaRebalanceAssemblyOperatorTest {

    private static final String HOST = "localhost";
    private static final String RESOURCE_NAME = "my-rebalance";
    private static final String CLUSTER_NAMESPACE = "cruise-control-namespace";
    private static final String CLUSTER_NAME = "kafka-cruise-control-test-cluster";

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_11;

    private static final Logger log = LogManager.getLogger(KafkaRebalanceAssemblyOperatorTest.class.getName());

    private static ClientAndServer ccServer;
    private KubernetesClient kubernetesClient;

    private CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList, DoneableKafkaRebalance> mockRebalanceOps;
    private CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> mockKafkaOps;
    private KafkaRebalanceAssemblyOperator kcrao;

    private final int replicas = 1;
    private final String image = "my-kafka-image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;

    private final String version = KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION;
    private final String ccImage = "my-cruise-control-image";

    private final CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
            .withImage(ccImage)
            .build();

    private final Kafka kafka =
            new KafkaBuilder(ResourceUtils.createKafka(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                        .editKafka()
                            .withVersion(version)
                        .endKafka()
                        .withCruiseControl(cruiseControlSpec)
                    .endSpec()
                    .build();

    @BeforeAll
    public static void beforeAll() throws IOException, URISyntaxException {
        ccServer = MockCruiseControl.getCCServer(CruiseControl.REST_API_PORT);
    }

    @AfterAll
    public static void afterAll() {
        ccServer.stop();
    }

    @BeforeEach
    public void beforeEach(Vertx vertx) {
        ccServer.reset();

        kubernetesClient = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaRebalance(), KafkaRebalance.class, KafkaRebalanceList.class, DoneableKafkaRebalance.class)
                .end()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        kcrao = new KafkaRebalanceAssemblyOperator(vertx, pfa, supplier) {
            @Override
            public String cruiseControlHost(String clusterName, String clusterNamespace) {
                return HOST;
            }

            @Override
            public CruiseControlApi cruiseControlClientProvider() {
                return new CruiseControlApiImpl(vertx, 1);
            }
        };

        mockRebalanceOps = supplier.kafkaRebalanceOperator;
        mockKafkaOps = supplier.kafkaOperator;
    }

    @AfterEach
    public void afterEach() {
        if (kubernetesClient != null) {
            kubernetesClient.close();
        }
    }

    /**
     * Tests the transition from New to ProposalReady
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The rebalance resource moves to ProposalReady state
     */
    @Test
    public void testNewToProposalReadyRebalance(VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {
                    // the resource moved from New directly to ProposalReady (no pending calls in the Mock server)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from New to PendingProposal and then finally to ProposalReady
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The rebalance proposal is not ready on the first call; the operator starts polling the Cruise Control REST API
     * 4. The rebalance resource moves to PendingProposal state
     * 5. The rebalance proposal is ready after the specified pending calls
     * 6. The rebalance resource moves to ProposalReady state
     */
    @Test
    public void testNewToPendingProposalToProposalReadyRebalance(VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 2);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.PendingProposal);
                })).compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr1);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from New to ProposalReady
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from New to PendingProposal and then Stopped (via annotation)
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The rebalance proposal is not ready yet; the operator starts polling the Cruise Control REST API
     * 4. The rebalance resource moves to PendingProposal state
     * 6. While the operator is waiting for the proposal, the rebalance resource is annotated with strimzi.io/rebalance=stop
     * 7. The operator stops polling the Cruise Control REST API
     * 8. The rebalance resource moves to Stopped state
     */
    @Test
    public void testNewToPendingProposalToStoppedRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 5);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient, new Runnable() {
                int count = 0;

                @Override
                public void run() {
                    if (++count == 4) {
                        // after a while, apply the "stop" annotation to the resource in the PendingProposal state
                        annotate(kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceAnnotation.stop);
                    }
                    return;
                }
            });

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.PendingProposal);
                })).compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr1);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalPending to Stopped
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.Stopped);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from New to PendingProposal then to Stopped (via annotation)
     * The resource is refreshed and it moves to PendingProposal again and finally to ProposalReady
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The rebalance proposal is not ready yet; the operator starts polling the Cruise Control REST API
     * 4. The rebalance resource moves to PendingProposal state
     * 6. While the operator is waiting for the proposal, the rebalance resource is annotated with strimzi.io/rebalance=stop
     * 7. The operator stops polling the Cruise Control REST API
     * 8. The rebalance resource moves to Stopped state
     * 9. The rebalance resource is annotated with strimzi.io/rebalance=refresh
     * 10. The operator requests a rebalance proposal to Cruise Control REST API
     * 11. The rebalance proposal is not ready yet; the operator starts polling the Cruise Control REST API
     * 12. The rebalance resource moves to PendingProposal state
     * 13. The rebalance proposal is ready after the specified pending calls
     * 14. The rebalance resource moves to ProposalReady state
     */
    @Test
    public void testNewToPendingProposalToStoppedAndRefreshRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 2);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient, new Runnable() {
                int count = 0;

                @Override
                public void run() {
                    if (++count == 4) {
                        // after a while, apply the "stop" annotation to the resource in the PendingProposal state
                        annotate(kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceAnnotation.stop);
                    }
                    return;
                }
            });

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.PendingProposal);
                })).compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr1);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalPending to Stopped
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.Stopped);
                })).compose(v -> {
                    // apply the "refresh" annotation to the resource in the Stopped state
                    KafkaRebalance refreshedKr = annotate(kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceAnnotation.refresh);

                    // trigger another reconcile to process the Stopped state
                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            refreshedKr);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from Stopped to PendingProposal
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.PendingProposal);
                })).compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr6 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr6);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from PendingProposal to ProposalReady
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from New to to ProposalReady
     * The rebalance proposal is approved and the resource moves to Rebalancing and finally to Ready
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The rebalance resource moves to ProposalReady state
     * 9. The rebalance resource is annotated with strimzi.io/rebalance=approve
     * 10. The operator requests the rebalance operation to Cruise Control REST API
     * 11. The rebalance operation is not done immediately; the operator starts polling the Cruise Control REST API
     * 12. The rebalance resource moves to Rebalancing state
     * 13. The rebalance operation is done
     * 14. The rebalance resource moves to Ready state
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyRebalance(VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {
                    // the resource moved from New to ProposalReady directly (no pending calls in the Mock server)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                })).compose(v -> {
                    // apply the "approve" annotation to the resource in the ProposalReady state
                    KafkaRebalance approvedKr = annotate(kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceAnnotation.approve);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            approvedKr);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to Rebalancing on approval
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.Rebalancing);
                })).compose(v -> {
                    // trigger another reconcile to process the Rebalancing state
                    KafkaRebalance kr4 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr4);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from Rebalancing to Ready
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.Ready);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from New to NotReady due to "missing hard goals" error
     *
     * 1. A new rebalance resource is created with some specified not hard goals; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The operator gets a "missing hard goals" error instead of a proposal
     * 4. The rebalance resource moves to NotReady state
     */
    @Test
    public void testNewWithMissingHardGoals(VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint to get error about hard goals
        MockCruiseControl.setupCCRebalanceBadGoalsError(ccServer);

        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec);

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceState.NotReady, CruiseControlRestException.class,
                            "Error processing POST request '/rebalance' due to: " +
                                    "'java.lang.IllegalArgumentException: Missing hard goals [NetworkInboundCapacityGoal, DiskCapacityGoal, RackAwareGoal, NetworkOutboundCapacityGoal, CpuCapacityGoal, ReplicaCapacityGoal] " +
                                    "in the provided goals: [RackAwareGoal, ReplicaCapacityGoal]. " +
                                    "Add skip_hard_goal_check=true parameter to ignore this sanity check.'.");
                    checkpoint.flag();
                })));
    }

    /**
     * Tests the transition from New to to ProposalReady skipping the hard goals check
     *
     * 1. A new rebalance resource is created with some specified not hard goals but with flag to skip the check; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The rebalance resource moves to ProposalReady state
     */
    @Test
    public void testNewToProposalReadySkipHardGoalsRebalance(VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .withSkipHardGoalCheck(true)
                .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec);

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {
                    // the resource moved from New directly to ProposalReady (no pending calls in the Mock server)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from New to NotReady due to "missing hard goals" error
     * The rebalance resource is update with skipping hard goals check flag and refreshed; it moves to ProposalReady state
     *
     * 1. A new rebalance resource is created with some specified not hard goals; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The operator gets a "missing hard goals" error instead of a proposal
     * 4. The rebalance resource moves to NotReady state
     * 5. The rebalance is updated with the skip hard goals check flag to "true" and annotated with strimzi.io/rebalance=refresh
     * 6. The operator requests a rebalance proposal to Cruise Control REST API
     * 7. The rebalance proposal is ready on the first call
     * 8. The rebalance resource moves to ProposalReady state
     */
    @Test
    public void testNewWithMissingHardGoalsAndRefresh(VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint to get error about hard goals
        MockCruiseControl.setupCCRebalanceBadGoalsError(ccServer);

        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec);

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceState.NotReady, CruiseControlRestException.class,
                            "Error processing POST request '/rebalance' due to: " +
                                    "'java.lang.IllegalArgumentException: Missing hard goals [NetworkInboundCapacityGoal, DiskCapacityGoal, RackAwareGoal, NetworkOutboundCapacityGoal, CpuCapacityGoal, ReplicaCapacityGoal] " +
                                    "in the provided goals: [RackAwareGoal, ReplicaCapacityGoal]. " +
                                    "Add skip_hard_goal_check=true parameter to ignore this sanity check.'.");
                }))).compose(v -> {

                    ccServer.reset();
                    try {
                        // Setup the rebalance endpoint with the number of pending calls before a response is received.
                        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
                    } catch (IOException | URISyntaxException e) {
                        context.failNow(e);
                    }

                    // set the skip hard goals check flag
                    KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    KafkaRebalance patchedKr = new KafkaRebalanceBuilder(kr2)
                            .editSpec()
                                .withSkipHardGoalCheck(true)
                            .endSpec()
                            .build();

                    Crds.kafkaRebalanceOperation(kubernetesClient)
                            .inNamespace(CLUSTER_NAMESPACE)
                            .withName(RESOURCE_NAME)
                            .patch(patchedKr);

                    // apply the "refresh" annotation to the resource in the NotReady state
                    patchedKr = annotate(kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceAnnotation.refresh);

                    // trigger another reconcile to process the NotReady state
                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            patchedKr);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from NotReady to ProposalReady
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from New to PendingProposal then the resource is deleted
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The rebalance proposal is not ready on the first call; the operator starts polling the Cruise Control REST API
     * 4. The rebalance resource moves to PendingProposal state
     * 5. The rebalance resource is deleted
     */
    @Test
    public void testNewToPendingProposalDeleteRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 2);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient, new Runnable() {
                int count = 0;

                @Override
                public void run() {
                    if (++count == 4) {
                        // delete the rebalance resource while on PendingProposal
                        Crds.kafkaRebalanceOperation(kubernetesClient)
                                .inNamespace(CLUSTER_NAMESPACE)
                                .withName(RESOURCE_NAME)
                                .delete();
                    }
                    return;
                }
            });

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.PendingProposal);
                })).compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr1);
                }).onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource doesn't exist anymore
                    KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertThat(kr2, is(nullValue()));
                    checkpoint.flag();
                })));
    }

    /**
     * Tests the transition from New to to ProposalReady
     * The rebalance proposal is approved and the resource moves to Rebalancing then to Stopped (via annotation)
     *
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The rebalance resource moves to ProposalReady state
     * 9. The rebalance resource is annotated with strimzi.io/rebalance=approve
     * 10. The operator requests the rebalance operation to Cruise Control REST API
     * 11. The rebalance operation is not done immediately; the operator starts polling the Cruise Control REST API
     * 12. The rebalance resource moves to Rebalancing state
     * 13. While the operator is waiting for the rebalance to be done, the rebalance resource is annotated with strimzi.io/rebalance=stop
     * 14. The operator stops polling the Cruise Control REST API and requests a stop execution
     * 15. The rebalance resource moves to Stopped state
     */
    @Test
    public void testNewToProposalReadyToRebalancingToStoppedRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, 2);
        MockCruiseControl.setupCCStopResponse(ccServer);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient, new Runnable() {
                int count = 0;

                @Override
                public void run() {
                    if (++count == 6) {
                        // after a while, apply the "stop" annotation to the resource in the Rebalancing state
                        annotate(kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceAnnotation.stop);
                    }
                    return;
                }
            });

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {
                    // the resource moved from New to ProposalReady directly (no pending calls in the Mock server)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                })).compose(v -> {
                    // apply the "approve" annotation to the resource in the ProposalReady state
                    KafkaRebalance approvedKr = annotate(kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceAnnotation.approve);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            approvedKr);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to Rebalancing on approval
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.Rebalancing);
                })).compose(v -> {
                    // trigger another reconcile to process the Rebalancing state
                    KafkaRebalance kr5 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr5);
                }).onComplete(context.succeeding(v -> {
                    // the resource moved from Rebalancing to Stopped
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.Stopped);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from New to NotReady due to missing Kafka cluster
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator checks that the Kafka cluster specified in the rebalance resource (via label) doesn't exist
     * 4. The rebalance resource moves to NotReady state
     */
    @Test
    public void testNoKafkaCluster(VertxTestContext context) {

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(null));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME,
                            KafkaRebalanceState.NotReady, NoSuchResourceException.class,
                            "Kafka resource '" + CLUSTER_NAME + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + CLUSTER_NAMESPACE  + ".");
                    checkpoint.flag();
                })));
    }

    /**
     * Tests the transition from New to NotReady due to missing Cruise Control deployment
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator checks that the Kafka cluster specified in the rebalance resource (via label) doesn't have Cruise Control configured
     * 4. The rebalance resource moves to NotReady state
     */
    @Test
    public void testNoCruiseControl(VertxTestContext context) {

        // build a Kafka cluster without the cruiseControl definition
        Kafka kafka =
                new KafkaBuilder(ResourceUtils.createKafka(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .editKafka()
                                .withVersion(version)
                            .endKafka()
                        .endSpec()
                        .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        // the Kafka cluster doesn't have the Cruise Control deployment
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME,
                            KafkaRebalanceState.NotReady, InvalidResourceException.class,
                            "Kafka resouce lacks 'cruiseControl' declaration : No deployed Cruise Control for doing a rebalance.");
                    checkpoint.flag();
                })));
    }

    /**
     * Tests the transition from New to NotReady due to missing Kafka cluster label in the resource
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator checks that the resource is missing the label to specify the Kafka cluster
     * 4. The rebalance resource moves to NotReady state
     */
    @Test
    public void testNoKafkaClusterInKafkaRebalanceLabel(VertxTestContext context) {

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, null, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME,
                            KafkaRebalanceState.NotReady, InvalidResourceException.class,
                            "Resource lacks label '" + Labels.STRIMZI_CLUSTER_LABEL + "': No cluster related to a possible rebalance.");
                    checkpoint.flag();
                })));
    }

    /**
     * Test the Cruise Control API REST client timing out
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The operator doesn't get a response on time; the resource moves to NotReady
     */
    @Test
    public void testCruiseControlTimingOut(VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received
        // and with a delay on response higher than the client timeout to test timing out
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0, 10);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {
                    // the resource moved from New to NotReady (mocked Cruise Control didn't reply on time)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME,
                            KafkaRebalanceState.NotReady, TimeoutException.class,
                            "The timeout period of 1000ms has been exceeded while executing POST");
                    checkpoint.flag();
                }));
    }

    /**
     * Test the Cruise Control server not reachable
     *
     * 1. A new rebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal to Cruise Control REST API
     * 3. The operator doesn't get a response on time; the resource moves to NotReady
     */
    @Test
    public void testCruiseControlNotReachable(VertxTestContext context) {

        // stop the mocked Cruise Control server to make it unreachable
        ccServer.stop();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> {

                    try {
                        ccServer = MockCruiseControl.getCCServer(CruiseControl.REST_API_PORT);
                    } catch (IOException | URISyntaxException e) {
                        context.failNow(e);
                    }

                    // the resource moved from New to NotReady (mocked Cruise Control not reachable)
                    assertState(context, kubernetesClient, CLUSTER_NAMESPACE, RESOURCE_NAME,
                            KafkaRebalanceState.NotReady, ConnectException.class,
                            "Connection refused");
                    checkpoint.flag();
                }));
    }

    public KafkaRebalanceState state(KubernetesClient kubernetesClient, String namespace, String resource) {
        KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(namespace).withName(resource).get();
        if (kafkaRebalance.getStatus() == null) {
            return KafkaRebalanceState.New;
        } else {
            return KafkaRebalanceState.valueOf(kcrao.rebalanceStateCondition(kafkaRebalance.getStatus()).getStatus());
        }
    }

    private KafkaRebalance annotate(KubernetesClient kubernetesClient, String namespace, String resource, KafkaRebalanceAnnotation annotationValue) {
        KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(namespace).withName(resource).get();

        KafkaRebalance patchedKr = new KafkaRebalanceBuilder(kafkaRebalance)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE, annotationValue.toString())
                .endMetadata()
                .build();

        Crds.kafkaRebalanceOperation(kubernetesClient)
                .inNamespace(namespace)
                .withName(resource)
                .patch(patchedKr);

        return patchedKr;
    }

    private void assertState(VertxTestContext context, KubernetesClient kubernetesClient, String namespace, String resource, KafkaRebalanceState state) {
        context.verify(() -> {
            KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(namespace).withName(resource).get();
            assertState(kafkaRebalance, state);
        });
    }

    private void assertState(VertxTestContext context, KubernetesClient kubernetesClient, String namespace, String resource, KafkaRebalanceState state, Class reason, String message) {
        context.verify(() -> {
            KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(namespace).withName(resource).get();
            assertState(kafkaRebalance, state, reason, message);
        });
    }

    private void assertState(KafkaRebalance kafkaRebalance, KafkaRebalanceState state) {
        assertThat(kafkaRebalance, notNullValue());
        assertThat(kafkaRebalance.getStatus(), notNullValue());
        assertThat(kafkaRebalance.getStatus().getConditions(), notNullValue());
        Condition stateCondition = kcrao.rebalanceStateCondition(kafkaRebalance.getStatus());
        assertThat(stateCondition, notNullValue());
        assertThat(stateCondition.getType(), is(state.toString()));
    }

    private void assertState(KafkaRebalance kafkaRebalance, KafkaRebalanceState state,
                             Class reason, String message) {
        assertState(kafkaRebalance, state);
        Condition stateCondition = kcrao.rebalanceStateCondition(kafkaRebalance.getStatus());
        assertThat(stateCondition.getReason(), is(reason.getSimpleName()));
        assertThat(stateCondition.getMessage(), containsString(message));
    }

    private KafkaRebalance createKafkaRebalance(String namespace, String clusterName, String resourceName,
                                                     KafkaRebalanceSpec kafkaRebalanceSpec) {
        KafkaRebalance kcRebalance = new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(resourceName)
                    .withLabels(clusterName != null ? Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME) : null)
                .endMetadata()
                .withSpec(kafkaRebalanceSpec)
                .build();

        return kcRebalance;
    }

    private void mockRebalanceOperator(CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList, DoneableKafkaRebalance> mockRebalanceOps,
                                       String namespace, String resource, KubernetesClient client) {
        mockRebalanceOperator(mockRebalanceOps, namespace, resource, client, null);
    }

    private void mockRebalanceOperator(CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList, DoneableKafkaRebalance> mockRebalanceOps,
                                       String namespace, String resource, KubernetesClient client, Runnable getAsyncFunction) {
        when(mockRebalanceOps.getAsync(namespace, resource)).thenAnswer(invocation -> {
            try {
                if (getAsyncFunction != null) {
                    getAsyncFunction.run();
                }
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(namespace)
                        .withName(resource)
                        .get());
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });
        when(mockRebalanceOps.updateStatusAsync(any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(namespace)
                        .withName(resource)
                        .patch(invocation.getArgument(0)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });
        when(mockRebalanceOps.patchAsync(any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(namespace)
                        .withName(resource)
                        .patch(invocation.getArgument(0)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });
    }
}
