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
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.MockCruiseControl;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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
import java.net.URISyntaxException;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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
            new KafkaBuilder(ResourceUtils.createKafkaCluster(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
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
        kcrao = new KafkaRebalanceAssemblyOperator(vertx, pfa, supplier, HOST);

        mockRebalanceOps = supplier.kafkaRebalanceOperator;
        mockKafkaOps = supplier.kafkaOperator;
    }

    @AfterEach
    public void afterEach() {
        if (kubernetesClient != null) {
            kubernetesClient.close();
        }
    }

    @Test
    public void testNewToProposalReadyRebalance(VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New directly to ProposalReady (no pending calls in the Mock server)
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.ProposalReady);
                    context.completeNow();
                })));
    }

    @Test
    public void testNewToPendingProposalToProposalReadyRebalance(VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 2);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.PendingProposal);
                }))).compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr1);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to ProposalReady
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.ProposalReady);
                    context.completeNow();
                })));
    }

    @Test
    public void testNewToPendingProposalToStoppedRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 5);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.PendingProposal);
                }))).compose(v -> {

                    vertx.setTimer(10000, t -> {
                        // after a while, apply the "stop" annotation to the resource in the PendingProposal state
                        KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                        KafkaRebalance stoppedKr = new KafkaRebalanceBuilder(kr2)
                                .editMetadata()
                                    .addToAnnotations(KafkaRebalanceAssemblyOperator.ANNO_STRIMZI_IO_REBALANCE, "stop")
                                .endMetadata()
                                .build();

                        Crds.kafkaRebalanceOperation(kubernetesClient)
                                .inNamespace(CLUSTER_NAMESPACE)
                                .withName(RESOURCE_NAME)
                                .patch(stoppedKr);
                    });

                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr1);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from ProposalPending to Stopped
                    KafkaRebalance kr3 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr3, KafkaRebalanceAssemblyOperator.State.Stopped);
                    context.completeNow();
                })));
    }

    @Test
    public void testNewToPendingProposalToStoppedAndRefreshRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 2);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.PendingProposal);
                }))).compose(v -> {

                    vertx.setTimer(2000, t -> {
                        // after a while, apply the "stop" annotation to the resource in the PendingProposal state
                        KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                        KafkaRebalance stoppedKr = new KafkaRebalanceBuilder(kr2)
                                .editMetadata()
                                    .addToAnnotations(KafkaRebalanceAssemblyOperator.ANNO_STRIMZI_IO_REBALANCE, "stop")
                                .endMetadata()
                                .build();

                        Crds.kafkaRebalanceOperation(kubernetesClient)
                                .inNamespace(CLUSTER_NAMESPACE)
                                .withName(RESOURCE_NAME)
                                .patch(stoppedKr);
                    });

                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr1);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from ProposalPending to Stopped
                    KafkaRebalance kr3 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr3, KafkaRebalanceAssemblyOperator.State.Stopped);
                }))).compose(v -> {

                    // apply the "refresh" annotation to the resource in the Stopped state
                    KafkaRebalance kr4 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    KafkaRebalance refreshedKr = new KafkaRebalanceBuilder(kr4)
                            .editMetadata()
                                .addToAnnotations(KafkaRebalanceAssemblyOperator.ANNO_STRIMZI_IO_REBALANCE, "refresh")
                            .endMetadata()
                            .build();

                    Crds.kafkaRebalanceOperation(kubernetesClient)
                            .inNamespace(CLUSTER_NAMESPACE)
                            .withName(RESOURCE_NAME)
                            .patch(refreshedKr);

                    // trigger another reconcile to process the Stopped state
                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            refreshedKr);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from Stopped to PendingProposal
                    KafkaRebalance kr5 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr5, KafkaRebalanceAssemblyOperator.State.PendingProposal);
                }))).compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr6 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr6);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from PendingProposal to ProposalReady
                    KafkaRebalance kr7 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr7, KafkaRebalanceAssemblyOperator.State.ProposalReady);
                    context.completeNow();
                })));
    }

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

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to ProposalReady directly (no pending calls in the Mock server)
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.ProposalReady);
                }))).compose(v -> {
                    // apply the "approve" annotation to the resource in the ProposalReady state
                    KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    KafkaRebalance approvedKr = new KafkaRebalanceBuilder(kr2)
                            .editMetadata()
                                .addToAnnotations(KafkaRebalanceAssemblyOperator.ANNO_STRIMZI_IO_REBALANCE, "approve")
                            .endMetadata()
                            .build();

                    Crds.kafkaRebalanceOperation(kubernetesClient)
                            .inNamespace(CLUSTER_NAMESPACE)
                            .withName(RESOURCE_NAME)
                            .patch(approvedKr);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            approvedKr);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from ProposalReady to Rebalancing on approval
                    KafkaRebalance kr3 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr3, KafkaRebalanceAssemblyOperator.State.Rebalancing);
                }))).compose(v -> {
                    // trigger another reconcile to process the Rebalancing state
                    KafkaRebalance kr4 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr4);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from Rebalancing to Ready
                    KafkaRebalance kr4 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr4, KafkaRebalanceAssemblyOperator.State.Ready);
                    context.completeNow();
                })));
    }

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

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.NotReady, RuntimeException.class,
                            "Error processing POST request '/rebalance' due to: " +
                                    "'java.lang.IllegalArgumentException: Missing hard goals [NetworkInboundCapacityGoal, DiskCapacityGoal, RackAwareGoal, NetworkOutboundCapacityGoal, CpuCapacityGoal, ReplicaCapacityGoal] " +
                                    "in the provided goals: [RackAwareGoal, ReplicaCapacityGoal]. " +
                                    "Add skip_hard_goal_check=true parameter to ignore this sanity check.'.");
                    context.completeNow();
                })));
    }

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

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New directly to ProposalReady (no pending calls in the Mock server)
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.ProposalReady);
                    context.completeNow();
                })));
    }

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

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.NotReady, RuntimeException.class,
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

                    // apply the "refresh" annotation to the resource in the NotReady state and fix the skip hard goals check flag
                    KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    KafkaRebalance patchedKr = new KafkaRebalanceBuilder(kr2)
                            .editMetadata()
                                .addToAnnotations(KafkaRebalanceAssemblyOperator.ANNO_STRIMZI_IO_REBALANCE, "refresh")
                            .endMetadata()
                            .editSpec()
                                .withSkipHardGoalCheck(true)
                            .endSpec()
                            .build();

                    Crds.kafkaRebalanceOperation(kubernetesClient)
                            .inNamespace(CLUSTER_NAMESPACE)
                            .withName(RESOURCE_NAME)
                            .patch(patchedKr);

                    // trigger another reconcile to process the NotReady state
                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            patchedKr);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from NotReady to ProposalReady
                    KafkaRebalance kr3 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr3, KafkaRebalanceAssemblyOperator.State.ProposalReady);
                    context.completeNow();
                })));
    }

    @Test
    public void testNewToPendingProposalDeleteRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 2);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.PendingProposal);
                }))).compose(v -> {

                    vertx.setTimer(2000, t -> {

                        Crds.kafkaRebalanceOperation(kubernetesClient)
                                .inNamespace(CLUSTER_NAMESPACE)
                                .withName(RESOURCE_NAME)
                                .delete();
                    });

                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr1);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from ProposalPending to Stopped
                    KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertThat(kr2, is(nullValue()));
                    context.completeNow();
                })));
    }

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
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to ProposalReady directly (no pending calls in the Mock server)
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.ProposalReady);
                }))).compose(v -> {
                    // apply the "approve" annotation to the resource in the ProposalReady state
                    KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    KafkaRebalance approvedKr = new KafkaRebalanceBuilder(kr2)
                            .editMetadata()
                                .addToAnnotations(KafkaRebalanceAssemblyOperator.ANNO_STRIMZI_IO_REBALANCE, "approve")
                            .endMetadata()
                            .build();

                    Crds.kafkaRebalanceOperation(kubernetesClient)
                            .inNamespace(CLUSTER_NAMESPACE)
                            .withName(RESOURCE_NAME)
                            .patch(approvedKr);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            approvedKr);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from ProposalReady to Rebalancing on approval
                    KafkaRebalance kr3 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr3, KafkaRebalanceAssemblyOperator.State.Rebalancing);
                }))).compose(v -> {

                    vertx.setTimer(2000, t -> {
                        // after a while, apply the "stop" annotation to the resource in the Rebalancing state
                        KafkaRebalance kr4 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                        KafkaRebalance stoppedKr = new KafkaRebalanceBuilder(kr4)
                                .editMetadata()
                                    .addToAnnotations(KafkaRebalanceAssemblyOperator.ANNO_STRIMZI_IO_REBALANCE, "stop")
                                .endMetadata()
                                .build();

                        Crds.kafkaRebalanceOperation(kubernetesClient)
                                .inNamespace(CLUSTER_NAMESPACE)
                                .withName(RESOURCE_NAME)
                                .patch(stoppedKr);
                    });

                    // trigger another reconcile to process the Rebalancing state
                    KafkaRebalance kr5 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr5);
                }).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from Rebalancing to Stopped
                    KafkaRebalance kr6 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr6, KafkaRebalanceAssemblyOperator.State.Stopped);
                    context.completeNow();
                })));
    }

    @Test
    public void testNoKafkaCluster(VertxTestContext context) {

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(null));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.NotReady, NoSuchResourceException.class,
                            "Kafka resource '" + CLUSTER_NAME + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + CLUSTER_NAMESPACE  + ".");
                    context.completeNow();
                })));
    }

    @Test
    public void testNoCruiseControl(VertxTestContext context) {

        // build a Kafka cluster without the cruiseControl definition
        Kafka kafka =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
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

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.NotReady, InvalidResourceException.class,
                            "Kafka resouce lacks 'cruiseControl' declaration : No deployed Cruise Control for doing a rebalance.");
                    context.completeNow();
                })));
    }

    @Test
    public void testNoKafkaClusterInKafkaRebalanceLabel(VertxTestContext context) {

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, null, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, CLUSTER_NAMESPACE, RESOURCE_NAME, kubernetesClient);

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.NotReady, InvalidResourceException.class,
                            "Resource lacks label '" + Labels.STRIMZI_CLUSTER_LABEL + "': No cluster related to a possible rebalance.");
                    context.completeNow();
                })));
    }

    private void assertState(KafkaRebalance kafkaRebalance, KafkaRebalanceAssemblyOperator.State state) {
        assertThat(kafkaRebalance, notNullValue());
        assertThat(kafkaRebalance.getStatus(), notNullValue());
        assertThat(kafkaRebalance.getStatus().getConditions(), notNullValue());
        Condition stateCondition = kcrao.rebalanceStateCondition(kafkaRebalance.getStatus());
        assertThat(stateCondition, notNullValue());
        assertThat(stateCondition.getStatus(), is(state.toString()));
    }

    private void assertState(KafkaRebalance kafkaRebalance, KafkaRebalanceAssemblyOperator.State state,
                             Class reason, String message) {
        assertState(kafkaRebalance, state);
        Condition stateCondition = kcrao.rebalanceStateCondition(kafkaRebalance.getStatus());
        assertThat(stateCondition.getReason(), is(reason.getSimpleName()));
        assertThat(stateCondition.getMessage(), is(message));
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
        when(mockRebalanceOps.getAsync(namespace, resource)).thenAnswer(invocation -> {
            try {
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
