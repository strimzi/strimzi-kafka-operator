/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpecBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceStatus;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlRestException;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlRetriableConnectionException;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.MockCruiseControl;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.impl.NoStackTraceTimeoutException;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class KafkaRebalanceAssemblyOperatorTest {
    private static final String HOST = "localhost";
    private static final String RESOURCE_NAME = "my-rebalance";
    private static final String CLUSTER_NAME = "kafka-cruise-control-test-cluster";
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
    private static final int REPLICAS = 3;
    private static final String IMAGE = "my-kafka-image";
    private static final int HEALTH_DELAY = 120;
    private static final int HEALTH_TIMEOUT = 30;
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private static MockCruiseControl ccServer;
    private static KubernetesClient client;
    private static MockKube3 mockKube;
    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private String namespace;
    private KafkaRebalanceAssemblyOperator krao;
    private ResourceOperatorSupplier supplier;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaRebalanceCrd()
                .withDeletionController()
                .build();
        mockKube.start();
        client = mockKube.client();

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");

        ccServer = new MockCruiseControl(CruiseControl.REST_API_PORT);
    }

    @AfterAll
    public static void afterAll() {
        sharedWorkerExecutor.close();
        vertx.close();
        mockKube.stop();
        ccServer.stop();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        // handling Kubernetes constraint for namespace limit name (63 characters)
        if (namespace.length() > 63) {
            namespace = namespace.substring(0, 63);
        }
        mockKube.prepareNamespace(namespace);

        ccServer.reset();

        supplier =  new ResourceOperatorSupplier(vertx, client, ResourceUtils.zookeeperLeaderFinder(vertx, client), ResourceUtils.adminClientProvider(),
                ResourceUtils.zookeeperScalerProvider(), ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(), PFA, 2_000);

        // Override to inject mocked cruise control address so real cruise control not required
        krao = new KafkaRebalanceAssemblyOperator(vertx, supplier, ResourceUtils.dummyClusterOperatorConfig()) {
            @Override
            public String cruiseControlHost(String clusterName, String clusterNamespace) {
                return HOST;
            }

            @Override
            public CruiseControlApi cruiseControlClientProvider(Secret ccSecret, Secret ccApiSecret, boolean apiAuthEnabled, boolean apiSslEnabled) {
                return new CruiseControlApiImpl(vertx, 1, ccSecret, ccApiSecret, true, true);
            }
        };
    }

    @AfterEach
    public void afterEach() {
        client.namespaces().withName(namespace).delete();
    }

    private void crdCreateKafka() {
        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(namespace, CLUSTER_NAME, REPLICAS, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT))
                .editSpec()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .withNewStatus()
                    .withObservedGeneration(1L)
                    .withConditions(new ConditionBuilder()
                            .withType("Ready")
                            .withStatus("True")
                            .build())
                .endStatus()
                .build();

        Crds.kafkaOperation(client).inNamespace(namespace).resource(kafka).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(kafka).updateStatus();
    }

    private void crdCreateCruiseControlSecrets() {
        Secret ccSecret = new SecretBuilder(MockCruiseControl.CC_SECRET)
                .editMetadata()
                    .withName(CruiseControlResources.secretName(CLUSTER_NAME))
                    .withNamespace(namespace)
                .endMetadata()
                .build();

        Secret ccApiSecret = new SecretBuilder(MockCruiseControl.CC_API_SECRET)
                .editMetadata()
                    .withName(CruiseControlResources.apiSecretName(CLUSTER_NAME))
                    .withNamespace(namespace)
                .endMetadata()
                .build();

        client.secrets().inNamespace(namespace).resource(ccSecret).create();
        client.secrets().inNamespace(namespace).resource(ccApiSecret).create();
    }

    /**
     * Tests the transition from 'New' to 'ProposalReady'
     *
     * 1. A new KafkaRebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource moves to the 'ProposalReady' state
     */
    @Test
    public void testNewToProposalReadyRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReady(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReady(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReady(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReady(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(0, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from 'New' directly to 'ProposalReady' (no pending calls in the Mock server)
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'NotReady' to 'ProposalReady' when
     * the kafkaRebalance resource  spec is updated with "skip hard goals check" state
     *
     * 1. A new KafkaRebalance resource is created with some specified not hard goals; it is in the New state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The operator receives a "missing hard goals" error instead of a proposal
     * 4. The KafkaRebalance resource moves to the 'NotReady' state
     * 5. The rebalance spec is updated with the 'skip hard goals check' field to "true"
     * 6. The operator requests a rebalance proposal through the Cruise Control REST API
     * 7. The rebalance proposal is ready on the first call
     * 8. The KafkaRebalance resource moves to the 'ProposalReady' state
     */
    @Test
    public void testKrNotReadyToProposalReadyOnSpecChange(VertxTestContext context) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint to get error about hard goals
        ccServer.setupCCRebalanceBadGoalsError(CruiseControlEndpoints.REBALANCE);

        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .build();
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(kr.getMetadata().getName()).get();
                    assertThat(kr1, StateMatchers.hasState());
                    Condition condition = krao.rebalanceStateCondition(kr1.getStatus());
                    assertThat(condition, StateMatchers.hasStateInCondition(KafkaRebalanceState.NotReady, CruiseControlRestException.class,
                            "Error processing POST request '/rebalance' due to: " +
                                    "'java.lang.IllegalArgumentException: Missing hard goals [NetworkInboundCapacityGoal, DiskCapacityGoal, RackAwareGoal, NetworkOutboundCapacityGoal, CpuCapacityGoal, ReplicaCapacityGoal] " +
                                    "in the provided goals: [RackAwareGoal, ReplicaCapacityGoal]. " +
                                    "Add skip_hard_goal_check=true parameter to ignore this sanity check.'."));
                })))
                .compose(v -> {

                    ccServer.reset();
                    try {
                        // Setup the rebalance endpoint with the number of pending calls before a response is received.
                        ccServer.setupCCRebalanceResponse(0, CruiseControlEndpoints.REBALANCE);
                    } catch (IOException | URISyntaxException e) {
                        context.failNow(e);
                    }

                    Crds.kafkaRebalanceOperation(client)
                            .inNamespace(namespace)
                            .withName(kr.getMetadata().getName())
                            .edit(currentKr -> new KafkaRebalanceBuilder(currentKr)
                                    .editSpec()
                                        .withSkipHardGoalCheck(true)
                                    .endSpec()
                                    .build());

                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource transitioned from 'NotReady' to 'ProposalReady'
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'PendingProposal' to 'ProposalReady'
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator requests a rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is not ready on the first call; the operator starts polling the Cruise Control REST API
     * 4. The KafkaRebalance resource moves to 'PendingProposal' state
     * 5. The rebalance proposal is ready after the specified pending calls
     * 6. The KafkaRebalance resource moves to 'ProposalReady' state
     */
    @Test
    public void testNewToPendingProposalToProposalReadyRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToProposalReady(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToProposalReadyAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToProposalReady(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToProposalReadyRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToProposalReady(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToPendingProposalToProposalReady(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(2, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from New to PendingProposal (proposal not ready yet due to the configured CC mock server pending calls)
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal);
                }))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource stays in PendingProposal (proposal not ready yet due to the configured CC mock server pending calls)
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal);
                }))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from PendingProposal to ProposalReady
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
                    context.verify(() -> {
                        KafkaRebalanceStatus rebalanceStatus = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(kr.getMetadata().getName()).get().getStatus();
                        assertTrue(rebalanceStatus.getOptimizationResult().size() > 0);
                        assertNotNull(rebalanceStatus.getSessionId());
                    });
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'PendingProposal' and then 'Stopped' (via annotation)
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator requests a rebalance proposal via the Cruise Control REST API
     * 3. The rebalance proposal is not ready yet; the operator starts polling the Cruise Control REST API
     * 4. The KafkaRebalance resource transitions to the 'PendingProposal' state
     * 6. While the operator is waiting for the proposal, the KafkaRebalance resource is annotated with strimzi.io/rebalance=stop
     * 7. The operator stops polling the Cruise Control REST API
     * 8. The KafkaRebalance resource moves to 'Stopped' state
     */
    @Test
    public void testNewToPendingProposalToStoppedRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStopped(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToStoppedRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToStoppedAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStopped(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToStoppedRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToStoppedRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStopped(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToPendingProposalToStopped(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(5, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from New to PendingProposal (proposal not ready yet due to the configured CC mock server pending calls)
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal);
                }))
                .compose(v -> {
                    // apply the "stop" annotation to the resource in the PendingProposal state
                    annotate(client, namespace, kr.getMetadata().getName(), KafkaRebalanceAnnotation.stop);

                    // trigger another reconcile to process the PendingProposal state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from PendingProposal to Stopped
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Stopped);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'PendingProposal' to 'Stopped' (via annotation)
     * The resource is refreshed and transitions to 'PendingProposal' again and finally to 'ProposalReady'
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The rebalance proposal is not ready yet; the operator starts polling the Cruise Control REST API
     * 4. The KafkaRebalance resource transitions to the 'PendingProposal' state
     * 6. While the operator is waiting for the proposal, the KafkaRebalance resource is annotated with 'strimzi.io/rebalance=stop'
     * 7. The operator stops polling the Cruise Control REST API
     * 8. The KafkaRebalance resource moves to the 'Stopped' state
     * 9. The KafkaRebalance resource is annotated with 'strimzi.io/rebalance=refresh'
     * 10. The operator requests a rebalance proposal through the Cruise Control REST API
     * 11. The rebalance proposal is not ready yet; the operator starts polling the Cruise Control REST API
     * 12. The KafkaRebalance resource moves to PendingProposal state
     * 13. The rebalance proposal is ready after the specified pending calls
     * 14. The KafkaRebalance resource moves to ProposalReady state
     */
    @Test
    public void testNewToPendingProposalToStoppedAndRefreshRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStoppedAndRefresh(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToStoppedAndRefreshRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToStoppedAndRefreshAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStoppedAndRefresh(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToStoppedAndRefreshRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToStoppedAndRefreshRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStoppedAndRefresh(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToPendingProposalToStoppedAndRefresh(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(2, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                // the resource moved from New to PendingProposal (proposal not ready yet due to the configured CC mock server pending calls)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal)))
                .compose(v -> {
                    // apply the "stop" annotation to the resource in the PendingProposal state
                    annotate(client, namespace, kr.getMetadata().getName(), KafkaRebalanceAnnotation.stop);

                    // trigger another reconcile to process the PendingProposal state
                    return krao.reconcile(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                // the resource moved from PendingProposal to Stopped
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Stopped)))
                .compose(v -> {
                    // apply the "refresh" annotation to the resource in the Stopped state
                    annotate(client, namespace, kr.getMetadata().getName(), KafkaRebalanceAnnotation.refresh);

                    // trigger another reconcile to process the Stopped state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                // the resource moved from Stopped to PendingProposal (proposal not ready yet due to the configured CC mock server pending calls)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal)))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(kr.getMetadata().getName()).get();

                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from PendingProposal to ProposalReady
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'ProposalReady'
     * The rebalance proposal is approved and the resource moves to 'Rebalancing' and finally to 'Ready'
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource transitions to the 'ProposalReady' state
     * 9. The KafkaRebalance resource is annotated with 'strimzi.io/rebalance=approve'
     * 10. The operator requests the rebalance operation through the Cruise Control REST API
     * 11. The rebalance operation is not done immediately; the operator starts polling the Cruise Control REST API
     * 12. The KafkaRebalance resource moves to the 'Rebalancing' state
     * 13. The rebalance operation is done
     * 14. The KafkaRebalance resource moves to the 'Ready' state
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReady(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReady(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReady(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    /**
     * Tests the transition from 'New' to 'ProposalReady'
     * The rebalance proposal is auto-approved and the resource moves to 'Rebalancing' and finally to 'Ready'
     *
     * 1. A new KafkaRebalance resource is created with the 'strimzi.io/rebalance-auto-approval' annotation; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource transitions to the 'ProposalReady' state
     * 5. No need to approve because of the 'strimzi.io/rebalance-auto-approval' annotation
     * 6. The operator requests the rebalance operation through the Cruise Control REST API
     * 7. The rebalance operation is not done immediately; the operator starts polling the Cruise Control REST API
     * 8. The KafkaRebalance resource moves to the 'Rebalancing' state
     * 9. The rebalance operation is done
     * 10. The KafkaRebalance resource moves to the 'Ready' state
     */
    @Test
    public void testAutoApprovalNewToProposalReadyToRebalancingToReadyRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        this.krNewToProposalReadyToRebalancingToReady(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testAutoApprovalNewToProposalReadyToRebalancingToReadyRebalance} for description
     */
    @Test
    public void testAutoApprovalNewToProposalReadyToRebalancingToReadyAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, true);
        this.krNewToProposalReadyToRebalancingToReady(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testAutoApprovalNewToProposalReadyToRebalancingToReadyRebalance} for description
     */
    @Test
    public void testAutoApprovalNewToReadyRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, true);
        this.krNewToProposalReadyToRebalancingToReady(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReadyToRebalancingToReady(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(0, endpoint);
        ccServer.setupCCUserTasksResponseNoGoals(0, 0);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                // the resource moved from 'New' to 'ProposalReady' directly (no pending calls in the Mock server)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady)))
                .compose(v -> {

                    // if "rebalance-auto-approval" annotation is set, no need for applying the "approve" annotation
                    // otherwise apply the "approve" annotation to the resource in the ProposalReady state
                    if (!Annotations.booleanAnnotation(kr, Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, false)) {
                        annotate(client, namespace, kr.getMetadata().getName(), KafkaRebalanceAnnotation.approve);
                    }

                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to Rebalancing on approval
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Rebalancing);
                }))
                .compose(v -> {
                    // trigger another reconcile to process the Rebalancing state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from Rebalancing to Ready
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Ready);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'ProposalReady' to `ReconciliationPaused`
     * The rebalance is paused and the resource moves to 'ReconciliationPaused'
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource transitions to the 'ProposalReady' state
     * 9. The KafkaRebalance resource is annotated with 'strimzi.io/pause-reconciliation=true'
     * 12. The KafkaRebalance resource moves to the 'ReconciliationPaused' state
     */
    @Test
    public void testNewToProposalReadyToReconciliationPausedRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToReconciliationPaused(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToReconciliationPausedRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToReconciliationPausedAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToReconciliationPaused(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToReconciliationPausedRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToReconciliationPausedRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToReconciliationPaused(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReadyToReconciliationPaused(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(0, endpoint);
        ccServer.setupCCUserTasksResponseNoGoals(0, 0);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                // the resource moved from 'New' to 'ProposalReady' directly (no pending calls in the Mock server)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady)))
                .compose(v -> {
                    // set the `ReconciliationPaused` annotation as true
                    KafkaRebalance patchedKr =
                            new KafkaRebalanceBuilder(Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(kr.getMetadata().getName()).get())
                                    .editMetadata()
                                        .addToAnnotations(Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"))
                                    .endMetadata()
                                    .build();

                    Crds.kafkaRebalanceOperation(client)
                            .inNamespace(namespace)
                            .withName(kr.getMetadata().getName())
                            .edit(currentKr -> patchedKr);

                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to ReconciliationPaused on pausing
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ReconciliationPaused);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'ProposalReady'
     * The rebalance proposal is approved and the resource moves to 'Rebalancing' and finally to 'Ready'
     * Then the Ready KafkaRebalance is refreshed and a moved to 'ProposalReady' again.
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource transitions to the 'ProposalReady' state
     * 5. The KafkaRebalance resource is annotated with 'strimzi.io/rebalance=approve'
     * 6. The operator requests the rebalance operation through the Cruise Control REST API
     * 7. The rebalance operation is not done immediately; the operator starts polling the Cruise Control REST API
     * 8. The KafkaRebalance resource moves to the 'Rebalancing' state
     * 9. The rebalance operation is done
     * 10. The KafkaRebalance resource moves to the 'Ready' state
     * 11. The KafkaRebalance resource is annotated with 'strimzi.io/rebalance=refresh'
     * 12. The KafkaRebalance resource moves to the 'ProposalReady' state
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyThenRefreshRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReadyThenRefresh(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToReadyThenRefreshRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyThenRefreshAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReadyThenRefresh(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToReadyThenRefreshRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyThenRefreshRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReadyThenRefresh(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReadyToRebalancingToReadyThenRefresh(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(0, endpoint);
        ccServer.setupCCUserTasksResponseNoGoals(0, 0);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                // the resource moved from 'New' to 'ProposalReady' directly (no pending calls in the Mock server)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady)))
                .compose(v -> {
                    // apply the "approve" annotation to the resource in the ProposalReady state
                    annotate(client, namespace, kr.getMetadata().getName(), KafkaRebalanceAnnotation.approve);

                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to Rebalancing on approval
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Rebalancing);
                }))
                .compose(v -> {
                    // trigger another reconcile to process the Rebalancing state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from Rebalancing to Ready
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Ready);
                }))
                .compose(v -> {
                    // apply the "refresh" annotation to the resource in the ProposalReady state
                    annotate(client, namespace, kr.getMetadata().getName(), KafkaRebalanceAnnotation.refresh);

                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from Ready to ProposalReady
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'NotReady' due to "missing hard goals" error
     *
     * 1. A new KafkaRebalance resource is created with some specified not hard goals; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The operator gets a "missing hard goals" error instead of a proposal
     * 4. The KafkaRebalance resource moves to the 'NotReady' state
     */
    @Test
    public void testNewWithMissingHardGoalsRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoals(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewWithMissingHardGoalsRebalance} for description
     */
    @Test
    public void testNewWithMissingHardGoalsAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                .withBrokers(3)
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoals(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewWithMissingHardGoalsRebalance} for description
     */
    @Test
    public void testNewWithMissingHardGoalsRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                .withBrokers(3)
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoals(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewWithMissingHardGoals(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint to get error about hard goals
        ccServer.setupCCRebalanceBadGoalsError(endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(kr.getMetadata().getName()).get();
                    assertThat(kr1, StateMatchers.hasState());
                    Condition condition = krao.rebalanceStateCondition(kr1.getStatus());
                    assertThat(condition, StateMatchers.hasStateInCondition(KafkaRebalanceState.NotReady, CruiseControlRestException.class,
                            "Error processing POST request '/rebalance' due to: " +
                                    "'java.lang.IllegalArgumentException: Missing hard goals [NetworkInboundCapacityGoal, DiskCapacityGoal, RackAwareGoal, NetworkOutboundCapacityGoal, CpuCapacityGoal, ReplicaCapacityGoal] " +
                                    "in the provided goals: [RackAwareGoal, ReplicaCapacityGoal]. " +
                                    "Add skip_hard_goal_check=true parameter to ignore this sanity check.'."));
                    checkpoint.flag();
                })));
    }

    /**
     * Tests the transition from 'New' to 'ProposalReady' skipping the hard goals check
     *
     * 1. A new KafkaRebalance resource is created with some specified goals not included in the hard goals but with flag to skip the check; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource transitions to the 'ProposalReady' state
     */
    @Test
    public void testNewToProposalReadySkipHardGoalsRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .withSkipHardGoalCheck(true)
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewToProposalReadySkipHardGoals(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadySkipHardGoalsRebalance} for description
     */
    @Test
    public void testNewToProposalReadySkipHardGoalsAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                .withBrokers(3)
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .withSkipHardGoalCheck(true)
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewToProposalReadySkipHardGoals(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadySkipHardGoalsRebalance} for description
     */
    @Test
    public void testNewToProposalReadySkipHardGoalsRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                .withBrokers(3)
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .withSkipHardGoalCheck(true)
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewToProposalReadySkipHardGoals(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReadySkipHardGoals(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(0, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from New directly to ProposalReady (no pending calls in the Mock server)
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'NotReady' due to "missing hard goals" error
     * The KafkaRebalance resource is updated with "skip hard goals check" and refreshed; it then moves to the 'ProposalReady' state
     *
     * 1. A new KafkaRebalance resource is created with some specified not hard goals; it is in the New state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The operator receives a "missing hard goals" error instead of a proposal
     * 4. The KafkaRebalance resource moves to the 'NotReady' state
     * 5. The rebalance is updated with the 'skip hard goals check' field to "true" and annotated with 'strimzi.io/rebalance=refresh'
     * 6. The operator requests a rebalance proposal through the Cruise Control REST API
     * 7. The rebalance proposal is ready on the first call
     * 8. The KafkaRebalance resource moves to the 'ProposalReady' state
     */
    @Test
    public void testNewWithMissingHardGoalsAndRefreshRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoalsAndRefresh(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewWithMissingHardGoalsAndRefreshRebalance} for description
     */
    @Test
    public void testNewWithMissingHardGoalsAndRefreshAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                .withBrokers(3)
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoalsAndRefresh(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewWithMissingHardGoalsAndRefreshRebalance} for description
     */
    @Test
    public void testNewWithMissingHardGoalsAndRefreshRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                .withBrokers(3)
                .withGoals("DiskCapacityGoal", "CpuCapacityGoal")
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoalsAndRefresh(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewWithMissingHardGoalsAndRefresh(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint to get error about hard goals
        ccServer.setupCCRebalanceBadGoalsError(endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(kr.getMetadata().getName()).get();
                    assertThat(kr1, StateMatchers.hasState());
                    Condition condition = krao.rebalanceStateCondition(kr1.getStatus());
                    assertThat(condition, StateMatchers.hasStateInCondition(KafkaRebalanceState.NotReady, CruiseControlRestException.class,
                            "Error processing POST request '/rebalance' due to: " +
                                    "'java.lang.IllegalArgumentException: Missing hard goals [NetworkInboundCapacityGoal, DiskCapacityGoal, RackAwareGoal, NetworkOutboundCapacityGoal, CpuCapacityGoal, ReplicaCapacityGoal] " +
                                    "in the provided goals: [RackAwareGoal, ReplicaCapacityGoal]. " +
                                    "Add skip_hard_goal_check=true parameter to ignore this sanity check.'."));
                })))
                .compose(v -> {

                    ccServer.reset();
                    try {
                        // Setup the rebalance endpoint with the number of pending calls before a response is received.
                        ccServer.setupCCRebalanceResponse(0, endpoint);
                    } catch (IOException | URISyntaxException e) {
                        context.failNow(e);
                    }

                    // set the skip hard goals check flag
                    Crds.kafkaRebalanceOperation(client)
                            .inNamespace(namespace)
                            .withName(kr.getMetadata().getName())
                            .edit(currentKr -> new KafkaRebalanceBuilder(currentKr)
                                    .editSpec()
                                        .withSkipHardGoalCheck(true)
                                    .endSpec()
                                    .build());

                    // apply the "refresh" annotation to the resource in the NotReady state
                    annotate(client, namespace, kr.getMetadata().getName(), KafkaRebalanceAnnotation.refresh);

                    // trigger another reconcile to process the NotReady state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource transitioned from 'NotReady' to 'ProposalReady'
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'PendingProposal' then the resource is deleted
     *
     * 1. A new KafkaRebalance resource is created; it is in the New state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The rebalance proposal is not ready on the first call; the operator starts polling the Cruise Control REST API
     * 4. The KafkaRebalance resource moves to the 'PendingProposal' state
     * 5. The KafkaRebalance resource is deleted
     */
    @Test
    public void testNewToPendingProposalDeleteRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalDelete(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalDeleteRebalance} for description
     */
    @Test
    public void testNewToPendingProposalDeleteAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalDelete(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalDeleteRebalance} for description
     */
    @Test
    public void testNewToPendingProposalDeleteRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalDelete(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToPendingProposalDelete(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(2, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                // the resource moved from 'New' to 'PendingProposal' (due to the configured Mock server pending calls)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal)))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal)))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(kr.getMetadata().getName()).delete();
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource should not exist anymore
                    KafkaRebalance currentKR = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(kr.getMetadata().getName()).get();
                    assertThat(currentKR, is(nullValue()));
                    checkpoint.flag();
                })));
    }

    /**
     * Tests the transition from 'New' to 'ProposalReady'
     * The rebalance proposal is approved and the resource moves to 'Rebalancing' then to 'Stopped' (via annotation)
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The rebalance proposal is ready on the first call
     * 4. The KafkaRebalance resource moves to the 'ProposalReady' state
     * 9. The KafkaRebalance resource is annotated with 'strimzi.io/rebalance=approve'
     * 10. The operator requests the rebalance operation through the Cruise Control REST API
     * 11. The rebalance operation is not done immediately; the operator starts polling the Cruise Control REST API
     * 12. The KafkaRebalance resource moves to the 'Rebalancing' state
     * 13. While the operator is waiting for the rebalance to be done, the KafkaRebalance resource is annotated with 'strimzi.io/rebalance=stop'
     * 14. The operator stops polling the Cruise Control REST API and requests a stop execution
     * 15. The KafkaRebalance resource moves to the 'Stopped' state
     */
    @Test
    public void testNewToProposalReadyToRebalancingToStoppedRebalance(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToStopped(context, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToStoppedRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToStoppedAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToStopped(context, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToStoppedRebalance} for description
     */
    @Test
    @Timeout(value = 60, timeUnit = TimeUnit.MINUTES)
    public void testNewToProposalReadyToRebalancingToStoppedRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToStopped(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReadyToRebalancingToStopped(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(0, endpoint);
        ccServer.setupCCUserTasksResponseNoGoals(0, 2);
        ccServer.setupCCStopResponse();

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()))
                // the resource moved from New to ProposalReady directly (no pending calls in the CC mock server)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady)))
                .compose(v -> {
                    // apply the "approve" annotation to the resource in the ProposalReady state
                    annotate(client, namespace, kr.getMetadata().getName(), KafkaRebalanceAnnotation.approve);

                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                // the resource moved from ProposalReady to Rebalancing on approval
                .onComplete(context.succeeding(v ->
                        assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Rebalancing)))
                .compose(v -> {
                    // apply the "stop" annotation to the resource in the Rebalancing state
                    annotate(client, namespace, kr.getMetadata().getName(), KafkaRebalanceAnnotation.stop);

                    // trigger another reconcile to process the Rebalancing state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, kr.getMetadata().getName()));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from Rebalancing to Stopped
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Stopped);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'NotReady' due to missing Kafka cluster
     *
     * 1. A new KafkaRebalance resource is created; it is in the New state
     * 2. The operator checks that the Kafka cluster specified in the KafkaRebalance resource (via label) doesn't exist
     * 4. The KafkaRebalance resource moves to NotReady state
     */
    @Test
    public void testNoKafkaCluster(VertxTestContext context) {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    assertState(context, client, namespace, RESOURCE_NAME,
                            KafkaRebalanceState.NotReady, NoSuchResourceException.class,
                            "Kafka resource '" + CLUSTER_NAME + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + namespace + ".");
                    checkpoint.flag();
                })));
    }

    /**
     * Tests that `KafkaRebalance` stays in `Ready` state when the Kafka cluster moves to `NotReady` state.
     *
     * 1. A new KafkaRebalance resource is created. It is moved directly to ready state
     * 2. The operator checks if the rebalance resource is in `Ready` state or not
     * 3. The Kafka cluster is now moved to `NotReady` state
     * 4. The KafkaRebalance resource still remains in `Ready` state
     */
    @Test
    public void testKafkaRebalanceStaysReadyWhenComplete(VertxTestContext context) {
        KafkaRebalance kr = new KafkaRebalanceBuilder(createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false))
                .withNewStatus()
                    .withObservedGeneration(1L)
                    .withConditions(new ConditionBuilder()
                            .withType("Ready")
                            .withStatus("True")
                            .build())
                .endStatus()
                .build();

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).updateStatus();

        crdCreateKafka();
        crdCreateCruiseControlSecrets();
        Checkpoint checkpoint = context.checkpoint();

        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertState(context, client, namespace, kr.getMetadata().getName(), KafkaRebalanceState.Ready);
                })))
                .compose(v -> {

                    Kafka kafkaPatch = new KafkaBuilder(Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get())
                            .withNewStatus()
                                .withObservedGeneration(1L)
                                .withConditions(new ConditionBuilder()
                                        .withType("NotReady")
                                        .withStatus("True")
                                        .build())
                            .endStatus()
                            .build();

                    Crds.kafkaOperation(client).inNamespace(namespace).resource(kafkaPatch).updateStatus();

                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to Rebalancing on approval
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.Ready);
                    checkpoint.flag();
                }));
    }

    /**
     * When the Kafka cluster does not match the selector labels in the cluster operator configuration, the
     * KafkaRebalance resource should be ignored and not reconciled.
     */
    @Test
    public void testKafkaClusterNotMatchingLabelSelector(VertxTestContext context) {
        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();

        ClusterOperatorConfig config = new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                .with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "120000")
                .with(ClusterOperatorConfig.CUSTOM_RESOURCE_SELECTOR.key(), Map.of("selectorLabel", "value").entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(","))).build();

        krao = new KafkaRebalanceAssemblyOperator(Vertx.vertx(), supplier, config);

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaRebalance currentKr = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(kr.getMetadata().getName()).get();
                    // The labels of the Kafka resource do not match the => the KafkaRebalance is skipped and status not updated
                    assertThat(currentKr.getStatus(), is(nullValue()));
                    checkpoint.flag();
                })));
    }

    @Test
    public void testRebalanceUsesUnknownProperty(VertxTestContext context) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        ccServer.setupCCRebalanceResponse(0, CruiseControlEndpoints.REBALANCE);

        String rebalanceString = "apiVersion: kafka.strimzi.io/v1beta2\n" +
                "kind: KafkaRebalance\n" +
                "metadata:\n" +
                "  name: " + RESOURCE_NAME + "\n" +
                "  namespace: " + namespace + "\n" +
                "  labels:\n" +
                "    strimzi.io/cluster: " + CLUSTER_NAME + "\n" +
                "spec:\n" +
                "  unknown: \"value\"";

        KafkaRebalance kr = TestUtils.fromYamlString(rebalanceString, KafkaRebalance.class);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
                .onComplete(context.succeeding(v -> {
                    // the resource moved from 'New' directly to 'ProposalReady' (no pending calls in the Mock server)
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    assertValidationCondition(context, client, namespace, RESOURCE_NAME, "UnknownFields");
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'NotReady' due to missing Cruise Control deployment
     *
     * 1. A new KafkaRebalance resource is created; it is in the New state
     * 2. The operator checks that the Kafka cluster specified in the KafkaRebalance resource (via label) doesn't have Cruise Control configured
     * 4. The KafkaRebalance resource moves to NotReady state
     */

    @Test
    public void testCruiseControlDisabled(VertxTestContext context) {
        // build a Kafka cluster without the cruiseControl definition
        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(namespace, CLUSTER_NAME, REPLICAS, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT))
                .withNewStatus()
                    .withObservedGeneration(1L)
                    .withConditions(new ConditionBuilder()
                            .withType("Ready")
                            .withStatus("True")
                            .build())
                .endStatus()
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(kafka).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(kafka).updateStatus();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    assertState(context, client, namespace, RESOURCE_NAME,
                            KafkaRebalanceState.NotReady, InvalidResourceException.class,
                            "Kafka resource lacks 'cruiseControl' declaration");
                    checkpoint.flag();
                })));
    }

    @Test
    public void testCruiseControlDisabledToEnabledBehaviour(VertxTestContext context) {
        // build a Kafka cluster without the cruiseControl definition
        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(namespace, CLUSTER_NAME, REPLICAS, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT))
                .withNewStatus()
                    .withObservedGeneration(1L)
                    .withConditions(new ConditionBuilder()
                            .withType("Ready")
                            .withStatus("True")
                            .build())
                .endStatus()
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(kafka).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(kafka).updateStatus();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // the resource moved from New to NotReady due to the error
                assertState(context, client, namespace, RESOURCE_NAME,
                        KafkaRebalanceState.NotReady, InvalidResourceException.class,
                        "Kafka resource lacks 'cruiseControl' declaration");
            })))
                .compose(v -> {
                    try {
                        // Setup the rebalance endpoint with the number of pending calls before a response is received.
                        ccServer.setupCCRebalanceResponse(0, CruiseControlEndpoints.REBALANCE);
                    } catch (IOException | URISyntaxException e) {
                        context.failNow(e);
                    }

                    Kafka kafkaPatch = new KafkaBuilder(Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get())
                            .editSpec()
                                .withNewCruiseControl()
                                .endCruiseControl()
                            .endSpec()
                            .build();

                    Crds.kafkaOperation(client).inNamespace(namespace).resource(kafkaPatch).update();
                    crdCreateCruiseControlSecrets();

                    // trigger another reconcile to process the NotReady state
                    return krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME));
                })
                .onComplete(context.succeeding(v -> {
                    // the resource transitioned from 'NotReady' to 'ProposalReady'
                    assertState(context, client, namespace, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to 'NotReady' due to missing Kafka cluster label in the resource
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator checks that the resource is missing the label to specify the Kafka cluster
     * 4. The KafkaRebalance resource moves to 'NotReady' state
     */
    @Test
    public void testNoKafkaClusterInKafkaRebalanceLabel(VertxTestContext context) {
        KafkaRebalance kr = createKafkaRebalance(namespace, null, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // the resource moved from New to NotReady due to the error
                assertState(context, client, namespace, RESOURCE_NAME,
                        KafkaRebalanceState.NotReady, InvalidResourceException.class,
                        "Resource lacks label '" + Labels.STRIMZI_CLUSTER_LABEL + "': No cluster related to a possible rebalance.");
                checkpoint.flag();
            })));
    }

    /**
     * Test the Cruise Control API REST client timing out
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The operator doesn't get a response on time; the resource moves to NotReady
     */
    @Test
    public void testCruiseControlTimingOut(VertxTestContext context) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received
        // and with a delay on response higher than the client timeout to test timing out
        ccServer.setupCCRebalanceResponse(0, 10, CruiseControlEndpoints.REBALANCE);

        KafkaRebalance kr =
                createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();

        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
            .onComplete(context.succeeding(v -> {
                // the resource moved from New to NotReady (mocked Cruise Control didn't reply on time)
                assertState(context, client, namespace, RESOURCE_NAME,
                        KafkaRebalanceState.NotReady, NoStackTraceTimeoutException.class,
                        "The timeout period of 1000ms has been exceeded while executing POST");
                checkpoint.flag();
            }));
    }

    /**
     * Test the Cruise Control server not reachable
     *
     * 1. A new KafkaRebalance resource is created; it is in the 'New' state
     * 2. The operator requests a rebalance proposal through the Cruise Control REST API
     * 3. The operator doesn't get a response on time; the resource moves to 'NotReady'
     */
    @Test
    public void testCruiseControlNotReachable(VertxTestContext context) {
        // stop the mocked Cruise Control server to make it unreachable
        ccServer.stop();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
            .onComplete(context.succeeding(v -> {
                try {
                    ccServer = new MockCruiseControl(CruiseControl.REST_API_PORT);
                } catch (Throwable t) {
                    context.failNow(t);
                }
                // the resource moved from 'New' to 'NotReady' (mocked Cruise Control not reachable)
                assertState(context, client, namespace, RESOURCE_NAME,
                        KafkaRebalanceState.NotReady, CruiseControlRetriableConnectionException.class,
                        "Connection refused");
                checkpoint.flag();
            }));
    }

    /**
     * Tests an intra-broker disk balance on a Kafka deployment without a JBOD configuration.
     *
     * 1. A new KafkaRebalance resource is created with rebalanceDisk=true for a Kafka cluster without a JBOD configuration.
     * 2. The operator sets an "InvalidResourceException" error instead of requesting a proposal since
     *    intra-broker balancing only applies to Kafka deployments that use JBOD storage with multiple disks.
     * 4. The KafkaRebalance resource moves to the 'NotReady' state
     */
    @Test
    public void testIntraBrokerDiskBalanceWithoutJbodConfig(VertxTestContext context) {
        KafkaRebalanceSpec kafkaRebalanceSpec = new KafkaRebalanceSpecBuilder()
                .withRebalanceDisk(true)
                .build();

        KafkaRebalance kr = createKafkaRebalance(namespace, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kr).create();
        crdCreateKafka();
        crdCreateCruiseControlSecrets();

        Checkpoint checkpoint = context.checkpoint();
        krao.reconcile(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, namespace, RESOURCE_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(RESOURCE_NAME).get();
                    assertThat(kr1, StateMatchers.hasState());
                    Condition condition = krao.rebalanceStateCondition(kr1.getStatus());
                    assertThat(condition.getReason(), is("InvalidResourceException"));
                    assertThat(condition.getMessage(), is("Cannot set rebalanceDisk=true for Kafka clusters with a non-JBOD storage config. " +
                            "Intra-broker balancing only applies to Kafka deployments that use JBOD storage with multiple disks."));
                    checkpoint.flag();
                })));
    }

    /**
     * annotate the KafkaRebalance, patch the (mocked) server with the resource and then return the annotated resource
     */
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
                .edit(kr -> patchedKr);

        return patchedKr;
    }

    private void assertState(VertxTestContext context, KubernetesClient kubernetesClient, String namespace, String resource, KafkaRebalanceState state) {
        context.verify(() -> {
            KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(namespace).withName(resource).get();
            assertThat(kafkaRebalance, StateMatchers.hasState());
            Condition condition = krao.rebalanceStateCondition(kafkaRebalance.getStatus());
            assertThat(Collections.singletonList(condition), StateMatchers.hasStateInConditions(state));
        });
    }

    private void assertValidationCondition(VertxTestContext context, KubernetesClient kubernetesClient, String namespace, String resource, String validationError) {
        context.verify(() -> {
            KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(namespace).withName(resource).get();
            assertThat(kafkaRebalance, StateMatchers.hasState());
            assertThat(kafkaRebalance.getStatus().getConditions().stream().filter(cond -> validationError.equals(cond.getReason())).findFirst(), notNullValue());
        });
    }

    private void assertState(VertxTestContext context, KubernetesClient kubernetesClient, String namespace, String resource, KafkaRebalanceState state, Class<?> reason, String message) {
        context.verify(() -> {
            KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(namespace).withName(resource).get();

            assertThat(kafkaRebalance, StateMatchers.hasState());
            Condition condition = krao.rebalanceStateCondition(kafkaRebalance.getStatus());
            assertThat(condition, StateMatchers.hasStateInCondition(state, reason, message));
        });
    }

    private KafkaRebalance createKafkaRebalance(String namespace, String clusterName, String resourceName,
                                                     KafkaRebalanceSpec kafkaRebalanceSpec, boolean isAutoApproval) {
        return new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(resourceName)
                    .withLabels(clusterName != null ? Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME) : null)
                    .withAnnotations(isAutoApproval ? Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true") : null)
                .endMetadata()
                .withSpec(kafkaRebalanceSpec)
                .build();
    }

    private static class StateMatchers extends AbstractResourceStateMatchers {

    }
}
