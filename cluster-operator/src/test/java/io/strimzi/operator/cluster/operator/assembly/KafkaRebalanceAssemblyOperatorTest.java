/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.KafkaRebalanceSpecBuilder;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlRestException;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.MockCruiseControl;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlRetriableConnectionException;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.NoStackTraceTimeoutException;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube2.MockKube2;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
import java.util.Optional;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class KafkaRebalanceAssemblyOperatorTest {
    private static final String HOST = "localhost";
    private static final String RESOURCE_NAME = "my-rebalance";
    private static final String CLUSTER_NAMESPACE = "cruise-control-namespace";
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

    private static ClientAndServer ccServer;
    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;

    private CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> mockRebalanceOps;
    private CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps;
    private SecretOperator mockSecretOps;
    private KafkaRebalanceAssemblyOperator kcrao;
    private ConfigMapOperator mockCmOps;

    private final int replicas = 1;
    private final String image = "my-kafka-image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;

    private final String version = KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION;
    private final String ccImage = "my-cruise-control-image";

    private final Kafka kafka =
            new KafkaBuilder(ResourceUtils.createKafka(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                        .editKafka()
                            .withVersion(version)
                        .endKafka()
                        .editOrNewCruiseControl()
                            .withImage(ccImage)
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

    @BeforeAll
    public static void beforeAll() throws IOException {
        ccServer = MockCruiseControl.server(CruiseControl.REST_API_PORT);
    }

    @AfterAll
    public static void afterAll() {
        ccServer.stop();
    }

    @BeforeEach
    public void beforeEach(Vertx vertx) {
        ccServer.reset();

        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaRebalanceCrd()
                .build();
        mockKube.start();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Override to inject mocked cruise control address so real cruise control not required
        kcrao = new KafkaRebalanceAssemblyOperator(vertx, supplier, ResourceUtils.dummyClusterOperatorConfig()) {
            @Override
            public String cruiseControlHost(String clusterName, String clusterNamespace) {
                return HOST;
            }

            @Override
            public CruiseControlApi cruiseControlClientProvider(Secret ccSecret, Secret ccApiSecret, boolean apiAuthEnabled, boolean apiSslEnabled) {
                return new CruiseControlApiImpl(vertx, 1, ccSecret, ccApiSecret, true, true);
            }
        };

        mockRebalanceOps = supplier.kafkaRebalanceOperator;
        mockKafkaOps = supplier.kafkaOperator;
        mockCmOps = supplier.configMapOperations;
        mockSecretOps = supplier.secretOperations;
    }

    @AfterEach
    public void afterEach() {
        mockKube.stop();
    }

    private void mockSecretResources() {
        when(mockSecretOps.getAsync(CLUSTER_NAMESPACE, CruiseControlResources.secretName(CLUSTER_NAME)))
                .thenReturn(Future.succeededFuture(MockCruiseControl.CC_SECRET));
        when(mockSecretOps.getAsync(CLUSTER_NAMESPACE, CruiseControlResources.apiSecretName(CLUSTER_NAME)))
                .thenReturn(Future.succeededFuture(MockCruiseControl.CC_API_SECRET));
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReady(context, 0, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReady(context, 0, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReady(context, 0, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReady(VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME))
                .thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                .onComplete(context.succeeding(v -> {
                    // the resource moved from 'New' directly to 'ProposalReady' (no pending calls in the Mock server)
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToProposalReady(context, 2, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToProposalReadyAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToProposalReady(context, 2, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToProposalReadyRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToProposalReadyRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToProposalReady(context, 2, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToPendingProposalToProposalReady(VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME))
                .thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                .onComplete(context.succeeding(v -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal);
                }))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();

                    return kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr1);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from PendingProposal to ProposalReady
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
                    context.verify(() -> {
                        KafkaRebalanceStatus rebalanceStatus = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get().getStatus();
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStopped(context, 5, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToStoppedRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToStoppedAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStopped(context, 5, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToStoppedRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToStoppedRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStopped(context, 5, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToPendingProposalToStopped(VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME))
                .thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client, new Runnable() {
            int count = 0;

            @Override
            public void run() {
                if (++count == 4) {
                    // after a while, apply the "stop" annotation to the resource in the PendingProposal state
                    annotate(client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceAnnotation.stop);
                }
            }
        });

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                .onComplete(context.succeeding(v -> {
                    // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal);
                }))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();

                    return kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr1);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalPending to Stopped
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.Stopped);
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStoppedAndRefresh(context, 2, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToStoppedAndRefreshRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToStoppedAndRefreshAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStoppedAndRefresh(context, 2, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalToStoppedAndRefreshRebalance} for description
     */
    @Test
    public void testNewToPendingProposalToStoppedAndRefreshRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalToStoppedAndRefresh(context, 2, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToPendingProposalToStoppedAndRefresh(VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client, new Runnable() {
            int count = 0;

            @Override
            public void run() {
                if (++count == 4) {
                    // after a while, apply the "stop" annotation to the resource in the PendingProposal state
                    annotate(client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceAnnotation.stop);
                }
            }
        });

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                kr)
                // the resource moved from New to PendingProposal (due to the configured Mock server pending calls)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal)))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            kr1);
                })
                // the resource moved from ProposalPending to Stopped
                .onComplete(context.succeeding(v ->
                        assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.Stopped)))
                .compose(v -> {
                    // apply the "refresh" annotation to the resource in the Stopped state
                    KafkaRebalance refreshedKr = annotate(client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceAnnotation.refresh);

                    // trigger another reconcile to process the Stopped state
                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            refreshedKr);
                })
                // the resource moved from Stopped to PendingProposal
                .onComplete(context.succeeding(v ->
                        assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal)))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance kr6 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            kr6);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from PendingProposal to ProposalReady
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReady(context, 0, 0, 0, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReady(context, 0, 0, 0, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToReadyRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReady(context, 0, 0, 0, CruiseControlEndpoints.REMOVE_BROKER, kr);
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, true);
        this.krNewToProposalReadyToRebalancingToReady(context, 0, 0, 0, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testAutoApprovalNewToProposalReadyToRebalancingToReadyRebalance} for description
     */
    @Test
    public void testAutoApprovalNewToProposalReadyToRebalancingToReadyAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, true);
        this.krNewToProposalReadyToRebalancingToReady(context, 0, 0, 0, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testAutoApprovalNewToProposalReadyToRebalancingToReadyRebalance} for description
     */
    @Test
    public void testAutoApprovalNewToProposalReadyToRebalancingToReadyRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, true);
        this.krNewToProposalReadyToRebalancingToReady(context, 0, 0, 0, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReadyToRebalancingToReady(VertxTestContext context, int pendingCalls, int activeCalls, int inExecutionCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, activeCalls, inExecutionCalls);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                // the resource moved from 'New' to 'ProposalReady' directly (no pending calls in the Mock server)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady)))
                .compose(v -> {

                    // if "rebalance-auto-approval" annotation is set, no need for applying the "approve" annotation
                    // otherwise apply the "approve" annotation to the resource in the ProposalReady state
                    KafkaRebalance approvedKr = Annotations.booleanAnnotation(kr, Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, false) ?
                            kr :
                            annotate(client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceAnnotation.approve);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            approvedKr);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to Rebalancing on approval
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.Rebalancing);
                }))
                .compose(v -> {
                    // trigger another reconcile to process the Rebalancing state
                    KafkaRebalance kr4 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            kr4);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from Rebalancing to Ready
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.Ready);
                    checkpoint.flag();
                }));
    }

    /**
     * Tests the transition from 'New' to to 'ProposalReady' to `ReconciliationPaused`
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToReconciliationPaused(context, 0, 0, 0, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToReconciliationPausedRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToReconciliationPausedAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToReconciliationPaused(context, 0, 0, 0, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToReconciliationPausedRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToReconciliationPausedRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToReconciliationPaused(context, 0, 0, 0, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReadyToReconciliationPaused(VertxTestContext context, int pendingCalls, int activeCalls, int inExecutionCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, activeCalls, inExecutionCalls);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                // the resource moved from 'New' to 'ProposalReady' directly (no pending calls in the Mock server)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady)))
                .compose(v -> {
                    // set the `ReconciliationPaused` annotation as true

                    KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();

                    KafkaRebalance patchedKr = new KafkaRebalanceBuilder(kr2)
                            .editMetadata()
                            .addToAnnotations(Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"))
                            .endMetadata()
                            .build();

                    Crds.kafkaRebalanceOperation(client)
                            .inNamespace(CLUSTER_NAMESPACE)
                            .withName(kr.getMetadata().getName())
                            .edit(kr3 -> patchedKr);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            kr2);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to ReconciliationPaused on pausing
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ReconciliationPaused);
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReadyThenRefresh(context, 0, 0, 0, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToReadyThenRefreshRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyThenRefreshAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReadyThenRefresh(context, 0, 0, 0, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToReadyThenRefreshRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToReadyThenRefreshRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToProposalReadyToRebalancingToReadyThenRefresh(context, 0, 0, 0, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReadyToRebalancingToReadyThenRefresh(VertxTestContext context, int pendingCalls, int activeCalls, int inExecutionCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, activeCalls, inExecutionCalls);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                // the resource moved from 'New' to 'ProposalReady' directly (no pending calls in the Mock server)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady)))
                .compose(v -> {
                    // apply the "approve" annotation to the resource in the ProposalReady state
                    KafkaRebalance approvedKr = annotate(client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceAnnotation.approve);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            approvedKr);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from ProposalReady to Rebalancing on approval
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.Rebalancing);
                }))
                .compose(v -> {
                    // trigger another reconcile to process the Rebalancing state
                    KafkaRebalance kr4 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            kr4);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from Rebalancing to Ready
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.Ready);
                }))
                .compose(v -> {
                    // apply the "refresh" annotation to the resource in the ProposalReady state
                    KafkaRebalance refreshKr = annotate(client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceAnnotation.refresh);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            refreshKr);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from Ready to ProposalReady
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
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

        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
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

        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
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

        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoals(context, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewWithMissingHardGoals(VertxTestContext context, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint to get error about hard goals
        MockCruiseControl.setupCCRebalanceBadGoalsError(ccServer, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();
                    assertThat(kr1, StateMatchers.hasState());
                    Condition condition = kcrao.rebalanceStateCondition(kr1.getStatus());
                    assertThat(condition, StateMatchers.hasStateInCondition(KafkaRebalanceState.NotReady, CruiseControlRestException.class,
                            "Error processing POST request '/rebalance' due to: " +
                                    "'java.lang.IllegalArgumentException: Missing hard goals [NetworkInboundCapacityGoal, DiskCapacityGoal, RackAwareGoal, NetworkOutboundCapacityGoal, CpuCapacityGoal, ReplicaCapacityGoal] " +
                                    "in the provided goals: [RackAwareGoal, ReplicaCapacityGoal]. " +
                                    "Add skip_hard_goal_check=true parameter to ignore this sanity check.'."));
                    checkpoint.flag();
                })));
    }

    @Test
    public void testUnknownPropertyInSpec(VertxTestContext context) throws IOException, URISyntaxException {
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 2, CruiseControlEndpoints.REBALANCE);

        String yaml = "apiVersion: kafka.strimzi.io/v1beta2\n" +
                "kind: KafkaRebalance\n" +
                "metadata:\n" +
                "  name: " + RESOURCE_NAME + "\n" +
                "  namespace: " + CLUSTER_NAMESPACE + "\n" +
                "  labels:\n" +
                "    strimzi.io/cluster: " + CLUSTER_NAME + "\n" +
                "spec:\n" +
                "  unknown: \"property\"\n" +
                "  goals:\n" +
                "    - CpuCapacityGoal\n" +
                "    - NetworkInboundCapacityGoal\n" +
                "    - DiskCapacityGoal\n" +
                "    - RackAwareGoal\n" +
                "    - NetworkOutboundCapacityGoal\n" +
                "    - ReplicaCapacityGoal\n";
        KafkaRebalance kr = TestUtils.fromYamlString(yaml, KafkaRebalance.class);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME), kr)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertThat(kr1, StateMatchers.hasState());
                    Optional<Condition> condition = kr1.getStatus().getConditions().stream().filter(cond -> "UnknownFields".equals(cond.getReason())).findFirst();
                    assertTrue(condition.isPresent());
                    assertThat(condition.get().getStatus(), is("True"));
                    assertThat(condition.get().getMessage(), is("Contains object at path spec with an unknown property: unknown"));
                    assertThat(condition.get().getType(), is("Warning"));
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

        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewToProposalReadySkipHardGoals(context, 0, CruiseControlEndpoints.REBALANCE, kr);
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

        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewToProposalReadySkipHardGoals(context, 0, CruiseControlEndpoints.ADD_BROKER, kr);
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

        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewToProposalReadySkipHardGoals(context, 0, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToProposalReadySkipHardGoals(VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                .onComplete(context.succeeding(v -> {
                    // the resource moved from New directly to ProposalReady (no pending calls in the Mock server)
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
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

        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoalsAndRefresh(context, 0, CruiseControlEndpoints.REBALANCE, kr);
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

        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoalsAndRefresh(context, 0, CruiseControlEndpoints.ADD_BROKER, kr);
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

        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);
        this.krNewWithMissingHardGoalsAndRefresh(context, 0, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewWithMissingHardGoalsAndRefresh(VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint to get error about hard goals
        MockCruiseControl.setupCCRebalanceBadGoalsError(ccServer, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();
                    assertThat(kr1, StateMatchers.hasState());
                    Condition condition = kcrao.rebalanceStateCondition(kr1.getStatus());
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
                        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);
                    } catch (IOException | URISyntaxException e) {
                        context.failNow(e);
                    }

                    // set the skip hard goals check flag
                    Crds.kafkaRebalanceOperation(client)
                            .inNamespace(CLUSTER_NAMESPACE)
                            .withName(kr.getMetadata().getName())
                            .edit(kr2 -> new KafkaRebalanceBuilder(kr2)
                                    .editSpec()
                                        .withSkipHardGoalCheck(true)
                                    .endSpec()
                                    .build());

                    // apply the "refresh" annotation to the resource in the NotReady state
                    KafkaRebalance annotatedPatchedKr = annotate(client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceAnnotation.refresh);

                    // trigger another reconcile to process the NotReady state
                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            annotatedPatchedKr);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource transitioned from 'NotReady' to 'ProposalReady'
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady);
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalDelete(context, 2, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalDeleteRebalance} for description
     */
    @Test
    public void testNewToPendingProposalDeleteAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalDelete(context, 2, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToPendingProposalDeleteRebalance} for description
     */
    @Test
    public void testNewToPendingProposalDeleteRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalDelete(context, 2, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToPendingProposalDelete(VertxTestContext context, int pendingCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client, new Runnable() {
            int count = 0;

            @Override
            public void run() {
                if (++count == 4) {
                    // delete the KafkaRebalance resource while on PendingProposal
                    Crds.kafkaRebalanceOperation(client)
                            .inNamespace(CLUSTER_NAMESPACE)
                            .withName(kr.getMetadata().getName())
                            .delete();
                }
            }
        });

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()), kr)
                // the resource moved from 'New' to 'PendingProposal' (due to the configured Mock server pending calls)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.PendingProposal)))
                .compose(v -> {
                    // trigger another reconcile to process the PendingProposal state
                    KafkaRebalance currentKR = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            currentKR);
                })
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource should not exist anymore
                    KafkaRebalance currentKR = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();
                    assertThat(currentKR, is(nullValue()));
                    checkpoint.flag();
                })));
    }

    /**
     * Tests the transition from 'New' to to 'ProposalReady'
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
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalDelete(context, 0, 0, 2, CruiseControlEndpoints.REBALANCE, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToStoppedRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToStoppedAddBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, ADD_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalDelete(context, 0, 0, 2, CruiseControlEndpoints.ADD_BROKER, kr);
    }

    /**
     * See the {@link KafkaRebalanceAssemblyOperatorTest#testNewToProposalReadyToRebalancingToStoppedRebalance} for description
     */
    @Test
    public void testNewToProposalReadyToRebalancingToStoppedRemoveBroker(VertxTestContext context) throws IOException, URISyntaxException {
        KafkaRebalance kr = createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, REMOVE_BROKER_KAFKA_REBALANCE_SPEC, false);
        this.krNewToPendingProposalDelete(context, 0, 0, 2, CruiseControlEndpoints.REMOVE_BROKER, kr);
    }

    private void krNewToPendingProposalDelete(VertxTestContext context, int pendingCalls, int activeCalls, int inExecutionCalls, CruiseControlEndpoints endpoint, KafkaRebalance kr) throws IOException, URISyntaxException {
        // Setup the rebalance and user tasks endpoints with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, pendingCalls, endpoint);
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, activeCalls, inExecutionCalls);
        MockCruiseControl.setupCCStopResponse(ccServer);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, kr.getMetadata().getName(), client, new Runnable() {
            int count = 0;

            @Override
            public void run() {
                if (++count == 6) {
                    // after a while, apply the "stop" annotation to the resource in the Rebalancing state
                    annotate(client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceAnnotation.stop);
                }
            }
        });

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                kr)
                // the resource moved from New to ProposalReady directly (no pending calls in the Mock server)
                .onComplete(context.succeeding(v ->
                        assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.ProposalReady)))
                .compose(v -> {
                    // apply the "approve" annotation to the resource in the ProposalReady state
                    KafkaRebalance approvedKr = annotate(client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceAnnotation.approve);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            approvedKr);
                })
                // the resource moved from ProposalReady to Rebalancing on approval
                .onComplete(context.succeeding(v ->
                        assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.Rebalancing)))
                .compose(v -> {
                    // trigger another reconcile to process the Rebalancing state
                    KafkaRebalance kr5 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(kr.getMetadata().getName()).get();

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, kr.getMetadata().getName()),
                            kr5);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource moved from Rebalancing to Stopped
                    assertState(context, client, CLUSTER_NAMESPACE, kr.getMetadata().getName(), KafkaRebalanceState.Stopped);
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

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(null));
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    assertState(context, client, CLUSTER_NAMESPACE, RESOURCE_NAME,
                            KafkaRebalanceState.NotReady, NoSuchResourceException.class,
                            "Kafka resource '" + CLUSTER_NAME + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + CLUSTER_NAMESPACE  + ".");
                    checkpoint.flag();
                })));
    }

    /**
     * When the Kafka cluster does not match the selector labels in the cluster operator configuration, the
     * KafkaRebalance resource should be ignored and not reconciled.
     */
    @Test
    public void testKafkaClusterNotMatchingLabelSelector(VertxTestContext context) {
        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        mockKafkaOps = supplier.kafkaOperator;
        mockRebalanceOps = supplier.kafkaRebalanceOperator;
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));

        ClusterOperatorConfig config = new ClusterOperatorConfig(
                singleton(CLUSTER_NAMESPACE),
                60_000,
                120_000,
                300_000,
                false,
                true,
                KafkaVersionTestUtils.getKafkaVersionLookup(),
                null,
                null,
                null,
                null,
                Labels.fromMap(Map.of("selectorLabel", "value")),
                "",
                10,
                10_000,
                30,
                false,
                1024,
                "cluster-operator-name",
                ClusterOperatorConfig.DEFAULT_POD_SECURITY_PROVIDER_CLASS, null);

        kcrao = new KafkaRebalanceAssemblyOperator(Vertx.vertx(), supplier, config);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).onComplete(context.succeeding(v -> context.verify(() -> {
                    // The labels of the Kafka resource do not match the => the KafkaRebalance should not be reconciled and the
                    // rebalance ops should have no interactions.
                    verifyNoInteractions(mockRebalanceOps);
                    checkpoint.flag();
                })));
    }

    @Test
    public void testRebalanceUsesUnknownProperty(VertxTestContext context) throws IOException, URISyntaxException {
        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0, CruiseControlEndpoints.REBALANCE);

        String rebalanceString = "apiVersion: kafka.strimzi.io/v1beta2\n" +
                "kind: KafkaRebalance\n" +
                "metadata:\n" +
                "  name: " + RESOURCE_NAME + "\n" +
                "  namespace: " + CLUSTER_NAMESPACE + "\n" +
                "  labels:\n" +
                "    strimzi.io/cluster: " + CLUSTER_NAME + "\n" +
                "spec:\n" +
                "  unknown: \"value\"";

        KafkaRebalance kr = TestUtils.fromYamlString(rebalanceString, KafkaRebalance.class);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME))
                .thenReturn(Future.succeededFuture(kafka));

        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME), kr)
                .onComplete(context.succeeding(v -> {
                    // the resource moved from 'New' directly to 'ProposalReady' (no pending calls in the Mock server)
                    assertState(context, client, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
                    assertValidationCondition(context, client, CLUSTER_NAMESPACE, RESOURCE_NAME, "UnknownFields");
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
        Kafka kafka =
                new KafkaBuilder(ResourceUtils.createKafka(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .editKafka()
                                .withVersion(version)
                            .endKafka()
                        .endSpec()
                        .withNewStatus()
                            .withObservedGeneration(1L)
                            .withConditions(new ConditionBuilder()
                                    .withType("Ready")
                                    .withStatus("True")
                                    .build())
                        .endStatus()
                        .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        // the Kafka cluster doesn't have the Cruise Control deployment
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME), kr)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    assertState(context, client, CLUSTER_NAMESPACE, RESOURCE_NAME,
                            KafkaRebalanceState.NotReady, InvalidResourceException.class,
                            "Kafka resource lacks 'cruiseControl' declaration");
                    checkpoint.flag();
                })));
    }

    @Test
    public void testCruiseControlDisabledToEnabledBehaviour(VertxTestContext context) {

        // build a Kafka cluster without the cruiseControl definition
        Kafka kafka =
                new KafkaBuilder(ResourceUtils.createKafka(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .editKafka()
                                .withVersion(version)
                            .endKafka()
                        .endSpec()
                        .withNewStatus()
                            .withObservedGeneration(1L)
                            .withConditions(new ConditionBuilder()
                                    .withType("Ready")
                                    .withStatus("True")
                                    .build())
                        .endStatus()
                        .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        // the Kafka cluster doesn't have the Cruise Control deployment
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME), kr)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // the resource moved from New to NotReady due to the error
                assertState(context, client, CLUSTER_NAMESPACE, RESOURCE_NAME,
                        KafkaRebalanceState.NotReady, InvalidResourceException.class,
                        "Kafka resource lacks 'cruiseControl' declaration");
            })))
                .compose(v -> {
                    try {
                        // Setup the rebalance endpoint with the number of pending calls before a response is received.
                        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0, CruiseControlEndpoints.REBALANCE);
                    } catch (IOException | URISyntaxException e) {
                        context.failNow(e);
                    }

                    Kafka kafkaPatch = new KafkaBuilder(ResourceUtils.createKafka(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
                            .editSpec()
                                .editKafka()
                                     .withVersion(version)
                                .endKafka()
                                .editOrNewCruiseControl()
                                    .withImage(ccImage)
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

                    when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafkaPatch));
                    mockSecretResources();
                    mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

                    // trigger another reconcile to process the NotReady state
                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr);
                })
                .onComplete(context.succeeding(v -> {
                    // the resource transitioned from 'NotReady' to 'ProposalReady'
                    assertState(context, client, CLUSTER_NAMESPACE, RESOURCE_NAME, KafkaRebalanceState.ProposalReady);
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

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, null, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME), kr)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // the resource moved from New to NotReady due to the error
                assertState(context, client, CLUSTER_NAMESPACE, RESOURCE_NAME,
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
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0, 10, CruiseControlEndpoints.REBALANCE);

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME), kr)
            .onComplete(context.succeeding(v -> {
                // the resource moved from New to NotReady (mocked Cruise Control didn't reply on time)
                assertState(context, client, CLUSTER_NAMESPACE, RESOURCE_NAME,
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

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, EMPTY_KAFKA_REBALANCE_SPEC, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME), kr)
            .onComplete(context.succeeding(v -> {
                try {
                    ccServer = MockCruiseControl.server(CruiseControl.REST_API_PORT);
                } catch (IOException e) {
                    context.failNow(e);
                }
                // the resource moved from 'New' to 'NotReady' (mocked Cruise Control not reachable)
                assertState(context, client, CLUSTER_NAMESPACE, RESOURCE_NAME,
                        KafkaRebalanceState.NotReady, CruiseControlRetriableConnectionException.class,
                        "Connection refused");
                checkpoint.flag();
            }));
    }

    /**
     * Tests an intra-broker disk balance on a Kakfa deployment without a JBOD configuration.
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

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, kafkaRebalanceSpec, false);

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).resource(kr).create();

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        mockSecretResources();
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, CLUSTER_NAMESPACE, RESOURCE_NAME, client);

        Checkpoint checkpoint = context.checkpoint();
        kcrao.reconcileRebalance(new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME), kr)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // the resource moved from New to NotReady due to the error
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertThat(kr1, StateMatchers.hasState());
                    Condition condition = kcrao.rebalanceStateCondition(kr1.getStatus());
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
            Condition condition = kcrao.rebalanceStateCondition(kafkaRebalance.getStatus());
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
            Condition condition = kcrao.rebalanceStateCondition(kafkaRebalance.getStatus());
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

    private void mockRebalanceOperator(CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> mockRebalanceOps, ConfigMapOperator mockCmOps,
                                       String namespace, String resource, KubernetesClient client) {
        mockRebalanceOperator(mockRebalanceOps, mockCmOps, namespace, resource, client, null);
    }

    private void mockRebalanceOperator(CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> mockRebalanceOps,
                                       ConfigMapOperator mockCmOps, String namespace, String resource, KubernetesClient client, Runnable getAsyncFunction) {


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
        when(mockRebalanceOps.updateStatusAsync(any(), any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .resource(invocation.getArgument(1))
                        .replaceStatus());
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });
        when(mockCmOps.reconcile(any(), eq(CLUSTER_NAMESPACE), eq(RESOURCE_NAME), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockRebalanceOps.patchAsync(any(), any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(namespace)
                        .withName(resource)
                        .edit(kr -> (KafkaRebalance) invocation.getArgument(1)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });
    }

    private static class StateMatchers extends AbstractResourceStateMatchers {

    }
}
