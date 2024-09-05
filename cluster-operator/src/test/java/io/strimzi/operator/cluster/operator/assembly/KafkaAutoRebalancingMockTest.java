/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceConfigurationBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceModeBrokers;
import io.strimzi.api.kafka.model.rebalance.KafkaAutoRebalanceState;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.strimzi.api.ResourceAnnotations.ANNO_STRIMZI_IO_REBALANCE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAutoRebalancingMockTest {
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private static final MockCertManager CERT_MANAGER = new MockCertManager();
    private static final PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(10, "a", "a");

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private static KubernetesClient client;
    private static MockKube3 mockKube;
    private String namespace;
    private ResourceOperatorSupplier supplier;
    private StrimziPodSetController podSetController;
    private KafkaAssemblyOperator operator;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaNodePoolCrd()
                .withKafkaConnectCrd()
                .withKafkaMirrorMaker2Crd()
                .withKafkaRebalanceCrd()
                .withStrimziPodSetCrd()
                .withPodController()
                .withDeploymentController()
                .withServiceController()
                .withDeletionController()
                .build();
        mockKube.start();
        client = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");

        Kafka cluster = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(namespace)
                    .withAnnotations(Map.of(
                            Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled",
                            Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"
                    ))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withConfig(new HashMap<>())
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                    .endKafka()
                    .withNewCruiseControl()
                        .withAutoRebalance(
                                new KafkaAutoRebalanceConfigurationBuilder()
                                        .withMode(KafkaRebalanceMode.ADD_BROKERS)
                                        .withNewTemplate("my-add-remove-brokers-rebalancing-template")
                                        .build(),
                                new KafkaAutoRebalanceConfigurationBuilder()
                                        .withMode(KafkaRebalanceMode.REMOVE_BROKERS)
                                        .withNewTemplate("my-add-remove-brokers-rebalancing-template")
                                        .build())
                    .endCruiseControl()
                .endSpec()
                .build();

        KafkaNodePool controllers = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("controllers")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("brokers")
                    .withNamespace(namespace)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(5)
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();

        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(controllers).create();
        Crds.kafkaNodePoolOperation(client).inNamespace(namespace).resource(brokers).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(cluster).create();

        supplier = new ResourceOperatorSupplier(vertx, client, null, ResourceUtils.adminClientProvider(), null,
                ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(), null, PFA, 2_000, mock(BrokersInUseCheck.class));

        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        operator = new KafkaAssemblyOperator(vertx, PFA, CERT_MANAGER, PASSWORD_GENERATOR, supplier, config);
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
        sharedWorkerExecutor.close();
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES) // TODO: to be removed after testing implementation
    public void testAutoRebalancingScaleDown(VertxTestContext context) {

        // getting the mocked BrokersInUseCheck class to mock broker scale down operation for check failure
        BrokersInUseCheck operations = supplier.brokersInUseCheck;
        when(operations.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(3, 4)));

        KafkaRebalance kafkaRebalanceTemplate = new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withName("my-add-remove-brokers-rebalancing-template")
                    .withAnnotations(Map.of(ANNO_STRIMZI_IO_REBALANCE, "template"))
                .endMetadata()
                .withNewSpec()
                    .withGoals("CpuCapacityGoal")
                .endSpec()
                .build();

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kafkaRebalanceTemplate).create();

        Checkpoint reconciliation = context.checkpoint();

        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling down the brokers
                    Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").edit(
                            p -> new KafkaNodePoolBuilder(p)
                                    .editSpec()
                                        .withReplicas(3)
                                    .endSpec()
                                    .build()
                    );
                })))
                // 2nd reconcile, getting the scaling down
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));
                    Optional<KafkaAutoRebalanceModeBrokers> removeModeBrokers = k.getStatus().getAutoRebalance().getModes().stream().filter(m -> m.getMode().equals(KafkaRebalanceMode.REMOVE_BROKERS)).findFirst();
                    assertThat(removeModeBrokers.isPresent(), is(true));
                    assertThat(removeModeBrokers.get().getBrokers().size(), is(2));
                    assertThat(removeModeBrokers.get().getBrokers().containsAll(List.of(3, 4)), is(true));

                    KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    assertThat(kafkaRebalance, is(notNullValue()));
                    assertThat(kafkaRebalance.getSpec().getMode(), is(KafkaRebalanceMode.REMOVE_BROKERS));
                    assertThat(kafkaRebalance.getSpec().getBrokers().size(), is(2));
                    assertThat(kafkaRebalance.getSpec().getBrokers().containsAll(List.of(3, 4)), is(true));
                    assertThat(kafkaRebalance.getSpec().getGoals().contains("CpuCapacityGoal"), is(true));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to New state
                    patchKafkaRebalanceState(kafkaRebalance, KafkaRebalanceState.New);
                })))
                // 3rd reconcile, handling auto-rebalancing with KafkaRebalance in New state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));

                    KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Rebalancing state
                    patchKafkaRebalanceState(kafkaRebalance, KafkaRebalanceState.Rebalancing);
                })))
                // 4th reconcile, handling auto-rebalancing with KafkaRebalance in Rebalancing state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));

                    KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to Ready state
                    patchKafkaRebalanceState(kafkaRebalance, KafkaRebalanceState.Ready);
                })))
                // 5th reconcile, handling auto-rebalancing with KafkaRebalance in Ready state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    assertThat(k.getStatus().getAutoRebalance().getModes(), is(nullValue()));

                    KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.REMOVE_BROKERS)).get();
                    assertThat(kafkaRebalance, is(nullValue()));

                    reconciliation.flag();
                })));
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES) // TODO: to be removed after testing implementation
    public void testAutoRebalancingScaleUp(VertxTestContext context) {

        KafkaRebalance kafkaRebalanceTemplate = new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withName("my-add-remove-brokers-rebalancing-template")
                    .withAnnotations(Map.of(ANNO_STRIMZI_IO_REBALANCE, "template"))
                .endMetadata()
                .withNewSpec()
                    .withGoals("CpuCapacityGoal")
                .endSpec()
                .build();

        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kafkaRebalanceTemplate).create();

        Checkpoint reconciliation = context.checkpoint();

        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));

                    // scaling up the brokers
                    Crds.kafkaNodePoolOperation(client).inNamespace(namespace).withName("brokers").edit(
                            p -> new KafkaNodePoolBuilder(p)
                                    .editSpec()
                                        .withReplicas(7)
                                    .endSpec()
                                    .build()
                    );
                })))
                // 2nd reconcile, getting the scaling up
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));
                    Optional<KafkaAutoRebalanceModeBrokers> addModeBrokers = k.getStatus().getAutoRebalance().getModes().stream().filter(m -> m.getMode().equals(KafkaRebalanceMode.ADD_BROKERS)).findFirst();
                    assertThat(addModeBrokers.isPresent(), is(true));
                    assertThat(addModeBrokers.get().getBrokers().size(), is(2));
                    assertThat(addModeBrokers.get().getBrokers().containsAll(List.of(8, 9)), is(true));

                    KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(client).inNamespace(namespace).withName(KafkaRebalanceUtils.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaRebalanceMode.ADD_BROKERS)).get();
                    assertThat(kafkaRebalance, is(notNullValue()));
                    assertThat(kafkaRebalance.getSpec().getMode(), is(KafkaRebalanceMode.ADD_BROKERS));
                    assertThat(kafkaRebalance.getSpec().getBrokers().size(), is(2));
                    assertThat(kafkaRebalance.getSpec().getBrokers().containsAll(List.of(8, 9)), is(true));
                    assertThat(kafkaRebalance.getSpec().getGoals().contains("CpuCapacityGoal"), is(true));

                    // simulate the auto-rebalancing KafkaRebalance custom resource got by the rebalance operator transitions to New state
                    patchKafkaRebalanceState(kafkaRebalance, KafkaRebalanceState.New);
                })))
                // 3rd reconcile, handling auto-rebalancing with KafkaRebalance in New state
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {

                    // TODO: TBD

                    reconciliation.flag();
                })));
    }

    @Test
    public void testNoAutoRebalancingIdleOnClusterCreation(VertxTestContext context) {
        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // just checking that on Kafka cluster creation with no scaling, the auto-rebalancing is just in Idle state
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    reconciliation.flag();
                })));
    }

    @Test
    public void testNoAutoRebalancingIdleNoScaling(VertxTestContext context) {
        Checkpoint reconciliation = context.checkpoint();
        // 1st reconcile, Kafka cluster creation
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // just checking that on Kafka cluster creation with no scaling, the auto-rebalancing is just in Idle state
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                })))
                // 2nd reconcile, no scaling down/up triggered
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // just checking that without any scaling, the auto-rebalancing just stays in Idle state
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
                    reconciliation.flag();
                })));
    }

    @Test
    public void testNoAutoRebalancingWithoutCruiseControl(VertxTestContext context) {
        // remove the Cruise Control definition, to test the auto-rebalancing reconciler behaviour
        Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).edit(
                k -> new KafkaBuilder(k)
                        .editSpec()
                            .withCruiseControl(null)
                        .endSpec()
                        .build()
        );

        Checkpoint reconciliation = context.checkpoint();
        operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // just checking that on Kafka cluster creation with no Cruise Control, the auto-rebalancing doesn't run
                    Kafka k = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get();
                    assertThat(k.getStatus().getAutoRebalance(), is(nullValue()));
                    reconciliation.flag();
                })));
    }

    private void patchKafkaRebalanceState(KafkaRebalance kafkaRebalance, KafkaRebalanceState state) {
        KafkaRebalance kafkaRebalancePatch = new KafkaRebalanceBuilder(kafkaRebalance)
                .withNewStatus()
                .withObservedGeneration(1L)
                .withConditions(new ConditionBuilder()
                        .withType(state.name())
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();
        Crds.kafkaRebalanceOperation(client).inNamespace(namespace).resource(kafkaRebalancePatch).updateStatus();
    }
}
