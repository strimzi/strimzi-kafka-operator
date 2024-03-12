/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
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
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class KafkaUpgradeDowngradeMockTest {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUpgradeDowngradeMockTest.class);

    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    private static final Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;
    private ResourceOperatorSupplier supplier;
    private StrimziPodSetController podSetController;
    private KafkaAssemblyOperator operator;

    /*
     * HELPER METHODS
     */

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaNodePoolCrd()
                .withKafkaConnectCrd()
                .withKafkaMirrorMaker2Crd()
                .withStrimziPodSetCrd()
                .withDeploymentController()
                .withPodController()
                .withServiceController()
                .withDeletionController()
                .build();
        mockKube.start();
        client = mockKube.client();

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void afterAll() {
        sharedWorkerExecutor.close();
        vertx.close();
        mockKube.stop();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo)   {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        mockKube.prepareNamespace(namespace);
    }

    @AfterEach
    public void afterEach() {
        podSetController.stop();
        client.namespaces().withName(namespace).delete();
    }

    private Future<Void> initialize()   {
        supplier =  new ResourceOperatorSupplier(vertx, client, ResourceUtils.zookeeperLeaderFinder(vertx, client), ResourceUtils.adminClientProvider(),
                ResourceUtils.zookeeperScalerProvider(), ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(), PFA, 2_000);

        podSetController = new StrimziPodSetController(namespace, Labels.EMPTY, supplier.kafkaOperator, supplier.connectOperator, supplier.mirrorMaker2Operator, supplier.strimziPodSetOperator, supplier.podOperations, supplier.metricsProvider, Integer.parseInt(ClusterOperatorConfig.POD_SET_CONTROLLER_WORK_QUEUE_SIZE.defaultValue()));
        podSetController.start();

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        operator = new KafkaAssemblyOperator(vertx, PFA, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);

        LOGGER.info("Reconciling initially -> create");
        return operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME));
    }

    private Kafka kafkaWithVersions(String kafkaVersion, String messageFormatVersion, String protocolVersion)   {
        return new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(kafkaVersion)
                        .withConfig(Map.of(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, messageFormatVersion,
                                        KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, protocolVersion)
                        )
                    .endKafka()
                .endSpec()
                .build();
    }

    private Kafka kafkaWithVersions(String kafkaVersion)   {
        return new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(kafkaVersion)
                    .endKafka()
                .endSpec()
                .build();
    }

    private void assertVersionsInKafkaStatus(String operatorVersion, String kafkaVersion) {
        KafkaStatus status = Crds.kafkaOperation(client).inNamespace(namespace).withName(CLUSTER_NAME).get().getStatus();
        assertThat(status.getOperatorLastSuccessfulVersion(), is(operatorVersion));
        assertThat(status.getKafkaVersion(), is(kafkaVersion));
    }

    private void assertVersionsInStrimziPodSet(String kafkaVersion, String messageFormatVersion, String protocolVersion, String image)  {
        StrimziPodSet sps = supplier.strimziPodSetOperator.client().inNamespace(namespace).withName(CLUSTER_NAME + "-kafka").get();
        assertThat(sps.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(kafkaVersion));

        sps.getSpec().getPods().stream().map(PodSetUtils::mapToPod).forEach(pod -> {
            assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(kafkaVersion));
            assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION), is(messageFormatVersion));
            assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION), is(protocolVersion));
            assertThat(pod.getSpec().getContainers().get(0).getImage(), is(image));
        });

        for (int i = 0; i < 3; i++) {
            Pod pod = client.pods().inNamespace(namespace).withName(CLUSTER_NAME + "-kafka-" + i).get();
            assertThat(pod.getSpec().getContainers().get(0).getImage(), is(image));
            assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(kafkaVersion));
            assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION), is(messageFormatVersion));
            assertThat(pod.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION), is(protocolVersion));
        }
    }

    /*
     * UPGRADE TESTS
     */

    // Tests upgrade without the message format and protocol versions configured. In Kafka 3.0 and older, one rolling
    // update should happen => the LMFV field is deprecated and does nto need separate upgrade.
    @Test
    public void testUpgradeWithoutMessageAndProtocolVersions(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);
        Crds.kafkaOperation(client).inNamespace(namespace).resource(initialKafka).create();

        Checkpoint reconciliation = context.checkpoint();
        initialize()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                })))
                .compose(i -> {
                    // Update Kafka
                    Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(updatedKafka).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                })))
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                            KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    // Tests upgrade with the message format and protocol versions changed together with Kafka version change. Two
    // rolling updates should happen => first with the old message and protocol versions and another one which rolls
    // also protocol and message versions.
    @Test
    public void testUpgradeWithNewMessageAndProtocolVersions(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);
        Crds.kafkaOperation(client).inNamespace(namespace).resource(initialKafka).create();

        Checkpoint reconciliation = context.checkpoint();
        initialize()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                })))
                .compose(i -> {
                    // Update Kafka
                    Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                            KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(updatedKafka).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                })))
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                            KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    // Tests upgrade with the user changing Kafka version, inter.broker.protocol.version and log.message.format.version
    // in separate steps.
    @Test
    public void testUpgradeWithNewMessageAndProtocolVersionsInSeparatePhases(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);
        Crds.kafkaOperation(client).inNamespace(namespace).resource(initialKafka).create();

        Checkpoint reconciliation = context.checkpoint();
        initialize()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                })))
                .compose(i -> {
                    // Update Kafka
                    Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(updatedKafka).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                })))
                .compose(i -> {
                    // Update Kafka
                    Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                            KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(updatedKafka).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                            KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    // Tests upgrade without any versions specified in the CR for Kafka 3.0 and higher
    @Test
    public void testUpgradeWithoutAnyVersions(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);
        Crds.kafkaOperation(client).inNamespace(namespace).resource(initialKafka).create();

        Checkpoint reconciliation = context.checkpoint();
        initialize()
                .onComplete(context.succeeding(v -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);
                    context.verify(() -> assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE));
                }))
                .compose(i -> {
                    // Update Kafka
                    Kafka updatedKafka = new KafkaBuilder(KAFKA).build();
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(updatedKafka).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                })))
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                            KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    /*
     * DOWNGRADE TESTS
     */

    // Test regular downgrade with message and protocol versions defined everywhere and properly rolled out to all brokers.
    // The message and protocol versions used is the same as Kafka version we downgrade to.
    @Test
    public void testDowngradeWithMessageAndProtocolVersions(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);
        Crds.kafkaOperation(client).inNamespace(namespace).resource(initialKafka).create();

        Checkpoint reconciliation = context.checkpoint();
        initialize()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                })))
                .compose(i -> {
                    // Update Kafka
                    Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);
                    Crds.kafkaOperation(client).inNamespace(namespace).resource(updatedKafka).update();
                    return Future.succeededFuture();
                })
                .compose(v -> operator.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, CLUSTER_NAME)))
                .onComplete(context.succeeding(i -> context.verify(() -> {
                    assertVersionsInKafkaStatus(KafkaAssemblyOperator.OPERATOR_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);
                    assertVersionsInStrimziPodSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }
}
