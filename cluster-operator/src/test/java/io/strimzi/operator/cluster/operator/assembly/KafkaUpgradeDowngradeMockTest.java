/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.mockkube2.MockKube2;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class KafkaUpgradeDowngradeMockTest {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUpgradeDowngradeMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16);
    private static Kafka basicKafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
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
    // Injected by Fabric8 Mock Kubernetes Server
    private KubernetesClient client;
    private MockKube2 mockKube;
    private KafkaAssemblyOperator operator;

    /*
     * HELPER METHODS
     */

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @AfterEach
    public void afterEach() {
        mockKube.stop();
    }

    private Future<Void> initialize(VertxTestContext context, Kafka initialKafka)   {
        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaCrd()
                .withInitialKafkas(initialKafka)
                .withStrimziPodSetCrd()
                .withStatefulSetController()
                .withPodController()
                .withServiceController()
                .withDeploymentController()
                .build();
        mockKube.start();

        ResourceOperatorSupplier supplier =  new ResourceOperatorSupplier(vertx, client, ResourceUtils.zookeeperLeaderFinder(vertx, client),
                ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(), ResourceUtils.metricsProvider(), pfa, 2_000);

        // This currently uses StatefulSets because of https://github.com/strimzi/strimzi-kafka-operator/issues/6946
        // This should move to StrimziPodSets after we move to Fabric8 6.0.0
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "-UseStrimziPodSets");

        operator = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);

        LOGGER.info("Reconciling initially -> create");
        return operator.reconcile(new Reconciliation("initial-reconciliation", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME));
    }

    private Kafka kafkaWithVersions(String kafkaVersion, String messageFormatVersion, String protocolVersion)   {
        return new KafkaBuilder(basicKafka)
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
        return new KafkaBuilder(basicKafka)
                .editSpec()
                    .editKafka()
                        .withVersion(kafkaVersion)
                    .endKafka()
                .endSpec()
                .build();
    }

    private void assertVersionsInStatefulSet(String kafkaVersion, String messageFormatVersion, String protocolVersion, String image)  {
        StatefulSet sts = client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get();
        assertThat(sts.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is(image));
        assertThat(sts.getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(kafkaVersion));
        assertThat(sts.getSpec().getTemplate().getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION), is(kafkaVersion));
        assertThat(sts.getSpec().getTemplate().getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION), is(messageFormatVersion));
        assertThat(sts.getSpec().getTemplate().getMetadata().getAnnotations().get(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION), is(protocolVersion));

        for (int i = 0; i < 3; i++) {
            Pod pod = client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).get();
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

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
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

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
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

        Kafka updatedKafka1 = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);

        Kafka updatedKafka2 = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka1))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka2))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
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

        Kafka updatedKafka = new KafkaBuilder(basicKafka).build();

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                })))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger2", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
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

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }
}
