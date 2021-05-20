/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.stringContainsInOrder;

@ExtendWith(VertxExtension.class)
public class KafkaUpgradeDowngradeMockTest {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUpgradeDowngradeMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_16);
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
                        .withNewListeners()
                            .addNewGenericKafkaListener()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                            .endGenericKafkaListener()
                        .endListeners()
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
    private KubernetesClient client;
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

    private Future<Void> initialize(VertxTestContext context, Kafka initialKafka)   {
        CustomResourceDefinition kafkaAssemblyCrd = Crds.kafka();

        client = new MockKube()
                .withCustomResourceDefinition(kafkaAssemblyCrd, Kafka.class, KafkaList.class)
                    .withInitialInstances(Collections.singleton(initialKafka))
                .end()
                .build();

        ResourceOperatorSupplier supplier =  new ResourceOperatorSupplier(vertx, client, ResourceUtils.zookeeperLeaderFinder(vertx, client),
                ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(), ResourceUtils.metricsProvider(), pfa, FeatureGates.NONE, 2_000);

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

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

    // Tests regular upgrade with the message format and protocol versions configured to the same Kafka
    // version as we are upgrading from.
    @Test
    public void testUpgradeWithMessageAndProtocolVersions(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);

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

                    reconciliation.flag();
                })));
    }

    // Tests recovery from failed upgrade with the message format and protocol versions configured to the same Kafka
    // version as we are upgrading from.
    @Test
    public void testUpgradeRecoveryWithMessageAndProtocolVersions(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);

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
                .compose(v -> {
                    StatefulSet sts = client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get();
                    StatefulSet modifiedSts = new StatefulSetBuilder(sts)
                            .editMetadata()
                                .addToAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                            .endMetadata()
                            .editSpec()
                                .editTemplate()
                                    .editMetadata()
                                        .addToAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                                        .addToAnnotations(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1")
                                    .endMetadata()
                                    .editSpec()
                                        .editContainer(0)
                                            .withImage(KafkaVersionTestUtils.LATEST_KAFKA_IMAGE)
                                        .endContainer()
                                    .endSpec()
                                .endTemplate()
                            .endSpec()
                            .build();
                    client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").createOrReplace(modifiedSts);

                    Pod pod = client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + 1).get();
                    Pod modifiedPod = new PodBuilder(pod)
                            .editMetadata()
                                .addToAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                                .addToAnnotations(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1")
                            .endMetadata()
                            .editSpec()
                                .editContainer(0)
                                    .withImage(KafkaVersionTestUtils.LATEST_KAFKA_IMAGE)
                                .endContainer()
                            .endSpec()
                            .build();
                    client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + 1).createOrReplace(modifiedPod);

                    return Future.succeededFuture();
                })
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    // Tests upgrade without the message format and protocol versions configured. Two rolling updates should happen => first
    // with the old message and protocol versions and another one which rolls also protocol and message versions.
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

    // Tests upgrade without the message format and protocol versions configured. Two rolling updates should happen => first
    // with the old message and protocol versions and another one which rolls also protocol and message versions.
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

    // Tests upgrade without any versions specified in the CR.
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

    // Tests regular upgrade with the message format and protocol versions configured to much older Kafka
    // version than we are upgrading from.
    @Test
    public void testUpgradeWithOlderMessageAndProtocolVersions(VertxTestContext context)  {
        String olderVersion = "2.0";

        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                olderVersion,
                olderVersion);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                olderVersion,
                olderVersion);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                                olderVersion,
                                olderVersion,
                                KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            olderVersion,
                            olderVersion,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    // Tests upgrade from Kafka version not supported by the current version of the operator with message format and
    // protocol versions specified.
    @Test
    public void testUpgradeFromUnsupportedKafkaVersionWithMessageAndProtocol(VertxTestContext context)  {
        KafkaVersion unsupported = VERSIONS.version("2.1.0");

        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                unsupported.messageVersion(),
                unsupported.protocolVersion());

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                unsupported.messageVersion(),
                unsupported.protocolVersion());

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                unsupported.messageVersion(),
                                unsupported.protocolVersion(),
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> {
                    StatefulSet sts = client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get();
                    StatefulSet modifiedSts = new StatefulSetBuilder(sts)
                            .editMetadata()
                                .addToAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, unsupported.version())
                            .endMetadata()
                            .editSpec()
                                .editTemplate()
                                    .editMetadata()
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION)
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION)
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION)
                                    .endMetadata()
                                    .editSpec()
                                        .editContainer(0)
                                            .withImage("strimzi/kafka:old-kafka-2.1.0")
                                        .endContainer()
                                    .endSpec()
                                .endTemplate()
                            .endSpec()
                            .build();
                    client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").createOrReplace(modifiedSts);

                    for (int i = 0; i < 3; i++) {
                        Pod pod = client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).get();
                        Pod modifiedPod = new PodBuilder(pod)
                                .editMetadata()
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION)
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION)
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION)
                                .endMetadata()
                                .editSpec()
                                    .editContainer(0)
                                        .withImage("strimzi/kafka:old-kafka-2.1.0")
                                    .endContainer()
                                .endSpec()
                                .build();
                        client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).createOrReplace(modifiedPod);
                    }

                    return Future.succeededFuture();
                })
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            unsupported.messageVersion(),
                            unsupported.protocolVersion(),
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    // Tests upgrade from Kafka version not supported by the current version of the operator without message format and
    // protocol versions specified.
    @Test
    public void testUpgradeFromUnsupportedKafkaVersionWithoutMessageAndProtocol(VertxTestContext context)  {
        KafkaVersion unsupported = VERSIONS.version("2.1.0");

        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                unsupported.messageVersion(),
                unsupported.protocolVersion());

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                unsupported.messageVersion(),
                                unsupported.protocolVersion(),
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> {
                    StatefulSet sts = client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get();
                    StatefulSet modifiedSts = new StatefulSetBuilder(sts)
                            .editMetadata()
                                .addToAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, unsupported.version())
                            .endMetadata()
                            .editSpec()
                                .editTemplate()
                                    .editMetadata()
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION)
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION)
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION)
                                    .endMetadata()
                                    .editSpec()
                                        .editContainer(0)
                                            .withImage("strimzi/kafka:old-kafka-2.1.0")
                                        .endContainer()
                                    .endSpec()
                                .endTemplate()
                            .endSpec()
                            .build();
                    client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").createOrReplace(modifiedSts);

                    for (int i = 0; i < 3; i++) {
                        Pod pod = client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).get();
                        Pod modifiedPod = new PodBuilder(pod)
                                .editMetadata()
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION)
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION)
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION)
                                .endMetadata()
                                .editSpec()
                                    .editContainer(0)
                                        .withImage("strimzi/kafka:old-kafka-2.1.0")
                                    .endContainer()
                                .endSpec()
                                .build();
                        client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).createOrReplace(modifiedPod);
                    }

                    return Future.succeededFuture();
                })
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            unsupported.messageVersion(),
                            unsupported.protocolVersion(),
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);

                    reconciliation.flag();
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

    // Tests upgrade when Kafka StatefulSet and/or Pods existing but without any of the version annotations. This
    // indicates that the Statefulset / Pods were not using the current or recent Strimzi version and since we do not
    // know the version, we should wail.
    @Test
    public void testUpgradeWithoutAnyVersionInPodsOrStsFails(VertxTestContext context)  {
        KafkaVersion unsupported = VERSIONS.version("2.1.0");

        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                unsupported.messageVersion(),
                unsupported.protocolVersion());

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                unsupported.messageVersion(),
                                unsupported.protocolVersion(),
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> {
                    StatefulSet sts = client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get();
                    StatefulSet modifiedSts = new StatefulSetBuilder(sts)
                            .editMetadata()
                                .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION)
                            .endMetadata()
                            .editSpec()
                                .editTemplate()
                                    .editMetadata()
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION)
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION)
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION)
                                    .endMetadata()
                                .endTemplate()
                            .endSpec()
                            .build();
                    client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").createOrReplace(modifiedSts);

                    for (int i = 0; i < 3; i++) {
                        Pod pod = client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).get();
                        Pod modifiedPod = new PodBuilder(pod)
                                .editMetadata()
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION)
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION)
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION)
                                .endMetadata()
                                .build();
                        client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).createOrReplace(modifiedPod);
                    }

                    return Future.succeededFuture();
                })
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v.getMessage(), stringContainsInOrder("Kafka Pods or StatefulSet exist, but do not contain the strimzi.io/kafka-version annotation to detect their version. Kafka upgrade cannot be detected."));

                    reconciliation.flag();
                })));
    }

    /*
     * NOOP tests
     */

    // Tests regular reconciliation without any upgrades
    @Test
    public void testNoopUpgrade(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                            KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                            KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    // Tesst that Kafka works fine when both Pods and StatefulSets is missing at the beginning of reconciliation
    @Test
    public void testNoopWhenStatefulSetAndPodsAreMissing(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> {
                    client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").delete();

                    for (int i = 0; i < 3; i++) {
                        client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).delete();
                    }

                    return Future.succeededFuture();
                })
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
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

    // Test partial downgrade => emulate previous downgrade failing in the middle and verify it is finished.
    @Test
    public void testDowngradeRecoveryWithMessageAndProtocolVersions(VertxTestContext context)  {
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
                .compose(v -> {
                    StatefulSet sts = client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get();
                    StatefulSet modifiedSts = new StatefulSetBuilder(sts)
                            .editMetadata()
                                .addToAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)
                            .endMetadata()
                            .editSpec()
                                .editTemplate()
                                    .editMetadata()
                                        .addToAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)
                                        .addToAnnotations(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1")
                                    .endMetadata()
                                    .editSpec()
                                        .editContainer(0)
                                            .withImage(KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE)
                                        .endContainer()
                                    .endSpec()
                                .endTemplate()
                            .endSpec()
                            .build();
                    client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").createOrReplace(modifiedSts);

                    Pod pod = client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + 1).get();
                    Pod modifiedPod = new PodBuilder(pod)
                            .editMetadata()
                                .addToAnnotations(KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION)
                                .addToAnnotations(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "1")
                            .endMetadata()
                            .editSpec()
                                .editContainer(0)
                                    .withImage(KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE)
                                .endContainer()
                            .endSpec()
                            .build();
                    client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + 1).createOrReplace(modifiedPod);

                    return Future.succeededFuture();
                })
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                            KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    // Test regular downgrade with message and protocol versions defined everywhere and properly rolled out to all brokers.
    // The message and protocol versions used are older than the Kafka version we downgrade to.
    @Test
    public void testDowngradeWithOlderMessageAndProtocolVersions(VertxTestContext context)  {
        String olderVersion = "2.0";

        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                olderVersion,
                olderVersion);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                olderVersion,
                olderVersion);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                olderVersion,
                                olderVersion,
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertVersionsInStatefulSet(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                            olderVersion,
                            olderVersion,
                            KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);

                    reconciliation.flag();
                })));
    }

    // Test downgrade with message and protocol versions defined to newer version than we downgrade to (this fails before
    // reaching downgrade since this is invalid Kafka CR).
    @Test
    public void testDowngradeWithWrongMessageAndProtocolVersionsFails(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v.getMessage(), stringContainsInOrder("does not match the required pattern"));

                    reconciliation.flag();
                })));
    }

    // Test downgrade without message and protocol versions configured and the default being used and rolled out to all brokers.
    @Test
    public void testDowngradeWithoutMessageAndProtocolVersionsFails(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v.getMessage(), stringContainsInOrder("used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to"));

                    reconciliation.flag();
                })));
    }

    // Test downgrade with message and protocol versions defined to correct version in the CR, but not on the broker pods.
    @Test
    public void testDowngradeWithWrongMessageAndProtocolVersionsOnPodsFails(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v.getMessage(), stringContainsInOrder("used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to"));

                    reconciliation.flag();
                })));
    }

    // Test downgrade with message and protocol versions defined to correct version in the CR, but not on the broker pods.
    @Test
    public void testDowngradeWithNoMessageAndProtocolVersionsOnPodsFails(VertxTestContext context)  {
        Kafka initialKafka = kafkaWithVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION);

        Kafka updatedKafka = kafkaWithVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);

        Checkpoint reconciliation = context.checkpoint();
        initialize(context, initialKafka)
                .onComplete(context.succeeding(v -> {
                    context.verify(() -> {
                        assertVersionsInStatefulSet(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                                KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                                KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                                KafkaVersionTestUtils.LATEST_KAFKA_IMAGE);
                    });
                }))
                .compose(v -> {
                    StatefulSet sts = client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").get();
                    StatefulSet modifiedSts = new StatefulSetBuilder(sts)
                            .editSpec()
                                .editTemplate()
                                    .editMetadata()
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION)
                                        .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION)
                                    .endMetadata()
                                .endTemplate()
                            .endSpec()
                            .build();
                    client.apps().statefulSets().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka").createOrReplace(modifiedSts);

                    for (int i = 0; i < 3; i++) {
                        Pod pod = client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).get();
                        Pod modifiedPod = new PodBuilder(pod)
                                .editMetadata()
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_LOG_MESSAGE_FORMAT_VERSION)
                                    .removeFromAnnotations(KafkaCluster.ANNO_STRIMZI_IO_INTER_BROKER_PROTOCOL_VERSION)
                                .endMetadata()
                                .build();
                        client.pods().inNamespace(NAMESPACE).withName(CLUSTER_NAME + "-kafka-" + i).createOrReplace(modifiedPod);
                    }

                    return Future.succeededFuture();
                })
                .compose(v -> operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v.getMessage(), stringContainsInOrder("log.message.format.version (null) and inter.broker.protocol.version (null) used by the brokers have to be set and be lower or equal to the Kafka broker version we downgrade to"));

                    reconciliation.flag();
                })));
    }
}
