/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.annotations.ParallelParametrizedTest;
import io.strimzi.test.annotations.ParallelSuite;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.storage.Storage.deleteClaim;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(VertxExtension.class)
@ParallelSuite
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaAssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaAssemblyOperatorMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static Vertx vertx;

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_16;

    /*
     * Params contains the parameterized fields of different configurations of a Kafka and Zookeeper Strimzi cluster
     */
    public static class Params {
        private final int zkReplicas;
        private final SingleVolumeStorage zkStorage;

        private final int kafkaReplicas;
        private final Storage kafkaStorage;
        private ResourceRequirements resources;

        public Params(int zkReplicas,
                      SingleVolumeStorage zkStorage,
                      int kafkaReplicas,
                      Storage kafkaStorage,
                      ResourceRequirements resources) {
            this.zkReplicas = zkReplicas;
            this.zkStorage = zkStorage;
            this.kafkaReplicas = kafkaReplicas;
            this.kafkaStorage = kafkaStorage;
            this.resources = resources;
        }

        public String toString() {
            return "zkReplicas=" + zkReplicas +
                    ",zkStorage=" + zkStorage +
                    ",kafkaReplicas=" + kafkaReplicas +
                    ",kafkaStorage=" + kafkaStorage +
                    ",resources=" + resources;
        }
    }

    public static Iterable<KafkaAssemblyOperatorMockTest.Params> data() {
        int[] replicas = {1, 3};
        Storage[] kafkaStorageConfigs = {
            new EphemeralStorage(),
            new PersistentClaimStorageBuilder()
                .withSize("123")
                .withStorageClass("foo")
                .withDeleteClaim(true)
                .build(),
            new PersistentClaimStorageBuilder()
                .withSize("123")
                .withStorageClass("foo")
                .withDeleteClaim(false)
                .build()
        };
        SingleVolumeStorage[] zkStorageConfigs = {
            new EphemeralStorage(),
            new PersistentClaimStorageBuilder()
                    .withSize("123")
                    .withStorageClass("foo")
                    .withDeleteClaim(true)
                    .build(),
            new PersistentClaimStorageBuilder()
                    .withSize("123")
                    .withStorageClass("foo")
                    .withDeleteClaim(false)
                    .build()
        };
        ResourceRequirements[] resources = {
            new ResourceRequirementsBuilder()
                .addToLimits("cpu", new Quantity("5000m"))
                .addToLimits("memory", new Quantity("5000m"))
                .addToRequests("cpu", new Quantity("5000"))
                .addToRequests("memory", new Quantity("5000m"))
                .build()
        };
        List<KafkaAssemblyOperatorMockTest.Params> result = new ArrayList();

        for (int zkReplica : replicas) {
            for (SingleVolumeStorage zkStorage : zkStorageConfigs) {
                for (int kafkaReplica : replicas) {
                    for (Storage kafkaStorage : kafkaStorageConfigs) {
                        for (ResourceRequirements resource : resources) {
                            result.add(new KafkaAssemblyOperatorMockTest.Params(
                                    zkReplica, zkStorage,
                                    kafkaReplica, kafkaStorage, resource));
                        }
                    }
                }
            }
        }

        return result;
    }


    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    class TestData {

        private final String namespaceName;
        private final String clusterName;
        private final Kafka kafkaCluster;
        private final KubernetesClient client;
        private final KafkaAssemblyOperator operator;

        public TestData(String namespaceName, String clusterName, Kafka kafkaCluster, KubernetesClient client, KafkaAssemblyOperator operator) {
            this.namespaceName = namespaceName;
            this.clusterName = clusterName;
            this.kafkaCluster = kafkaCluster;
            this.client = client;
            this.operator = operator;
        }

        public Kafka getKafkaCluster() {
            return kafkaCluster;
        }
        public KubernetesClient getClient() {
            return client;
        }
        public KafkaAssemblyOperator getOperator() {
            return operator;
        }

        public String getNamespaceName() {
            return namespaceName;
        }
        public String getClusterName() {
            return clusterName;
        }
    }

    /*
     * init is equivalent to a @BeforeEach method
     * since this is a parameterized set, the tests params are only available at test start
     * This must be called before each test
     */
    private TestData init(Params params) {
        final String namespaceName = NAMESPACE + "-" + new Random().nextInt(Integer.MAX_VALUE);
        final String clusterName = CLUSTER_NAME + "-" + new Random().nextInt(Integer.MAX_VALUE);

        Kafka cluster = new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespaceName)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(params.kafkaReplicas)
                        .withStorage(params.kafkaStorage)
                        .withNewListeners()
                            .addNewGenericKafkaListener()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                            .endGenericKafkaListener()
                            .addNewGenericKafkaListener()
                                .withName("tls")
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                            .endGenericKafkaListener()
                        .endListeners()
                        .withMetrics(singletonMap("foo", "bar"))
                        .withResources(params.resources)
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(params.zkReplicas)
                        .withStorage(params.zkStorage)
                        .withMetrics(singletonMap("foo", "bar"))
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        CustomResourceDefinition kafkaAssemblyCrd = Crds.kafka();

        KubernetesClient client = new MockKube()
                .withCustomResourceDefinition(kafkaAssemblyCrd, Kafka.class, KafkaList.class)
                    .withInitialInstances(Collections.singleton(cluster))
                .end()
                .build();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        ResourceOperatorSupplier supplier = supplierWithMocks(client);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        KafkaAssemblyOperator operator = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(),
                new PasswordGenerator(10, "a", "a"), supplier, config);

        return new TestData(namespaceName, clusterName, cluster, client, operator);
    }

    private ResourceOperatorSupplier supplierWithMocks(KubernetesClient client) {
        ZookeeperLeaderFinder leaderFinder = ResourceUtils.zookeeperLeaderFinder(vertx, client);
        return new ResourceOperatorSupplier(vertx, client, leaderFinder,
                ResourceUtils.adminClientProvider(), ResourceUtils.zookeeperScalerProvider(),
                ResourceUtils.metricsProvider(), new PlatformFeaturesAvailability(true, kubernetesVersion),
                2_000);
    }

    private void assertResourceRequirements(VertxTestContext context, String statefulSetName, TestData testData, Params params) {
        context.verify(() -> {
            StatefulSet statefulSet = testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(statefulSetName).get();
            assertThat(statefulSet, is(notNullValue()));
            ResourceRequirements requirements = statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getResources();
            if (params.resources != null && params.resources.getRequests() != null) {
                assertThat(requirements.getRequests(), hasEntry("cpu", params.resources.getRequests().get("cpu")));
                assertThat(requirements.getRequests(), hasEntry("memory", params.resources.getRequests().get("memory")));
            }
            if (params.resources != null && params.resources.getLimits() != null) {
                assertThat(requirements.getLimits(), hasEntry("cpu", params.resources.getLimits().get("cpu")));
                assertThat(requirements.getLimits(), hasEntry("memory", params.resources.getLimits().get("memory")));
            }
        });
    }

    private Future<Void> initialReconcile(VertxTestContext context, TestData testData, Params params) {
        LOGGER.info("Reconciling initially -> create");

        return testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName()))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                StatefulSet kafkaSts = testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.kafkaClusterName(testData.getClusterName())).get();
                kafkaSts.setStatus(new StatefulSetStatus());

                assertThat(kafkaSts, is(notNullValue()));
                assertThat(kafkaSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "0"));
                assertThat(kafkaSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                assertThat(kafkaSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));

                StatefulSet zkSts = testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(ZookeeperCluster.zookeeperClusterName(testData.getClusterName())).get();
                assertThat(zkSts, is(notNullValue()));
                assertThat(zkSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION, "0"));
                assertThat(zkSts.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));

                assertThat(testData.getClient().configMaps().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.metricAndLogConfigsName(testData.getClusterName())).get(), is(notNullValue()));
                assertThat(testData.getClient().configMaps().inNamespace(testData.getNamespaceName()).withName(ZookeeperCluster.zookeeperMetricAndLogConfigsName(testData.getClusterName())).get(), is(notNullValue()));
                assertResourceRequirements(context, KafkaCluster.kafkaClusterName(testData.getClusterName()), testData, params);
                assertThat(testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.clientsCaKeySecretName(testData.getClusterName())).get(), is(notNullValue()));
                assertThat(testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.clientsCaCertSecretName(testData.getClusterName())).get(), is(notNullValue()));
                assertThat(testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.clusterCaCertSecretName(testData.getClusterName())).get(), is(notNullValue()));
                assertThat(testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.brokersSecretName(testData.getClusterName())).get(), is(notNullValue()));
                assertThat(testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(ZookeeperCluster.nodesSecretName(testData.getClusterName())).get(), is(notNullValue()));
            })));
    }

    private void createKafkaCluster() {

    }

    /** Create a cluster from a Kafka */
    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcile(Params params, VertxTestContext context) {
        TestData testData = init(params);

        Checkpoint async = context.checkpoint();
        initialReconcile(context, testData, params)
            .onComplete(context.succeeding())
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> async.flag()));
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileReplacesAllDeletedSecrets(Params params, VertxTestContext context) {
        TestData testData = init(params);

        initialReconcileThenDeleteSecretsThenReconcile(context,
                testData,
                params,
                KafkaCluster.clientsCaKeySecretName(testData.getClusterName()),
                KafkaCluster.clientsCaCertSecretName(testData.getClusterName()),
                KafkaCluster.clusterCaCertSecretName(testData.getClusterName()),
                KafkaCluster.brokersSecretName(testData.getClusterName()),
                ZookeeperCluster.nodesSecretName(testData.getClusterName()),
                ClusterOperator.secretName(testData.getClusterName()));
    }

    /**
     * Test the operator re-creates secrets if they get deleted
     */
    private void initialReconcileThenDeleteSecretsThenReconcile(VertxTestContext context, TestData testData, Params params, String... secrets) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context, testData, params)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String secret: secrets) {
                    testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(secret).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                    assertThat("Expected secret " + secret + " to not exist",
                            testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(secret).get(), is(nullValue()));
                }
                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String secret: secrets) {
                    assertThat("Expected secret " + secret + " to have been recreated",
                            testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(secret).get(), is(notNullValue()));
                }
                async.flag();
            })));
    }

    /**
     * Test the operator re-creates services if they get deleted
     */
    private void initialReconcileThenDeleteServicesThenReconcile(VertxTestContext context, TestData testData, Params params, String... services) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context, testData, params)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service : services) {
                    testData.getClient().services().inNamespace(testData.getNamespaceName()).withName(service).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                    assertThat("Expected service " + service + " to be not exist",
                            testData.getClient().services().inNamespace(testData.getNamespaceName()).withName(service).get(), is(nullValue()));
                }
                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                for (String service: services) {
                    assertThat("Expected service " + service + " to have been recreated",
                            testData.getClient().services().inNamespace(testData.getNamespaceName()).withName(service).get(), is(notNullValue()));
                }
                async.flag();
            })));
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileReplacesDeletedZookeeperServices(Params params, VertxTestContext context) {
        TestData testData = init(params);

        initialReconcileThenDeleteServicesThenReconcile(context,
                testData,
                params,
                ZookeeperCluster.serviceName(testData.getClusterName()),
                ZookeeperCluster.headlessServiceName(testData.getClusterName()));
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileReplacesDeletedKafkaServices(Params params, VertxTestContext context) {
        TestData testData = init(params);

        initialReconcileThenDeleteServicesThenReconcile(context,
                testData,
                params,
                KafkaCluster.serviceName(testData.getClusterName()),
                KafkaCluster.headlessServiceName(testData.getClusterName()));
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileReplacesDeletedZookeeperStatefulSet(Params params, VertxTestContext context) {
        TestData testData = init(params);

        String statefulSet = ZookeeperCluster.zookeeperClusterName(testData.getClusterName());
        initialReconcileThenDeleteStatefulSetsThenReconcile(context, statefulSet, testData, params);
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileReplacesDeletedKafkaStatefulSet(Params params, VertxTestContext context) {
        TestData testData = init(params);

        String statefulSet = KafkaCluster.kafkaClusterName(testData.getClusterName());
        initialReconcileThenDeleteStatefulSetsThenReconcile(context, statefulSet, testData, params);
    }

    private void initialReconcileThenDeleteStatefulSetsThenReconcile(VertxTestContext context, String statefulSet, TestData testData, Params params) {
        Checkpoint async = context.checkpoint();

        initialReconcile(context, testData, params)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(statefulSet).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                assertThat("Expected sts " + statefulSet + " should not exist",
                        testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(statefulSet).get(), is(nullValue()));

                LOGGER.info("Reconciling again -> update");
            })))
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat("Expected sts " + statefulSet + " should have been re-created",
                        testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(statefulSet).get(), is(notNullValue()));
                async.flag();
            })));
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileUpdatesKafkaPersistentVolumes(Params params, VertxTestContext context) {
        TestData testData = init(params);
        assumeTrue(params.kafkaStorage instanceof PersistentClaimStorage, "Parameterized Test only runs for Params with Kafka Persistent storage");

        String originalStorageClass = Storage.storageClass(params.kafkaStorage);

        Checkpoint async = context.checkpoint();
        initialReconcile(context, testData, params)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertStorageClass(context, KafkaCluster.kafkaClusterName(testData.getClusterName()), originalStorageClass, testData);

                // Try to update the storage class
                String changedClass = originalStorageClass + "2";

                Kafka patchedPersistenceKafka = new KafkaBuilder(testData.getKafkaCluster())
                        .editSpec()
                            .editKafka()
                                .withNewPersistentClaimStorage()
                                    .withStorageClass(changedClass)
                                    .withSize("123")
                                .endPersistentClaimStorage()
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(testData).patch(patchedPersistenceKafka);

                LOGGER.info("Updating with changed storage class");
            })))
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the storage class was not changed
                assertStorageClass(context, KafkaCluster.kafkaClusterName(testData.getClusterName()), originalStorageClass, testData);
                async.flag();
            })));
    }

    private Resource<Kafka> kafkaAssembly(TestData testData) {
        CustomResourceDefinition crd = testData.getClient().apiextensions().v1().customResourceDefinitions().withName(Kafka.CRD_NAME).get();
        return testData.getClient().customResources(CustomResourceDefinitionContext.fromCrd(crd), Kafka.class, KafkaList.class)
                .inNamespace(testData.getNamespaceName()).withName(testData.getClusterName());
    }

    private void assertStorageClass(VertxTestContext context, String statefulSetName, String expectedClass, TestData testData) {
        StatefulSet statefulSet = testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(statefulSetName).get();
        context.verify(() -> {
            assertThat(statefulSet, is(notNullValue()));
            // Check the storage class is initially "foo"
            List<PersistentVolumeClaim> volumeClaimTemplates = statefulSet.getSpec().getVolumeClaimTemplates();
            assertThat(volumeClaimTemplates, hasSize(1));
            assertThat(volumeClaimTemplates.get(0).getSpec().getStorageClassName(), is(expectedClass));
        });
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileUpdatesKafkaStorageType(Params params, VertxTestContext context) {
        TestData testData = init(params);

        AtomicReference<List<PersistentVolumeClaim>> originalPVCs = new AtomicReference<>();
        AtomicReference<List<Volume>> originalVolumes = new AtomicReference<>();
        AtomicReference<List<Container>> originalInitContainers = new AtomicReference<>();

        Checkpoint async = context.checkpoint();
        initialReconcile(context, testData, params)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                originalPVCs.set(Optional.ofNullable(testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.kafkaClusterName(testData.getClusterName())).get())
                        .map(StatefulSet::getSpec)
                        .map(StatefulSetSpec::getVolumeClaimTemplates)
                        .orElse(new ArrayList<>()));
                originalVolumes.set(Optional.ofNullable(testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.kafkaClusterName(testData.getClusterName())).get())
                        .map(StatefulSet::getSpec)
                        .map(StatefulSetSpec::getTemplate)
                        .map(PodTemplateSpec::getSpec)
                        .map(PodSpec::getVolumes)
                        .orElse(new ArrayList<>()));
                originalInitContainers.set(Optional.ofNullable(testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.kafkaClusterName(testData.getClusterName())).get())
                        .map(StatefulSet::getSpec)
                        .map(StatefulSetSpec::getTemplate)
                        .map(PodTemplateSpec::getSpec)
                        .map(PodSpec::getInitContainers)
                        .orElse(new ArrayList<>()));

                // Update the storage type
                // ephemeral -> persistent
                // or
                // persistent -> ephemeral
                Kafka updatedStorageKafka = null;
                if (params.kafkaStorage instanceof EphemeralStorage) {
                    updatedStorageKafka = new KafkaBuilder(testData.getKafkaCluster())
                            .editSpec()
                                .editKafka()
                                    .withNewPersistentClaimStorage()
                                        .withSize("123")
                                    .endPersistentClaimStorage()
                                .endKafka()
                            .endSpec()
                            .build();
                } else if (params.kafkaStorage instanceof PersistentClaimStorage) {
                    updatedStorageKafka = new KafkaBuilder(testData.getKafkaCluster())
                            .editSpec()
                                .editKafka()
                                    .withNewEphemeralStorage()
                                    .endEphemeralStorage()
                                .endKafka()
                            .endSpec()
                            .build();
                } else {
                    context.failNow(new Exception("If storage is not ephemeral or persistent something has gone wrong"));
                }
                kafkaAssembly(testData).patch(updatedStorageKafka);

                LOGGER.info("Updating with changed storage type");
            })))
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check the Volumes and PVCs were not changed
                assertPVCs(context, KafkaCluster.kafkaClusterName(testData.getClusterName()), originalPVCs.get(), testData);
                assertVolumes(context, KafkaCluster.kafkaClusterName(testData.getClusterName()), originalVolumes.get(), testData);
                assertInitContainers(context, KafkaCluster.kafkaClusterName(testData.getClusterName()), originalInitContainers.get(), testData);
                async.flag();
            })));
    }

    private void assertPVCs(VertxTestContext context, String statefulSetName, List<PersistentVolumeClaim> originalPVCs, TestData testData) {
        context.verify(() -> {
            StatefulSet statefulSet = testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(statefulSetName).get();
            assertThat(statefulSet, is(notNullValue()));
            List<PersistentVolumeClaim> pvcs = statefulSet.getSpec().getVolumeClaimTemplates();
            assertThat(originalPVCs.size(), is(pvcs.size()));
            assertThat(originalPVCs, is(pvcs));
        });

    }

    private void assertVolumes(VertxTestContext context, String statefulSetName, List<Volume> originalVolumes, TestData testData) {
        context.verify(() -> {
            StatefulSet statefulSet = testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(statefulSetName).get();
            assertThat(statefulSet, is(notNullValue()));
            List<Volume> volumes = statefulSet.getSpec().getTemplate().getSpec().getVolumes();
            assertThat(originalVolumes.size(), is(volumes.size()));
            assertThat(originalVolumes, is(volumes));
        });
    }

    private void assertInitContainers(VertxTestContext context, String statefulSetName, List<Container> originalInitContainers, TestData testData) {
        context.verify(() -> {
            StatefulSet statefulSet = testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(statefulSetName).get();
            assertThat(statefulSet, is(notNullValue()));
            List<Container> initContainers = statefulSet.getSpec().getTemplate().getSpec().getInitContainers();
            assertThat(originalInitContainers.size(), is(initContainers.size()));
            assertThat(originalInitContainers, is(initContainers));
        });
    }


    /** Test that we can change the deleteClaim flag, and that it's honoured */
    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileUpdatesKafkaWithChangedDeleteClaim(Params params, VertxTestContext context) {
        TestData testData = init(params);
        assumeTrue(params.kafkaStorage instanceof PersistentClaimStorage, "Kafka delete claims do not apply to non-persistent volumes");

        Map<String, String> kafkaLabels = new HashMap<>();
        kafkaLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        kafkaLabels.put(Labels.STRIMZI_CLUSTER_LABEL, testData.getClusterName());
        kafkaLabels.put(Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(testData.getClusterName()));

        Map<String, String> zkLabels = new HashMap<>();
        zkLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        zkLabels.put(Labels.STRIMZI_CLUSTER_LABEL, testData.getClusterName());
        zkLabels.put(Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(testData.getClusterName()));

        AtomicReference<Set<String>> kafkaPvcs = new AtomicReference<>();
        AtomicReference<Set<String>> zkPvcs = new AtomicReference<>();
        AtomicBoolean originalKafkaDeleteClaim = new AtomicBoolean();


        Checkpoint async = context.checkpoint();

        initialReconcile(context, testData, params)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                kafkaPvcs.set(testData.getClient().persistentVolumeClaims().inNamespace(testData.getNamespaceName()).withLabels(kafkaLabels).list().getItems()
                        .stream()
                        .map(pvc -> pvc.getMetadata().getName())
                        .collect(Collectors.toSet()));

                zkPvcs.set(testData.getClient().persistentVolumeClaims().inNamespace(testData.getNamespaceName()).withLabels(zkLabels).list().getItems()
                        .stream()
                        .map(pvc -> pvc.getMetadata().getName())
                        .collect(Collectors.toSet()));

                originalKafkaDeleteClaim.set(deleteClaim(params.kafkaStorage));

                // Try to update the storage class
                Kafka updatedStorageKafka = new KafkaBuilder(testData.getKafkaCluster()).editSpec().editKafka()
                        .withNewPersistentClaimStorage()
                        .withSize("123")
                        .withStorageClass("foo")
                        .withDeleteClaim(!originalKafkaDeleteClaim.get())
                        .endPersistentClaimStorage().endKafka().endSpec().build();
                kafkaAssembly(testData).patch(updatedStorageKafka);
                LOGGER.info("Updating with changed delete claim");
            })))
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // check that the new delete-claim annotation is on the PVCs
                for (String pvcName: kafkaPvcs.get()) {
                    assertThat(testData.getClient().persistentVolumeClaims().inNamespace(testData.getNamespaceName()).withName(pvcName).get()
                                    .getMetadata().getAnnotations(),
                            hasEntry(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(!originalKafkaDeleteClaim.get())));
                }
                kafkaAssembly(testData).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                LOGGER.info("Reconciling again -> delete");
            })))
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> async.flag()));
    }

    /** Create a cluster from a Kafka Cluster CM */
    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileKafkaScaleDown(Params params, VertxTestContext context) {
        TestData testData = init(params);

        assumeTrue(params.kafkaReplicas > 1, "Skipping scale down test because there's only 1 broker");

        int scaleDownTo = params.kafkaReplicas - 1;
        // final ordinal will be deleted
        String deletedPod = KafkaCluster.kafkaPodName(testData.getClusterName(), scaleDownTo);

        AtomicInteger brokersInternalCertsCount = new AtomicInteger();

        Checkpoint async = context.checkpoint();
        initialReconcile(context, testData, params)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                brokersInternalCertsCount.set(testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.brokersSecretName(testData.getClusterName())).get()
                        .getData()
                        .size());

                assertThat(testData.getClient().pods().inNamespace(testData.getNamespaceName()).withName(deletedPod).get(), is(notNullValue()));

                Kafka scaledDownCluster = new KafkaBuilder(testData.getKafkaCluster())
                        .editSpec()
                            .editKafka()
                                .withReplicas(scaleDownTo)
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(testData).patch(scaledDownCluster);

                LOGGER.info("Scaling down to {} Kafka pods", scaleDownTo);
            })))
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.kafkaClusterName(testData.getClusterName())).get().getSpec().getReplicas(),
                        is(scaleDownTo));
                assertThat("Expected pod " + deletedPod + " to have been deleted",
                        testData.getClient().pods().inNamespace(testData.getNamespaceName()).withName(deletedPod).get(),
                        is(nullValue()));

                // removing one pod, the related private and public keys, keystore and password (4 entries) should not be in the Secrets
                assertThat(testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.brokersSecretName(testData.getClusterName())).get()
                                .getData(),
                        aMapWithSize(brokersInternalCertsCount.get() - 4));

                // TODO assert no rolling update
                async.flag();
            })));
    }

    /** Create a cluster from a Kafka Cluster CM */
    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileKafkaScaleUp(Params params, VertxTestContext context) {
        TestData testData = init(params);

        AtomicInteger brokersInternalCertsCount = new AtomicInteger();

        Checkpoint async = context.checkpoint();
        int scaleUpTo = params.kafkaReplicas + 1;
        String newPod = KafkaCluster.kafkaPodName(testData.getClusterName(), params.kafkaReplicas);

        initialReconcile(context, testData, params)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                brokersInternalCertsCount.set(testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.brokersSecretName(testData.getClusterName())).get()
                        .getData()
                        .size());

                assertThat(testData.getClient().pods().inNamespace(testData.getNamespaceName()).withName(newPod).get(), is(nullValue()));

                Kafka scaledUpKafka = new KafkaBuilder(testData.getKafkaCluster())
                        .editSpec()
                            .editKafka()
                                .withReplicas(scaleUpTo)
                            .endKafka()
                        .endSpec()
                        .build();
                kafkaAssembly(testData).patch(scaledUpKafka);

                LOGGER.info("Scaling up to {} Kafka pods", scaleUpTo);
            })))
            .compose(v -> testData.getOperator().reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName())))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(testData.getClient().apps().statefulSets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.kafkaClusterName(testData.getClusterName())).get().getSpec().getReplicas(),
                        is(scaleUpTo));
                assertThat("Expected pod " + newPod + " to have been created",
                        testData.getClient().pods().inNamespace(testData.getNamespaceName()).withName(newPod).get(),
                        is(notNullValue()));

                // adding one pod, the related private and public keys, keystore and password should be added to the Secrets
                assertThat(testData.getClient().secrets().inNamespace(testData.getNamespaceName()).withName(KafkaCluster.brokersSecretName(testData.getClusterName())).get().getData(),
                        aMapWithSize(brokersInternalCertsCount.get() + 4));

                // TODO assert no rolling update
                async.flag();
            })));
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileResumePartialRoll(Params params, VertxTestContext context) {
        TestData testData = init(params);

        Checkpoint async = context.checkpoint();
        initialReconcile(context, testData, params)
            .onComplete(v -> async.flag());
    }

    /** Test the ZK version change functions */
    private void reconcileZkVersionChange(VertxTestContext context, String initialKafkaVersion, String changedKafkaVersion, String changedImage, TestData testData, Params params) {
        // We set the versions in the initial cluster to allow downgrades / upgrades
        KafkaVersion lowerVersion = KafkaVersion.compareDottedVersions(initialKafkaVersion, changedKafkaVersion) > 0 ? VERSIONS.version(changedKafkaVersion) : VERSIONS.version(initialKafkaVersion);

        Map<String, Object> config = new HashMap<>(2);
        config.put(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, lowerVersion.messageVersion());
        config.put(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, lowerVersion.protocolVersion());

        testData.getKafkaCluster().getSpec().getKafka().setConfig(config);
        testData.getKafkaCluster().getSpec().getKafka().setVersion(initialKafkaVersion);

        // We prepare updated Kafka with new version
        Kafka updatedKafka = new KafkaBuilder(testData.getKafkaCluster())
                .editSpec()
                    .editKafka()
                        .withVersion(changedKafkaVersion)
                    .endKafka()
                .endSpec()
                .build();

        KafkaAssemblyOperator.ReconciliationState initialState = testData.getOperator().new ReconciliationState(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, testData.getNamespaceName(), testData.getClusterName()),
                updatedKafka);

        Checkpoint async = context.checkpoint();
        initialReconcile(context, testData, params)
            .onComplete(context.succeeding())
            .compose(v -> initialState.getKafkaClusterDescription())
            .compose(v -> initialState.prepareVersionChange())
            .compose(v -> initialState.getZookeeperDescription())
            .compose(state -> state.zkVersionChange())
            .onComplete(context.succeeding(state -> context.verify(() -> {
                assertThat(state.zkCluster.getImage(), is(changedImage));
                async.flag();
            })));
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileZookeeperUpgradeFromPreviousToLatest(Params params, VertxTestContext context) {
        TestData testData = init(params);

        String initialKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String changedKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String changedImage = KafkaVersionTestUtils.LATEST_KAFKA_IMAGE;

        reconcileZkVersionChange(context, initialKafkaVersion, changedKafkaVersion, changedImage, testData, params);
    }

    @ParallelParametrizedTest
    @MethodSource("data")
    public void testReconcileZookeeperDowngradeFromLatestToPrevious(Params params, VertxTestContext context) {
        TestData testData = init(params);

        String initialKafkaVersion = KafkaVersionTestUtils.LATEST_KAFKA_VERSION;
        String changedKafkaVersion = KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION;
        String changedImage = KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE;

        reconcileZkVersionChange(context, initialKafkaVersion, changedKafkaVersion, changedImage, testData, params);
    }
}
