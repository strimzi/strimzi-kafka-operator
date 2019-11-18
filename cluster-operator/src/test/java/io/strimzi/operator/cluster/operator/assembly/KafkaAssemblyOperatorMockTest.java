/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.storage.Storage.deleteClaim;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaAssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaAssemblyOperatorMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_9;

    private int zkReplicas;
    private SingleVolumeStorage zkStorage;

    private int kafkaReplicas;
    private Storage kafkaStorage;
    private ResourceRequirements resources;
    private KubernetesClient mockClient;

    public static class Params {
        private final int zkReplicas;
        private final SingleVolumeStorage zkStorage;

        private final int kafkaReplicas;
        private final Storage kafkaStorage;
        private ResourceRequirements resources;

        public Params(int zkReplicas,
                      SingleVolumeStorage zkStorage, int kafkaReplicas,
                      Storage kafkaStorage,
                      ResourceRequirements resources) {
            this.kafkaReplicas = kafkaReplicas;
            this.kafkaStorage = kafkaStorage;
            this.zkReplicas = zkReplicas;
            this.zkStorage = zkStorage;
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

    public void setFields(KafkaAssemblyOperatorMockTest.Params params) {
        this.zkReplicas = params.zkReplicas;
        this.zkStorage = params.zkStorage;

        this.kafkaReplicas = params.kafkaReplicas;
        this.kafkaStorage = params.kafkaStorage;

        this.resources = params.resources;
        this.before();
    }

    private Vertx vertx;
    private Kafka cluster;

    public void before() {
        this.cluster = new KafkaBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(CLUSTER_NAME)
                        .withNamespace(NAMESPACE)
                        .withLabels(Labels.userLabels(TestUtils.map("foo", "bar")).toMap())
                        .build())
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(kafkaReplicas)
                        .withStorage(kafkaStorage)
                        .withMetrics(singletonMap("foo", "bar"))
                        .withResources(resources)
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(zkReplicas)
                        .withStorage(zkStorage)
                        .withMetrics(singletonMap("foo", "bar"))
                    .endZookeeper()
                    .withNewTopicOperator()
                        .withImage("")
                    .endTopicOperator()
                .endSpec()
                .build();

        CustomResourceDefinition kafkaAssemblyCrd = Crds.kafka();

        mockClient = new MockKube().withCustomResourceDefinition(kafkaAssemblyCrd, Kafka.class, KafkaList.class, DoneableKafka.class)
                .withInitialInstances(Collections.singleton(cluster)).end().build();
    }

    @BeforeEach
    public void createVertxInstance() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    public void closeVertxInstace(VertxTestContext context) throws InterruptedException {
        context.completeNow();
        vertx.close();
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    private ResourceOperatorSupplier supplierWithMocks() {
        ZookeeperLeaderFinder leaderFinder = ResourceUtils.zookeeperLeaderFinder(vertx, mockClient);
        return new ResourceOperatorSupplier(vertx, mockClient, leaderFinder,
                ResourceUtils.adminClientProvider(),
                new PlatformFeaturesAvailability(true, kubernetesVersion), 2_000);
    }

    private KafkaAssemblyOperator createCluster(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        setFields(params);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        ResourceOperatorSupplier supplier = supplierWithMocks();
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        KafkaAssemblyOperator kco = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(), new PasswordGenerator(10, "a", "a"), supplier, config);

        LOGGER.info("Reconciling initially -> create");
        CountDownLatch createAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            StatefulSet kafkaSs = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get();
            kafkaSs.setStatus(new StatefulSetStatus());
            context.verify(() -> assertThat(kafkaSs, is(notNullValue())));
            context.verify(() -> assertThat(kafkaSs.getSpec().getTemplate().getMetadata().getAnnotations().get(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION), is("0")));
            context.verify(() -> assertThat(kafkaSs.getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0")));
            context.verify(() -> assertThat(kafkaSs.getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION), is("0")));
            StatefulSet zkSs = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME)).get();
            context.verify(() -> assertThat(zkSs.getSpec().getTemplate().getMetadata().getAnnotations().get(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION), is("0")));
            context.verify(() -> assertThat(zkSs.getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION), is("0")));
            context.verify(() -> assertThat(zkSs, is(notNullValue())));
            context.verify(() -> assertThat(mockClient.apps().deployments().inNamespace(NAMESPACE).withName(TopicOperator.topicOperatorName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.configMaps().inNamespace(NAMESPACE).withName(KafkaCluster.metricAndLogConfigsName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.configMaps().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperMetricAndLogConfigsName(CLUSTER_NAME)).get(), is(notNullValue())));
            assertResourceRequirements(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME));
            context.verify(() -> assertThat(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsCaKeySecretName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsCaCertSecretName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clusterCaCertSecretName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.secrets().inNamespace(NAMESPACE).withName(ZookeeperCluster.nodesSecretName(CLUSTER_NAME)).get(), is(notNullValue())));
            context.verify(() -> assertThat(mockClient.secrets().inNamespace(NAMESPACE).withName(TopicOperator.secretName(CLUSTER_NAME)).get(), is(notNullValue())));
            createAsync.countDown();
        });
        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }
        return kco;
    }

    /** Create a cluster from a Kafka Cluster CM */
    @ParameterizedTest
    @MethodSource("data")
    public void testCreateUpdate(Params params, VertxTestContext context) throws InterruptedException, TimeoutException, ExecutionException {
        KafkaAssemblyOperator kco = createCluster(params, context);
        LOGGER.info("Reconciling again -> update");
        CountDownLatch updateAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            updateAsync.countDown();
        });
        if (!updateAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }
        context.completeNow();
    }

    private void assertPvcs(VertxTestContext context, Set<String> expectedClaims) {
        context.verify(() -> assertThat(mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).list().getItems().stream()
                        .map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet()), is(expectedClaims)));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterWithoutKafkaSecrets(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        updateClusterWithoutSecrets(params, context,
                KafkaCluster.clientsCaKeySecretName(CLUSTER_NAME),
                KafkaCluster.clientsCaCertSecretName(CLUSTER_NAME),
                KafkaCluster.clusterCaCertSecretName(CLUSTER_NAME),
                KafkaCluster.brokersSecretName(CLUSTER_NAME),
                ZookeeperCluster.nodesSecretName(CLUSTER_NAME),
                TopicOperator.secretName(CLUSTER_NAME),
                ClusterOperator.secretName(CLUSTER_NAME));
    }

    private void updateClusterWithoutSecrets(Params params, VertxTestContext context, String... secrets) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaAssemblyOperator kco = createCluster(params, context);
        for (String secret: secrets) {
            mockClient.secrets().inNamespace(NAMESPACE).withName(secret).delete();
            assertThat("Expected secret " + secret + " to be not exist",
                    mockClient.secrets().inNamespace(NAMESPACE).withName(secret).get(), is(nullValue()));
        }
        LOGGER.info("Reconciling again -> update");
        Checkpoint updateAsync = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            for (String secret: secrets) {
                assertThat(
                        "Expected secret " + secret + " to have been recreated",
                        mockClient.secrets().inNamespace(NAMESPACE).withName(secret).get(), is(notNullValue()));
            }
            updateAsync.flag();
        });
    }

    /**
     * Test the operator re-creates services if they get deleted
     */
    private void updateClusterWithoutServices(Params params, VertxTestContext context, String... services) throws InterruptedException, ExecutionException, TimeoutException {
        setFields(params);
        KafkaAssemblyOperator kco = createCluster(params, context);
        for (String service: services) {
            mockClient.services().inNamespace(NAMESPACE).withName(service).delete();
            assertThat("Expected service " + service + " to be not exist",
                    mockClient.services().inNamespace(NAMESPACE).withName(service).get(), is(nullValue()));
        }
        LOGGER.info("Reconciling again -> update");
        Checkpoint updateAsync = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            for (String service: services) {
                assertThat(
                        "Expected service " + service + " to have been recreated",
                        mockClient.services().inNamespace(NAMESPACE).withName(service).get(), is(notNullValue()));
            }
            updateAsync.flag();
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterWithoutZkServices(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        updateClusterWithoutServices(params, context,
                ZookeeperCluster.serviceName(CLUSTER_NAME),
                ZookeeperCluster.headlessServiceName(CLUSTER_NAME));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterWithoutKafkaServices(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        updateClusterWithoutServices(params, context,
                KafkaCluster.serviceName(CLUSTER_NAME),
                KafkaCluster.headlessServiceName(CLUSTER_NAME));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterWithoutZkStatefulSet(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String statefulSet = ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME);
        updateClusterWithoutStatefulSet(params, context, statefulSet);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterWithoutKafkaStatefulSet(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String statefulSet = KafkaCluster.kafkaClusterName(CLUSTER_NAME);
        updateClusterWithoutStatefulSet(params, context, statefulSet);
    }

    private void updateClusterWithoutStatefulSet(Params params, VertxTestContext context, String statefulSet) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaAssemblyOperator kco = createCluster(params, context);

        mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).delete();
        assertThat("Expected ss " + statefulSet + " to be not exist",
                mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).get(), is(nullValue()));

        LOGGER.info("Reconciling again -> update");
        Checkpoint updateAsync = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));

            assertThat(
                    "Expected ss " + statefulSet + " to have been recreated",
                    mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).get(), is(notNullValue()));
            updateAsync.flag();
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaWithChangedPersistentVolume(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        setFields(params);
        assumeTrue(kafkaStorage instanceof PersistentClaimStorage);

        KafkaAssemblyOperator kco = createCluster(params, context);
        String originalStorageClass = Storage.storageClass(kafkaStorage);
        assertStorageClass(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalStorageClass);

        Checkpoint updateAsync = context.checkpoint();

        // Try to update the storage class
        String changedClass = originalStorageClass + "2";

        Kafka changedClusterCm = new KafkaBuilder(cluster).editSpec().editKafka()
                .withNewPersistentClaimStorage()
                    .withStorageClass(changedClass)
                    .withSize("123")
                .endPersistentClaimStorage().endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Updating with changed storage class");
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            // Check the storage class was not changed
            assertStorageClass(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalStorageClass);
            updateAsync.flag();
        });
    }

    private Resource<Kafka, DoneableKafka> kafkaAssembly(String namespace, String name) {
        CustomResourceDefinition crd = mockClient.customResourceDefinitions().withName(Kafka.CRD_NAME).get();
        return mockClient.customResources(crd, Kafka.class, KafkaList.class, DoneableKafka.class)
                .inNamespace(namespace).withName(name);
    }

    private void assertStorageClass(VertxTestContext context, String statefulSetName, String expectedClass) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.verify(() -> assertThat(statefulSet, is(notNullValue())));
        // Check the storage class is initially "foo"
        List<PersistentVolumeClaim> volumeClaimTemplates = statefulSet.getSpec().getVolumeClaimTemplates();
        context.verify(() -> assertThat(volumeClaimTemplates.isEmpty(), is(false)));
        context.verify(() -> assertThat(volumeClaimTemplates.get(0).getSpec().getStorageClassName(), is(expectedClass)));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaWithChangedStorageType(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaAssemblyOperator kco = createCluster(params, context);
        List<PersistentVolumeClaim> originalPVCs = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getVolumeClaimTemplates();
        List<Volume> originalVolumes = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getTemplate().getSpec().getVolumes();
        List<Container> originalInitContainers = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getTemplate().getSpec().getInitContainers();

        Checkpoint updateAsync = context.checkpoint();

        // Try to update the storage type
        Kafka changedClusterCm = null;
        if (kafkaStorage instanceof EphemeralStorage) {
            changedClusterCm = new KafkaBuilder(cluster).editSpec().editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("123")
                    .endPersistentClaimStorage().endKafka().endSpec().build();
        } else if (kafkaStorage instanceof PersistentClaimStorage) {
            changedClusterCm = new KafkaBuilder(cluster).editSpec().editKafka()
                    .withNewEphemeralStorage()
                    .endEphemeralStorage().endKafka().endSpec().build();
        } else {
            fail();
        }
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Updating with changed storage type");
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            // Check the Volumes and PVCs were not changed
            assertPVCs(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalPVCs);
            assertVolumes(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalVolumes);
            assertInitContainers(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalInitContainers);
            updateAsync.flag();
        });
    }

    private void assertPVCs(VertxTestContext context, String statefulSetName, List<PersistentVolumeClaim> originalPVCs) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.verify(() -> assertThat(statefulSet, is(notNullValue())));
        List<PersistentVolumeClaim> pvcs = statefulSet.getSpec().getVolumeClaimTemplates();
        context.verify(() -> assertThat(originalPVCs.size(), is(pvcs.size())));
        context.verify(() -> assertThat(originalPVCs, is(pvcs)));
    }

    private void assertVolumes(VertxTestContext context, String statefulSetName, List<Volume> originalVolumes) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.verify(() -> assertThat(statefulSet, is(notNullValue())));
        List<Volume> volumes = statefulSet.getSpec().getTemplate().getSpec().getVolumes();
        context.verify(() -> assertThat(originalVolumes.size(), is(volumes.size())));
        context.verify(() -> assertThat(originalVolumes, is(volumes)));
    }

    private void assertInitContainers(VertxTestContext context, String statefulSetName, List<Container> originalInitContainers) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.verify(() -> assertThat(statefulSet, is(notNullValue())));
        List<Container> initContainers = statefulSet.getSpec().getTemplate().getSpec().getInitContainers();
        context.verify(() -> assertThat(originalInitContainers.size(), is(initContainers.size())));
        context.verify(() -> assertThat(originalInitContainers, is(initContainers)));
    }

    private void assertResourceRequirements(VertxTestContext context, String statefulSetName) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.verify(() -> assertThat(statefulSet, is(notNullValue())));
        ResourceRequirements requirements = statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getResources();
        if (resources != null && resources.getRequests() != null) {
            context.verify(() -> assertThat(requirements.getRequests().get("cpu").getAmount(), is(resources.getRequests().get("cpu").getAmount())));
            context.verify(() -> assertThat(requirements.getRequests().get("memory").getAmount(), is(resources.getRequests().get("memory").getAmount())));
        }
        if (resources != null && resources.getLimits() != null) {
            context.verify(() -> assertThat(requirements.getLimits().get("cpu").getAmount(), is(resources.getLimits().get("cpu").getAmount())));
            context.verify(() -> assertThat(requirements.getLimits().get("memory").getAmount(), is(resources.getLimits().get("memory").getAmount())));
        }
    }

    /** Test that we can change the deleteClaim flag, and that it's honoured */
    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaWithChangedDeleteClaim(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        setFields(params);
        assumeTrue(kafkaStorage instanceof PersistentClaimStorage);

        KafkaAssemblyOperator kco = createCluster(params, context);

        Map<String, String> labels = new HashMap<>();
        labels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        labels.put(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);
        labels.put(Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(CLUSTER_NAME));

        Set<String> kafkaPvcs = mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).withLabels(labels).list().getItems()
                .stream()
                .map(pvc -> pvc.getMetadata().getName())
                .collect(Collectors.toSet());

        labels.put(Labels.STRIMZI_NAME_LABEL, ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME));

        Set<String> zkPvcs = mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).withLabels(labels).list().getItems()
                .stream()
                .map(pvc -> pvc.getMetadata().getName())
                .collect(Collectors.toSet());

        Set<String> allPvcs = new HashSet<>();
        allPvcs.addAll(kafkaPvcs);
        allPvcs.addAll(zkPvcs);

        boolean originalKafkaDeleteClaim = deleteClaim(kafkaStorage);

        // Try to update the storage class
        Kafka changedClusterCm = new KafkaBuilder(cluster).editSpec().editKafka()
                .withNewPersistentClaimStorage()
                .withSize("123")
                .withStorageClass("foo")
                .withDeleteClaim(!originalKafkaDeleteClaim)
                .endPersistentClaimStorage().endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Updating with changed delete claim");
        CountDownLatch updateAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            updateAsync.countDown();
        });
        updateAsync.await();

        // check that the new delete-claim annotation is on the PVCs
        for (String pvcName: kafkaPvcs) {
            assertThat(Boolean.valueOf(mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).withName(pvcName).get()
                    .getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM)), is(!originalKafkaDeleteClaim));
        }


        LOGGER.info("Reconciling again -> delete");
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).delete();
        Checkpoint deleteAsync = context.checkpoint();
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            deleteAsync.flag();
        });
        context.completeNow();
    }

    /** Create a cluster from a Kafka Cluster CM */
    @ParameterizedTest
    @MethodSource("data")
    public void testKafkaScaleDown(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        setFields(params);
        if (kafkaReplicas <= 1) {
            LOGGER.info("Skipping scale down test because there's only 1 broker");
            context.completeNow();
            return;
        }
        KafkaAssemblyOperator kco = createCluster(params, context);
        Checkpoint updateAsync = context.checkpoint();

        int brokersInternalCerts = mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get().getData().size();

        int newScale = kafkaReplicas - 1;
        String deletedPod = KafkaCluster.kafkaPodName(CLUSTER_NAME, newScale);
        context.verify(() -> assertThat(mockClient.pods().inNamespace(NAMESPACE).withName(deletedPod).get(), is(notNullValue())));

        Kafka changedClusterCm = new KafkaBuilder(cluster).editSpec().editKafka()
                .withReplicas(newScale).endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Scaling down to {} Kafka pods", newScale);
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            context.verify(() -> assertThat(mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getReplicas(), is(newScale)));
            context.verify(() -> assertThat("Expected pod " + deletedPod + " to have been deleted",
                    mockClient.pods().inNamespace(NAMESPACE).withName(deletedPod).get(),
                    is(nullValue())));

            // removing one pod, the related private and public keys should not be in the Secrets
            context.verify(() -> assertThat(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get().getData().size(),
                    is(brokersInternalCerts - 2)));

            // TODO assert no rolling update
            updateAsync.flag();
            context.completeNow();
        });
    }

    /** Create a cluster from a Kafka Cluster CM */
    @ParameterizedTest
    @MethodSource("data")
    public void testKafkaScaleUp(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaAssemblyOperator kco = createCluster(params, context);
        Checkpoint updateAsync = context.checkpoint();

        int brokersInternalCerts = mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get().getData().size();

        int newScale = kafkaReplicas + 1;
        String newPod = KafkaCluster.kafkaPodName(CLUSTER_NAME, kafkaReplicas);
        context.verify(() -> assertThat(mockClient.pods().inNamespace(NAMESPACE).withName(newPod).get(), is(nullValue())));

        Kafka changedClusterCm = new KafkaBuilder(cluster).editSpec().editKafka()
                .withReplicas(newScale).endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Scaling up to {} Kafka pods", newScale);
        kco.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            context.verify(() -> assertThat(mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getReplicas(),
                    is(newScale)));
            context.verify(() -> assertThat("Expected pod " + newPod + " to have been created",
                    mockClient.pods().inNamespace(NAMESPACE).withName(newPod).get(),
                    is(notNullValue())));

            // adding one pod, the related private and public keys should be added to the Secrets
            context.verify(() -> assertThat(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get().getData().size(),
                    is(brokersInternalCerts + 2)));

            // TODO assert no rolling update
            updateAsync.flag();
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testResumePartialRoll(Params params, VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaAssemblyOperator kco = createCluster(params, context);
        context.completeNow();
    }
}
