/*
 * Copyright 2018, Strimzi authors.
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
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.storage.Storage.deleteClaim;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class KafkaAssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaAssemblyOperatorMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef"),
            singletonMap("2.0.0", "strimzi/kafka:latest-kafka-2.0.0"), emptyMap(), emptyMap(), emptyMap()) { };

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_9;

    private final int zkReplicas;
    private final SingleVolumeStorage zkStorage;

    private final int kafkaReplicas;
    private final Storage kafkaStorage;
    private final ResourceRequirements resources;
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

    @Parameterized.Parameters(name = "{0}")
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

    public KafkaAssemblyOperatorMockTest(KafkaAssemblyOperatorMockTest.Params params) {
        this.zkReplicas = params.zkReplicas;
        this.zkStorage = params.zkStorage;

        this.kafkaReplicas = params.kafkaReplicas;
        this.kafkaStorage = params.kafkaStorage;

        this.resources = params.resources;
    }

    private Vertx vertx;
    private Kafka cluster;

    @Before
    public void before() throws MalformedURLException {
        this.vertx = Vertx.vertx();
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

    @After
    public void after() {
        this.vertx.close();
    }

    @AfterClass
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    private ResourceOperatorSupplier supplierWithMocks() {
        ZookeeperLeaderFinder leaderFinder = ResourceUtils.zookeeperLeaderFinder(vertx, mockClient);
        return new ResourceOperatorSupplier(vertx, mockClient, leaderFinder, new PlatformFeaturesAvailability(true, kubernetesVersion), 2_000);
    }

    private KafkaAssemblyOperator createCluster(TestContext context) {
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        ResourceOperatorSupplier supplier = supplierWithMocks();
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        KafkaAssemblyOperator kco = new KafkaAssemblyOperator(vertx, pfa, new MockCertManager(), supplier, config);

        LOGGER.info("Reconciling initially -> create");
        Async createAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            StatefulSet kafkaSs = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get();
            kafkaSs.setStatus(new StatefulSetStatus());
            context.assertNotNull(kafkaSs);
            context.assertEquals("0", kafkaSs.getSpec().getTemplate().getMetadata().getAnnotations().get(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION));
            context.assertEquals("0", kafkaSs.getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION));
            context.assertEquals("0", kafkaSs.getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION));
            StatefulSet zkSs = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME)).get();
            context.assertEquals("0", zkSs.getSpec().getTemplate().getMetadata().getAnnotations().get(StatefulSetOperator.ANNO_STRIMZI_IO_GENERATION));
            context.assertEquals("0", zkSs.getSpec().getTemplate().getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION));
            context.assertNotNull(zkSs);
            context.assertNotNull(mockClient.apps().deployments().inNamespace(NAMESPACE).withName(TopicOperator.topicOperatorName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.configMaps().inNamespace(NAMESPACE).withName(KafkaCluster.metricAndLogConfigsName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.configMaps().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperMetricAndLogConfigsName(CLUSTER_NAME)).get());
            assertResourceRequirements(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME));
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsCaKeySecretName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsCaCertSecretName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clusterCaCertSecretName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(ZookeeperCluster.nodesSecretName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(TopicOperator.secretName(CLUSTER_NAME)).get());
            createAsync.complete();
        });
        createAsync.await();
        return kco;
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testCreateUpdate(TestContext context) {
        KafkaAssemblyOperator kco = createCluster(context);
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync.complete();
        });
        updateAsync.await();
    }

    private void assertPvcs(TestContext context, Set<String> expectedClaims) {
        context.assertEquals(expectedClaims,
                mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).list().getItems().stream()
                        .map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet()));
    }

    @Test
    public void testUpdateClusterWithoutKafkaSecrets(TestContext context) {
        updateClusterWithoutSecrets(context,
                KafkaCluster.clientsCaKeySecretName(CLUSTER_NAME),
                KafkaCluster.clientsCaCertSecretName(CLUSTER_NAME),
                KafkaCluster.clusterCaCertSecretName(CLUSTER_NAME),
                KafkaCluster.brokersSecretName(CLUSTER_NAME),
                ZookeeperCluster.nodesSecretName(CLUSTER_NAME),
                TopicOperator.secretName(CLUSTER_NAME),
                ClusterOperator.secretName(CLUSTER_NAME));
    }

    private void updateClusterWithoutSecrets(TestContext context, String... secrets) {

        KafkaAssemblyOperator kco = createCluster(context);
        for (String secret: secrets) {
            mockClient.secrets().inNamespace(NAMESPACE).withName(secret).delete();
            assertNull("Expected secret " + secret + " to be not exist",
                    mockClient.secrets().inNamespace(NAMESPACE).withName(secret).get());
        }
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (String secret: secrets) {
                assertNotNull(
                        "Expected secret " + secret + " to have been recreated",
                        mockClient.secrets().inNamespace(NAMESPACE).withName(secret).get());
            }
            updateAsync.complete();
        });
    }

    /**
     * Test the operator re-creates services if they get deleted
     */
    private void updateClusterWithoutServices(TestContext context, String... services) {

        KafkaAssemblyOperator kco = createCluster(context);
        for (String service: services) {
            mockClient.services().inNamespace(NAMESPACE).withName(service).delete();
            assertNull("Expected service " + service + " to be not exist",
                    mockClient.services().inNamespace(NAMESPACE).withName(service).get());
        }
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (String service: services) {
                assertNotNull(
                        "Expected service " + service + " to have been recreated",
                        mockClient.services().inNamespace(NAMESPACE).withName(service).get());
            }
            updateAsync.complete();
        });
    }

    @Test
    public void testUpdateClusterWithoutZkServices(TestContext context) {
        updateClusterWithoutServices(context,
                ZookeeperCluster.serviceName(CLUSTER_NAME),
                ZookeeperCluster.headlessServiceName(CLUSTER_NAME));
    }

    @Test
    public void testUpdateClusterWithoutKafkaServices(TestContext context) {
        updateClusterWithoutServices(context,
                KafkaCluster.serviceName(CLUSTER_NAME),
                KafkaCluster.headlessServiceName(CLUSTER_NAME));
    }

    @Test
    public void testUpdateClusterWithoutZkStatefulSet(TestContext context) {
        String statefulSet = ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME);
        updateClusterWithoutStatefulSet(context, statefulSet);
    }

    @Test
    public void testUpdateClusterWithoutKafkaStatefulSet(TestContext context) {
        String statefulSet = KafkaCluster.kafkaClusterName(CLUSTER_NAME);
        updateClusterWithoutStatefulSet(context, statefulSet);
    }

    private void updateClusterWithoutStatefulSet(TestContext context, String statefulSet) {
        KafkaAssemblyOperator kco = createCluster(context);

        mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).delete();
        assertNull("Expected ss " + statefulSet + " to be not exist",
                mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).get());

        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());

            assertNotNull(
                    "Expected ss " + statefulSet + " to have been recreated",
                    mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).get());

            updateAsync.complete();
        });
    }

    @Test
    public void testUpdateKafkaWithChangedPersistentVolume(TestContext context) {
        Assume.assumeTrue(kafkaStorage instanceof PersistentClaimStorage);

        KafkaAssemblyOperator kco = createCluster(context);
        String originalStorageClass = Storage.storageClass(kafkaStorage);
        assertStorageClass(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalStorageClass);

        Async updateAsync = context.async();

        // Try to update the storage class
        String changedClass = originalStorageClass + "2";

        Kafka changedClusterCm = new KafkaBuilder(cluster).editSpec().editKafka()
                .withNewPersistentClaimStorage()
                    .withStorageClass(changedClass)
                    .withSize("123")
                .endPersistentClaimStorage().endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Updating with changed storage class");
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            // Check the storage class was not changed
            assertStorageClass(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalStorageClass);
            updateAsync.complete();
        });
    }

    private Resource<Kafka, DoneableKafka> kafkaAssembly(String namespace, String name) {
        CustomResourceDefinition crd = mockClient.customResourceDefinitions().withName(Kafka.CRD_NAME).get();
        return mockClient.customResources(crd, Kafka.class, KafkaList.class, DoneableKafka.class)
                .inNamespace(namespace).withName(name);
    }

    private void assertStorageClass(TestContext context, String statefulSetName, String expectedClass) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.assertNotNull(statefulSet);
        // Check the storage class is initially "foo"
        List<PersistentVolumeClaim> volumeClaimTemplates = statefulSet.getSpec().getVolumeClaimTemplates();
        context.assertFalse(volumeClaimTemplates.isEmpty());
        context.assertEquals(expectedClass, volumeClaimTemplates.get(0).getSpec().getStorageClassName());
    }

    @Test
    public void testUpdateKafkaWithChangedStorageType(TestContext context) {
        KafkaAssemblyOperator kco = createCluster(context);
        List<PersistentVolumeClaim> originalPVCs = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getVolumeClaimTemplates();
        List<Volume> originalVolumes = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getTemplate().getSpec().getVolumes();
        List<Container> originalInitContainers = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getTemplate().getSpec().getInitContainers();

        Async updateAsync = context.async();

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
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            // Check the Volumes and PVCs were not changed
            assertPVCs(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalPVCs);
            assertVolumes(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalVolumes);
            assertInitContainers(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalInitContainers);
            updateAsync.complete();
        });
    }

    private void assertPVCs(TestContext context, String statefulSetName, List<PersistentVolumeClaim> originalPVCs) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.assertNotNull(statefulSet);
        List<PersistentVolumeClaim> pvcs = statefulSet.getSpec().getVolumeClaimTemplates();
        context.assertEquals(pvcs.size(), originalPVCs.size());
        context.assertEquals(pvcs, originalPVCs);
    }

    private void assertVolumes(TestContext context, String statefulSetName, List<Volume> originalVolumes) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.assertNotNull(statefulSet);
        List<Volume> volumes = statefulSet.getSpec().getTemplate().getSpec().getVolumes();
        context.assertEquals(volumes.size(), originalVolumes.size());
        context.assertEquals(volumes, originalVolumes);
    }

    private void assertInitContainers(TestContext context, String statefulSetName, List<Container> originalInitContainers) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.assertNotNull(statefulSet);
        List<Container> initContainers = statefulSet.getSpec().getTemplate().getSpec().getInitContainers();
        context.assertEquals(initContainers.size(), originalInitContainers.size());
        context.assertEquals(initContainers, originalInitContainers);
    }

    private void assertResourceRequirements(TestContext context, String statefulSetName) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.assertNotNull(statefulSet);
        ResourceRequirements requirements = statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getResources();
        if (resources != null && resources.getRequests() != null) {
            context.assertEquals(resources.getRequests().get("cpu").getAmount(), requirements.getRequests().get("cpu").getAmount());
            context.assertEquals(resources.getRequests().get("memory").getAmount(), requirements.getRequests().get("memory").getAmount());
        }
        if (resources != null && resources.getLimits() != null) {
            context.assertEquals(resources.getLimits().get("cpu").getAmount(), requirements.getLimits().get("cpu").getAmount());
            context.assertEquals(resources.getLimits().get("memory").getAmount(), requirements.getLimits().get("memory").getAmount());
        }
    }

    /** Test that we can change the deleteClaim flag, and that it's honoured */
    @Test
    public void testUpdateKafkaWithChangedDeleteClaim(TestContext context) {
        Assume.assumeTrue(kafkaStorage instanceof PersistentClaimStorage);

        KafkaAssemblyOperator kco = createCluster(context);

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
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync.complete();
        });
        updateAsync.await();

        // check that the new delete-claim annotation is on the PVCs
        for (String pvcName: kafkaPvcs) {
            assertEquals(!originalKafkaDeleteClaim,
                Boolean.valueOf(mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).withName(pvcName).get()
                    .getMetadata().getAnnotations().get(AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM)));
        }


        LOGGER.info("Reconciling again -> delete");
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).delete();
        Async deleteAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            deleteAsync.complete();
        });
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testKafkaScaleDown(TestContext context) {
        if (kafkaReplicas <= 1) {
            LOGGER.info("Skipping scale down test because there's only 1 broker");
            return;
        }
        KafkaAssemblyOperator kco = createCluster(context);
        Async updateAsync = context.async();

        int brokersInternalCerts = mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get().getData().size();

        int newScale = kafkaReplicas - 1;
        String deletedPod = KafkaCluster.kafkaPodName(CLUSTER_NAME, newScale);
        context.assertNotNull(mockClient.pods().inNamespace(NAMESPACE).withName(deletedPod).get());

        Kafka changedClusterCm = new KafkaBuilder(cluster).editSpec().editKafka()
                .withReplicas(newScale).endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Scaling down to {} Kafka pods", newScale);
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            context.assertEquals(newScale,
                    mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getReplicas());
            context.assertNull(mockClient.pods().inNamespace(NAMESPACE).withName(deletedPod).get(),
                    "Expected pod " + deletedPod + " to have been deleted");

            // removing one pod, the related private and public keys should not be in the Secrets
            context.assertEquals(brokersInternalCerts - 2,
                    mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get().getData().size());

            // TODO assert no rolling update
            updateAsync.complete();
        });
        updateAsync.await();
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testKafkaScaleUp(TestContext context) {

        KafkaAssemblyOperator kco = createCluster(context);
        Async updateAsync = context.async();

        int brokersInternalCerts = mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get().getData().size();

        int newScale = kafkaReplicas + 1;
        String newPod = KafkaCluster.kafkaPodName(CLUSTER_NAME, kafkaReplicas);
        context.assertNull(mockClient.pods().inNamespace(NAMESPACE).withName(newPod).get());

        Kafka changedClusterCm = new KafkaBuilder(cluster).editSpec().editKafka()
                .withReplicas(newScale).endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Scaling up to {} Kafka pods", newScale);
        kco.reconcileAssembly(new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            context.assertEquals(newScale,
                    mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get().getSpec().getReplicas());
            context.assertNotNull(mockClient.pods().inNamespace(NAMESPACE).withName(newPod).get(),
                    "Expected pod " + newPod + " to have been created");

            // adding one pod, the related private and public keys should be added to the Secrets
            context.assertEquals(brokersInternalCerts + 2,
                    mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get().getData().size());

            // TODO assert no rolling update
            updateAsync.complete();
        });
        updateAsync.await();
    }

    @Test
    public void testResumePartialRoll(TestContext context) {


        KafkaAssemblyOperator kco = createCluster(context);
    }

}
