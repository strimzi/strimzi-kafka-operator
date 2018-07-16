/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.DoneableKafkaAssembly;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.KafkaAssemblyBuilder;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.api.kafka.model.ResourcesBuilder;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.Storage.deleteClaim;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class KafkaAssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaAssemblyOperatorMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private final int zkReplicas;
    private final Storage zkStorage;

    private final int kafkaReplicas;
    private final Storage kafkaStorage;
    private final Resources resources;
    private KubernetesClient mockClient;

    public static class Params {
        private final int zkReplicas;
        private final Storage zkStorage;

        private final int kafkaReplicas;
        private final Storage kafkaStorage;
        private Resources resources;

        public Params(int zkReplicas,
                      Storage zkStorage, int kafkaReplicas,
                      Storage kafkaStorage,
                      Resources resources) {
            this.kafkaReplicas = kafkaReplicas;
            this.kafkaStorage = kafkaStorage;
            this.zkReplicas = zkReplicas;
            this.zkStorage = zkStorage;
            this.resources = resources;
        }

        public String toString() {
            return "zkReplicas=" + zkReplicas +
                    ",zkStorage=" + kafkaStorage +
                    ",kafkaReplicas=" + kafkaReplicas +
                    ",kafkaStorage=" + kafkaStorage +
                    ",resources=" + resources;
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<KafkaAssemblyOperatorMockTest.Params> data() {
        int[] replicas = {1, 3};
        io.strimzi.api.kafka.model.Storage[] storageConfigs = {
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
        Resources[] resources = {
            new ResourcesBuilder()
            .withNewLimits()
                .withMilliCpu("5000")
                .withMemory("5000")
            .endLimits()
            .withNewRequests()
                .withMilliCpu("5000")
                .withMemory("5000")
            .endRequests()
            .build()
        };
        List<KafkaAssemblyOperatorMockTest.Params> result = new ArrayList();

        for (int zkReplica : replicas) {
            for (Storage zkStorage : storageConfigs) {
                for (int kafkaReplica : replicas) {
                    for (Storage kafkaStorage : storageConfigs) {
                        for (Resources resource : resources) {
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
    private KafkaAssembly cluster;

    @Before
    public void before() {
        this.vertx = Vertx.vertx();
        this.cluster = new KafkaAssemblyBuilder()
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

        CustomResourceDefinition kafkaAssemblyCrd = TestUtils.fromYamlFile(TestUtils.KAFKA_CRD, CustomResourceDefinition.class);

        mockClient = new MockKube().withCustomResourceDefinition(kafkaAssemblyCrd, KafkaAssembly.class, KafkaAssemblyList.class, DoneableKafkaAssembly.class)
                .withInitialInstances(Collections.singleton(cluster)).end().build();
    }

    @After
    public void after() {
        this.vertx.close();
    }

    private ResourceOperatorSupplier supplierWithMocks() {
        return new ResourceOperatorSupplier(vertx, mockClient, 2_000);
    }

    private KafkaAssemblyOperator createCluster(TestContext context) {
        ResourceOperatorSupplier supplier = supplierWithMocks();
        KafkaAssemblyOperator kco = new KafkaAssemblyOperator(vertx, true, 2_000,
                new MockCertManager(), supplier);

        LOGGER.info("Reconciling initially -> create");
        Async createAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            StatefulSet kafkaSs = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaClusterName(CLUSTER_NAME)).get();
            context.assertNotNull(kafkaSs);
            context.assertEquals("0", kafkaSs.getSpec().getTemplate().getMetadata().getAnnotations().get(StatefulSetOperator.ANNOTATION_GENERATION));
            StatefulSet zkSs = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME)).get();
            context.assertEquals("0", zkSs.getSpec().getTemplate().getMetadata().getAnnotations().get(StatefulSetOperator.ANNOTATION_GENERATION));
            context.assertNotNull(zkSs);
            context.assertNotNull(mockClient.extensions().deployments().inNamespace(NAMESPACE).withName(TopicOperator.topicOperatorName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.configMaps().inNamespace(NAMESPACE).withName(KafkaCluster.metricAndLogConfigsName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.configMaps().inNamespace(NAMESPACE).withName(ZookeeperCluster.zookeeperMetricAndLogConfigsName(CLUSTER_NAME)).get());
            assertResourceRequirements(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME));
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsCASecretName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsPublicKeyName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clusterPublicKeyName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(ZookeeperCluster.nodesSecretName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(AbstractModel.getClusterCaName(CLUSTER_NAME)).get());
            context.assertNotNull(mockClient.secrets().inNamespace(NAMESPACE).withName(TopicOperator.secretName(CLUSTER_NAME)).get());
            createAsync.complete();
        });
        createAsync.await();
        return kco;
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testCreateUpdateDelete(TestContext context) {
        Set<String> expectedClaims = resilientPvcs();

        KafkaAssemblyOperator kco = createCluster(context);
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync.complete();
        });
        updateAsync.await();
        LOGGER.info("Reconciling again -> delete");
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).delete();
        Async deleteAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            assertPvcs(context, expectedClaims);
            context.assertNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsCASecretName(CLUSTER_NAME)).get());
            context.assertNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clientsPublicKeyName(CLUSTER_NAME)).get());
            context.assertNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.clusterPublicKeyName(CLUSTER_NAME)).get());
            context.assertNull(mockClient.secrets().inNamespace(NAMESPACE).withName(KafkaCluster.brokersSecretName(CLUSTER_NAME)).get());
            context.assertNull(mockClient.secrets().inNamespace(NAMESPACE).withName(ZookeeperCluster.nodesSecretName(CLUSTER_NAME)).get());
            context.assertNull(mockClient.secrets().inNamespace(NAMESPACE).withName(TopicOperator.secretName(CLUSTER_NAME)).get());
            deleteAsync.complete();
        });
    }

    private void assertPvcs(TestContext context, Set<String> expectedClaims) {
        context.assertEquals(expectedClaims,
                mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).list().getItems().stream()
                        .map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet()));
    }

    /**
     * Create PVCs for appropriate for the kafka and ZK storage test parameters,
     * return the names of the PVCs which shouldn't be deleted according to the deleteClaim config
     */
    private Set<String> resilientPvcs() {
        Set<String> expectedClaims = new HashSet<>();
        Set<String> kafkaPvcNames = createPvcs(kafkaStorage,
                kafkaReplicas, podId -> KafkaCluster.getPersistentVolumeClaimName(KafkaCluster.kafkaClusterName(CLUSTER_NAME), podId)
        );
        if (!deleteClaim(kafkaStorage)) {
            expectedClaims.addAll(kafkaPvcNames);
        }
        Set<String> zkPvcNames = createPvcs(zkStorage,
                zkReplicas, podId -> ZookeeperCluster.getPersistentVolumeClaimName(ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME), podId)
        );
        if (!deleteClaim(zkStorage)) {
            expectedClaims.addAll(zkPvcNames);
        }
        return expectedClaims;
    }

    /**
     * Create PVCs in the mockClient
     * according to the given storage, number of replicas and naming scheme,
     * return the names of the PVCs created
     */
    private Set<String> createPvcs(Storage storage, int replicas, Function<Integer, String> pvcNameFn) {
        Set<String> expectedClaims = new HashSet<>();
        if (storage instanceof PersistentClaimStorage) {
            for (int i = 0; i < replicas; i++) {
                String pvcName = pvcNameFn.apply(i);
                mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).withName(pvcName).create(
                        new PersistentVolumeClaimBuilder().withNewMetadata()
                                .withNamespace(NAMESPACE)
                                .withName(pvcName)
                                .endMetadata()
                                .build());
                expectedClaims.add(pvcName);
            }
        }
        return expectedClaims;
    }

    @Test
    public void testUpdateClusterWithoutKafkaSecrets(TestContext context) {
        updateClusterWithoutSecrets(context,
                KafkaCluster.clientsCASecretName(CLUSTER_NAME),
                KafkaCluster.clientsPublicKeyName(CLUSTER_NAME),
                KafkaCluster.clusterPublicKeyName(CLUSTER_NAME),
                KafkaCluster.brokersSecretName(CLUSTER_NAME),
                ZookeeperCluster.nodesSecretName(CLUSTER_NAME),
                TopicOperator.secretName(CLUSTER_NAME));
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
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
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
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
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
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());

            assertNotNull(
                    "Expected ss " + statefulSet + " to have been recreated",
                    mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSet).get());

            updateAsync.complete();
        });
    }

    private void deleteClusterWithoutServices(TestContext context, String... services) {
        KafkaAssemblyOperator kco = createCluster(context);
        Set<String> ssNames = mockClient.apps().statefulSets().inNamespace(NAMESPACE).list().getItems().stream().map(r -> r.getMetadata().getName()).collect(Collectors.toSet());
        Set<String> deploymentNames = mockClient.extensions().deployments().inNamespace(NAMESPACE).list().getItems().stream().map(r -> r.getMetadata().getName()).collect(Collectors.toSet());
        Set<String> cmNames = new HashSet<>(mockClient.configMaps().inNamespace(NAMESPACE).list().getItems().stream().map(r -> r.getMetadata().getName()).collect(Collectors.toSet()));
        cmNames.remove(CLUSTER_NAME);
        Set<String> pvcNames = mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).list().getItems().stream().map(r -> r.getMetadata().getName()).collect(Collectors.toSet());
        for (String service: services) {
            mockClient.services().inNamespace(NAMESPACE).withName(service).delete();
            assertNull("Expected service " + service + " to be not exist",
                    mockClient.services().inNamespace(NAMESPACE).withName(service).get());
        }
        LOGGER.info("Deleting");
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).delete();
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (String service: services) {
                assertNull(
                        "Expected service " + service + " to still not exist",
                        mockClient.services().inNamespace(NAMESPACE).withName(service).get());
            }
            for (String ss: ssNames) {
                assertNull(
                        "Expected ss " + ss + " to still not exist",
                        mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ss).get());
            }
            // assert other resources do not exist either
            for (String r: ssNames) {
                assertNull(
                        "Expected r " + r + " to still not exist",
                        mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(r).get());
            }
            for (String r: deploymentNames) {
                assertNull(
                        "Expected r " + r + " to still not exist",
                        mockClient.extensions().deployments().inNamespace(NAMESPACE).withName(r).get());
            }
            for (String r: cmNames) {
                assertNull(
                        "Expected r " + r + " to still not exist",
                        mockClient.configMaps().inNamespace(NAMESPACE).withName(r).get());
            }
            for (String r: pvcNames) {
                assertNull(
                        "Expected r " + r + " to still not exist",
                        mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).withName(r).get());
            }
            updateAsync.complete();
        });
    }

    @Test
    public void testDeleteClusterWithoutZkServices(TestContext context) {
        deleteClusterWithoutServices(context,
                ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME),
                ZookeeperCluster.headlessServiceName(CLUSTER_NAME));
    }

    @Test
    public void testDeleteClusterWithoutKafkaServices(TestContext context) {
        deleteClusterWithoutServices(context,
                KafkaCluster.kafkaClusterName(CLUSTER_NAME),
                KafkaCluster.headlessServiceName(CLUSTER_NAME));
    }

    private void deleteClusterWithoutStatefulSet(TestContext context, String... statefulSets) {

        KafkaAssemblyOperator kco = createCluster(context);
        Set<String> ssNames = mockClient.apps().statefulSets().inNamespace(NAMESPACE).list().getItems().stream().map(r -> r.getMetadata().getName()).collect(Collectors.toSet());
        Set<String> serviceNames = mockClient.services().inNamespace(NAMESPACE).list().getItems().stream().map(r -> r.getMetadata().getName()).collect(Collectors.toSet());
        Set<String> deploymentNames = mockClient.extensions().deployments().inNamespace(NAMESPACE).list().getItems().stream().map(r -> r.getMetadata().getName()).collect(Collectors.toSet());
        Set<String> cmNames = new HashSet<>(mockClient.configMaps().inNamespace(NAMESPACE).list().getItems().stream().map(r -> r.getMetadata().getName()).collect(Collectors.toSet()));
        cmNames.remove(CLUSTER_NAME);
        Set<String> pvcNames = mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).list().getItems().stream().map(r -> r.getMetadata().getName()).collect(Collectors.toSet());

        for (String ss: statefulSets) {
            mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ss).delete();
            assertNull("Expected ss " + ss + " to be not exist",
                    mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ss).get());
        }
        LOGGER.info("Deleting");
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).delete();
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (String ss: statefulSets) {
                assertNull(
                        "Expected ss " + ss + " to still not exist",
                        mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(ss).get());
            }
            // assert other resources do not exist either
            for (String r: ssNames) {
                assertNull(
                        "Expected r " + r + " to still not exist",
                        mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(r).get());
            }
            for (String r: serviceNames) {
                assertNull(
                        "Expected r " + r + " to still not exist",
                        mockClient.services().inNamespace(NAMESPACE).withName(r).get());
            }
            for (String r: deploymentNames) {
                assertNull(
                        "Expected r " + r + " to still not exist",
                        mockClient.extensions().deployments().inNamespace(NAMESPACE).withName(r).get());
            }
            for (String r: cmNames) {
                assertNull(
                        "Expected r " + r + " to still not exist",
                        mockClient.configMaps().inNamespace(NAMESPACE).withName(r).get());
            }
            for (String r: pvcNames) {
                assertNull(
                        "Expected r " + r + " to still not exist",
                        mockClient.persistentVolumeClaims().inNamespace(NAMESPACE).withName(r).get());
            }
            updateAsync.complete();
        });
    }

    @Test
    public void testDeleteClusterWithoutZkStatefulSet(TestContext context) {
        deleteClusterWithoutStatefulSet(context,
                ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME));
    }

    @Test
    public void testDeleteClusterWithoutKafkaStatefulSet(TestContext context) {
        deleteClusterWithoutStatefulSet(context,
                KafkaCluster.kafkaClusterName(CLUSTER_NAME));
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

        KafkaAssembly changedClusterCm = new KafkaAssemblyBuilder(cluster).editSpec().editKafka()
                .withNewPersistentClaimStorageStorage()
                    .withStorageClass(changedClass)
                .endPersistentClaimStorageStorage().endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Updating with changed storage class");
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            // Check the storage class was not changed
            assertStorageClass(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalStorageClass);
            updateAsync.complete();
        });
    }

    private Resource<KafkaAssembly, DoneableKafkaAssembly> kafkaAssembly(String namespace, String name) {
        CustomResourceDefinition crd = mockClient.customResourceDefinitions().withName(KafkaAssembly.CRD_NAME).get();
        return mockClient.customResources(crd, KafkaAssembly.class, KafkaAssemblyList.class, DoneableKafkaAssembly.class)
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
        KafkaAssembly changedClusterCm = null;
        if (kafkaStorage instanceof EphemeralStorage) {
            changedClusterCm = new KafkaAssemblyBuilder(cluster).editSpec().editKafka()
                    .withNewPersistentClaimStorageStorage()
                        .withSize("123")
                    .endPersistentClaimStorageStorage().endKafka().endSpec().build();
        } else if (kafkaStorage instanceof PersistentClaimStorage) {
            changedClusterCm = new KafkaAssemblyBuilder(cluster).editSpec().editKafka()
                    .withNewEphemeralStorageStorage()
                    .endEphemeralStorageStorage().endKafka().endSpec().build();
        } else {
            fail();
        }
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Updating with changed storage type");
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
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
            context.assertEquals(resources.getRequests().getMilliCpu(), requirements.getRequests().get("cpu").getAmount());
        }
        if (resources != null && resources.getRequests() != null) {
            context.assertEquals(resources.getRequests().getMemory(), requirements.getRequests().get("memory").getAmount());
        }
        if (resources != null && resources.getLimits() != null) {
            context.assertEquals(resources.getLimits().getMilliCpu(), requirements.getLimits().get("cpu").getAmount());
        }
        if (resources != null && resources.getLimits() != null) {
            context.assertEquals(resources.getLimits().getMemory(), requirements.getLimits().get("memory").getAmount());
        }
    }

    /** Test that we can change the deleteClaim flag, and that it's honoured */
    @Test
    public void testUpdateKafkaWithChangedDeleteClaim(TestContext context) {
        Assume.assumeTrue(kafkaStorage instanceof PersistentClaimStorage);

        Set<String> allPvcs = new HashSet<>();
        Set<String> kafkaPvcs = createPvcs(kafkaStorage,
                kafkaReplicas, podId -> KafkaCluster.getPersistentVolumeClaimName(KafkaCluster.kafkaClusterName(CLUSTER_NAME), podId)
        );
        Set<String> zkPvcs = createPvcs(zkStorage,
                zkReplicas, podId -> ZookeeperCluster.getPersistentVolumeClaimName(ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME), podId)
        );
        allPvcs.addAll(kafkaPvcs);
        allPvcs.addAll(zkPvcs);

        KafkaAssemblyOperator kco = createCluster(context);

        boolean originalKafkaDeleteClaim = deleteClaim(kafkaStorage);

        // Try to update the storage class
        KafkaAssembly changedClusterCm = new KafkaAssemblyBuilder(cluster).editSpec().editKafka()
                .withNewPersistentClaimStorageStorage()
                .withDeleteClaim(!originalKafkaDeleteClaim)
                .endPersistentClaimStorageStorage().endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Updating with changed delete claim");
        Async updateAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync.complete();
        });
        updateAsync.await();

        LOGGER.info("Reconciling again -> delete");
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).delete();
        Async deleteAsync = context.async();
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            assertPvcs(context, !originalKafkaDeleteClaim ? deleteClaim(zkStorage) ? emptySet() : zkPvcs :
                    deleteClaim(zkStorage) ? kafkaPvcs : allPvcs);
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

        KafkaAssembly changedClusterCm = new KafkaAssemblyBuilder(cluster).editSpec().editKafka()
                .withReplicas(newScale).endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Scaling down to {} Kafka pods", newScale);
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
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

        KafkaAssembly changedClusterCm = new KafkaAssemblyBuilder(cluster).editSpec().editKafka()
                .withReplicas(newScale).endKafka().endSpec().build();
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).patch(changedClusterCm);

        LOGGER.info("Scaling up to {} Kafka pods", newScale);
        kco.reconcileAssembly(new Reconciliation("test-trigger", AssemblyType.KAFKA, NAMESPACE, CLUSTER_NAME), ar -> {
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
    public void testReconcileAllDeleteCase(TestContext context) throws InterruptedException {
        KafkaAssemblyOperator kco = createCluster(context);
        kafkaAssembly(NAMESPACE, CLUSTER_NAME).delete();

        LOGGER.info("reconcileAll after KafkaAssembly deletion -> All resources should be deleted");
        kco.reconcileAll("test-trigger", NAMESPACE).await();

        // TODO: Should verify that all resources were removed from MockKube
        // Assert no CMs, Services, StatefulSets, Deployments, Secrets are left
        context.assertTrue(mockClient.configMaps().inNamespace(NAMESPACE).list().getItems().isEmpty());
        context.assertTrue(mockClient.services().inNamespace(NAMESPACE).list().getItems().isEmpty());
        context.assertTrue(mockClient.apps().statefulSets().inNamespace(NAMESPACE).list().getItems().isEmpty());
        context.assertTrue(mockClient.extensions().deployments().inNamespace(NAMESPACE).list().getItems().isEmpty());
        // just the "internal-ca" certs is left because it's global (not cluster specific)
        // JAKUB
        context.assertEquals(0, mockClient.secrets().inNamespace(NAMESPACE).list().getItems().size());
    }

    @Test
    public void testResumePartialRoll(TestContext context) {


        KafkaAssemblyOperator kco = createCluster(context);
    }

}
