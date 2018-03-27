/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Labels;
import io.strimzi.controller.cluster.resources.Storage;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class KafkaClusterOperationsMockIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterOperationsMockIT.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private final int zkReplicas;
    private final JsonObject zkStorage;

    private final int kafkaReplicas;
    private final JsonObject kafkaStorage;
    private KubernetesClient mockClient;

    public static class Params {
        private final int zkReplicas;
        private final JsonObject zkStorage;

        private final int kafkaReplicas;
        private final JsonObject kafkaStorage;

        public Params(int zkReplicas, JsonObject zkStorage, int kafkaReplicas, JsonObject kafkaStorage) {
            this.kafkaReplicas = kafkaReplicas;
            this.kafkaStorage = kafkaStorage;
            this.zkReplicas = zkReplicas;
            this.zkStorage = zkStorage;
        }

        public String toString() {
            return "zkReplicas=" + zkReplicas +
                    ",zkStorage=" + kafkaStorage +
                    ",kafkaReplicas=" + kafkaReplicas +
                    ",kafkaStorage=" + kafkaStorage;
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<KafkaClusterOperationsMockIT.Params> data() {
        int[] kafkaReplicas = {1, 2, 3};
        int[] zkReplicas = {1, 2, 3};
        JsonObject[] storageConfigs = {
                new JsonObject("{\"type\": \"ephemeral\"}"),

                new JsonObject("{\"type\": \"persistent-claim\", " +
                        "\"size\": \"123\", " +
                        "\"class\": \"foo\"," +
                        "\"delete-claim\": true}"),

                new JsonObject("{\"type\": \"persistent-claim\", " +
                        "\"size\": \"123\", " +
                        "\"class\": \"foo\"," +
                        "\"delete-claim\": false}"),

                new JsonObject("{\"type\": \"local\", " +
                        "\"size\": \"123\", " +
                        "\"class\": \"foo\"}")
        };
        List<KafkaClusterOperationsMockIT.Params> result = new ArrayList();

        for (int zkReplica : zkReplicas) {
            for (JsonObject zkStorage : storageConfigs) {
                for (int kafkaReplica : kafkaReplicas) {
                    for (JsonObject kafkaStorage : storageConfigs) {
                        result.add(new KafkaClusterOperationsMockIT.Params(
                                zkReplica, zkStorage,
                                kafkaReplica, kafkaStorage));
                    }
                }
            }
        }

        return result;
    }

    public KafkaClusterOperationsMockIT(KafkaClusterOperationsMockIT.Params params) {
        this.zkReplicas = params.zkReplicas;
        this.zkStorage = params.zkStorage;

        this.kafkaReplicas = params.kafkaReplicas;
        this.kafkaStorage = params.kafkaStorage;
    }

    /** Return the storage type the test cluster initially uses */
    public Storage.StorageType storageType(JsonObject json) {
        return Storage.fromJson(json).type();
    }

    /** Return the storage class the test cluster initially uses */
    public String storageClass(JsonObject json) {
        return json.getString(Storage.STORAGE_CLASS_FIELD);
    }

    /** Return the storage delete-claim the test cluster initially uses */
    public boolean deleteClaim(JsonObject json) {
        return json.getBoolean(Storage.DELETE_CLAIM_FIELD, false);
    }


    private Vertx vertx;
    private ConfigMap cluster;

    @Before
    public void before() {
        this.vertx = Vertx.vertx();
        this.cluster = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
                .withLabels(Labels.forKind("cluster").withType(KafkaClusterOperations.CLUSTER_TYPE_KAFKA).toMap())
                .endMetadata()
                .withData(map(KafkaCluster.KEY_REPLICAS, String.valueOf(kafkaReplicas),
                        KafkaCluster.KEY_STORAGE, kafkaStorage.toString(),
                        ZookeeperCluster.KEY_REPLICAS, String.valueOf(zkReplicas),
                        ZookeeperCluster.KEY_STORAGE, zkStorage.toString()))
                .build();
        mockClient = new MockKube().withInitialCms(Collections.singleton(cluster)).build();
    }

    @After
    public void after() {
        this.vertx.close();
    }

    private static <T> Map<T, T> map(T... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        Map<T, T> result = new HashMap<>(pairs.length / 2);
        for (int i = 0; i < pairs.length; i += 2) {
            result.put(pairs[i], pairs[i + 1]);
        }
        return result;
    }

    // TODO Test with invalid cluster CMs:
    // - Test with absent storage key
    // - Test with storage key lacking a type

    private KafkaClusterOperations createCluster(TestContext context) {
        ConfigMapOperations cmops = new ConfigMapOperations(vertx, mockClient);
        ServiceOperations svcops = new ServiceOperations(vertx, mockClient);
        KafkaSetOperations ksops = new KafkaSetOperations(vertx, mockClient);
        ZookeeperSetOperations zksops = new ZookeeperSetOperations(vertx, mockClient);
        DeploymentOperations depops = new DeploymentOperations(vertx, mockClient);
        PvcOperations pvcops = new PvcOperations(vertx, mockClient);
        KafkaClusterOperations kco = new KafkaClusterOperations(vertx, true, 2_000,
                cmops, svcops, zksops, ksops, pvcops, depops);

        LOGGER.info("Reconciling initially -> create");
        Async createAsync = context.async();
        kco.createOrUpdate(NAMESPACE, cluster.getMetadata().getName(), ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            createAsync.complete();
        });
        createAsync.await();
        return kco;
    }

    /** Create a cluster from a Kafka Cluster CM */
    @Test
    public void testCreateUpdateDelete(TestContext context) {
        Set<String> expectedClaims = resilientPvcs();

        KafkaClusterOperations kco = createCluster(context);
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.createOrUpdate(NAMESPACE, CLUSTER_NAME, ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            updateAsync.complete();
        });
        updateAsync.await();
        LOGGER.info("Reconciling again -> delete");
        mockClient.configMaps().inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
        Async deleteAsync = context.async();
        kco.delete(NAMESPACE, CLUSTER_NAME, ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            assertPvcs(context, expectedClaims);
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
    private Set<String> createPvcs(JsonObject storage, int replicas, Function<Integer, String> pvcNameFn) {
        Set<String> expectedClaims = new HashSet<>();
        if (storageType(storage).equals(Storage.StorageType.PERSISTENT_CLAIM)) {
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

    /**
     * Test the controller re-creates services if they get deleted
     */
    private void updateClusterWithoutServices(TestContext context, String... services) {

        KafkaClusterOperations kco = createCluster(context);
        for (String service: services) {
            mockClient.services().inNamespace(NAMESPACE).withName(service).delete();
            assertNull("Expected service " + service + " to be not exist",
                    mockClient.services().inNamespace(NAMESPACE).withName(service).get());
        }
        LOGGER.info("Reconciling again -> update");
        Async updateAsync = context.async();
        kco.createOrUpdate(NAMESPACE, CLUSTER_NAME, ar -> {
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
                ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME),
                ZookeeperCluster.zookeeperHeadlessName(CLUSTER_NAME));
    }

    @Test
    public void testUpdateClusterWithoutKafkaServices(TestContext context) {
        updateClusterWithoutServices(context,
                KafkaCluster.kafkaClusterName(CLUSTER_NAME),
                KafkaCluster.headlessName(CLUSTER_NAME));
    }

    private void deleteClusterWithoutServices(TestContext context, String... services) {

        KafkaClusterOperations kco = createCluster(context);
        for (String service: services) {
            mockClient.services().inNamespace(NAMESPACE).withName(service).delete();
            assertNull("Expected service " + service + " to be not exist",
                    mockClient.services().inNamespace(NAMESPACE).withName(service).get());
        }
        LOGGER.info("Deleting");
        Async updateAsync = context.async();
        kco.delete(NAMESPACE, CLUSTER_NAME, ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            for (String service: services) {
                assertNull(
                        "Expected service " + service + " to still not exist",
                        mockClient.services().inNamespace(NAMESPACE).withName(service).get());
            }
            // TODO assert other resources do not exist either
            updateAsync.complete();
        });
    }

    @Test
    public void testDeleteClusterWithoutZkServices(TestContext context) {
        deleteClusterWithoutServices(context,
                ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME),
                ZookeeperCluster.zookeeperHeadlessName(CLUSTER_NAME));
    }

    @Test
    public void testDeleteClusterWithoutKafkaServices(TestContext context) {
        deleteClusterWithoutServices(context,
                KafkaCluster.kafkaClusterName(CLUSTER_NAME),
                KafkaCluster.headlessName(CLUSTER_NAME));
    }

    private void deleteClusterWithoutStatefulSet(TestContext context, String... statefulSets) {

        KafkaClusterOperations kco = createCluster(context);
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
        Async updateAsync = context.async();
        kco.delete(NAMESPACE, CLUSTER_NAME, ar -> {
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
        if (Storage.StorageType.PERSISTENT_CLAIM.equals(storageType(kafkaStorage))) {

            // TODO move this to mock kube, and make it cope properly with scale down
            mockClient.pods().inNamespace(NAMESPACE).withName(KafkaCluster.kafkaPodName(CLUSTER_NAME, 0)).watch(new Watcher<Pod>() {
                @Override
                public void eventReceived(Watcher.Action action, Pod resource) {
                    if (action == Action.DELETED) {
                        vertx.setTimer(200, timerId -> {
                            String podName = resource.getMetadata().getName();
                            mockClient.pods().inNamespace(NAMESPACE).withName(podName).create(resource);
                        });
                    }
                }

                @Override
                public void onClose(KubernetesClientException e) {

                }
            });

            KafkaClusterOperations kco = createCluster(context);
            String originalStorageClass = storageClass(kafkaStorage);
            assertStorageClass(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalStorageClass);

            Async updateAsync = context.async();

            // Try to update the storage class
            String changedClass = originalStorageClass + "2";

            HashMap<String, String> data = new HashMap<>(cluster.getData());
            data.put(KafkaCluster.KEY_STORAGE,
                    new JsonObject(kafkaStorage.toString()).put(Storage.STORAGE_CLASS_FIELD, changedClass).toString());
            ConfigMap changedClusterCm = new ConfigMapBuilder(cluster).withData(data).build();
            mockClient.configMaps().inNamespace(NAMESPACE).withName(CLUSTER_NAME).patch(changedClusterCm);

            LOGGER.info("Updating with changed storage class");
            kco.createOrUpdate(NAMESPACE, CLUSTER_NAME, ar -> {
                if (ar.failed()) ar.cause().printStackTrace();
                context.assertTrue(ar.succeeded());
                // Check the storage class was not changed
                assertStorageClass(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalStorageClass);
                updateAsync.complete();
            });
        }
    }

    private void assertStorageClass(TestContext context, String statefulSetName, String expectedClass) {
        StatefulSet statefulSet = mockClient.apps().statefulSets().inNamespace(NAMESPACE).withName(statefulSetName).get();
        context.assertNotNull(statefulSet);
        // Check the storage class is initially "foo"
        List<PersistentVolumeClaim> volumeClaimTemplates = statefulSet.getSpec().getVolumeClaimTemplates();
        context.assertFalse(volumeClaimTemplates.isEmpty());
        context.assertEquals(expectedClass, volumeClaimTemplates.get(0).getSpec().getStorageClassName());
    }

    /** Test that we can change the deleteClaim flag, and that it's honoured */
    @Test
    public void testUpdateKafkaWithChangedDeleteClaim(TestContext context) {
        if (Storage.StorageType.PERSISTENT_CLAIM.equals(storageType(kafkaStorage))) {
            Set<String> allPvcs = new HashSet<>();
            Set<String> kafkaPvcs = createPvcs(kafkaStorage,
                    kafkaReplicas, podId -> KafkaCluster.getPersistentVolumeClaimName(KafkaCluster.kafkaClusterName(CLUSTER_NAME), podId)
            );
            Set<String> zkPvcs = createPvcs(zkStorage,
                    zkReplicas, podId -> ZookeeperCluster.getPersistentVolumeClaimName(ZookeeperCluster.zookeeperClusterName(CLUSTER_NAME), podId)
            );
            allPvcs.addAll(kafkaPvcs);
            allPvcs.addAll(zkPvcs);

            KafkaClusterOperations kco = createCluster(context);

            boolean originalKafkaDeleteClaim = deleteClaim(kafkaStorage);
            //assertDeleteClaim(context, KafkaCluster.kafkaClusterName(CLUSTER_NAME), originalKafkaDeleteClaim);

            // Try to update the storage class
            boolean changedKafkaDeleteClaim = !originalKafkaDeleteClaim;
            HashMap<String, String> data = new HashMap<>(cluster.getData());
            data.put(KafkaCluster.KEY_STORAGE,
                    new JsonObject(kafkaStorage.toString()).put(Storage.DELETE_CLAIM_FIELD, changedKafkaDeleteClaim).toString());
            ConfigMap changedClusterCm = new ConfigMapBuilder(cluster).withData(data).build();
            mockClient.configMaps().inNamespace(NAMESPACE).withName(CLUSTER_NAME).patch(changedClusterCm);

            LOGGER.info("Updating with changed delete claim");
            Async updateAsync = context.async();
            kco.createOrUpdate(NAMESPACE, CLUSTER_NAME, ar -> {
                if (ar.failed()) ar.cause().printStackTrace();
                context.assertTrue(ar.succeeded());
                updateAsync.complete();
            });
            updateAsync.await();

            LOGGER.info("Reconciling again -> delete");
            mockClient.configMaps().inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
            Async deleteAsync = context.async();
            kco.delete(NAMESPACE, CLUSTER_NAME, ar -> {
                if (ar.failed()) ar.cause().printStackTrace();
                context.assertTrue(ar.succeeded());
                assertPvcs(context, changedKafkaDeleteClaim ? deleteClaim(zkStorage) ? emptySet() : zkPvcs :
                        deleteClaim(zkStorage) ? kafkaPvcs : allPvcs);
                deleteAsync.complete();
            });
        }
    }

}
