/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ResourceUtils;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Storage;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class KafkaClusterOperationsTest {

    private final boolean openShift;
    private final boolean metrics;
    private final String storage;
    private final boolean deleteClaim;

    public static class Params {
        private final boolean openShift;
        private final boolean metrics;
        private final String storage;

        public Params(boolean openShift, boolean metrics, String storage) {
            this.openShift = openShift;
            this.metrics = metrics;
            this.storage = storage;
        }
    }

    @Parameterized.Parameters
    public static Iterable<Params> data() {
        boolean[] shiftiness = {true, false};
        boolean[] metrics = {true, false};
        String[] storageConfigs = {
                "{\"type\": \"ephemeral\"}",
                "{\"type\": \"persistent-claim\", " +
                        "\"size\": \"123\", " +
                        "\"class\": \"foo\"," +
                        "\"delete-claim\": true}",
                "{\"type\": \"local\", " +
                        "\"size\": \"123\", " +
                        "\"class\": \"foo\"}"
        };
        List<Params> result = new ArrayList();
        for (boolean shift: shiftiness) {
            for (boolean metric: metrics) {
                for (String storage: storageConfigs) {
                    result.add(new Params(shift, metric, storage));
                }
            }
        }
        return result;
    }

    public KafkaClusterOperationsTest(Params params) {
        this.openShift = params.openShift;
        this.metrics = params.metrics;
        this.storage = params.storage;
        this.deleteClaim = Storage.fromJson(new JsonObject(params.storage)).isDeleteClaim();
    }

    protected static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateCluster(TestContext context) {
        createCluster(context, getConfigMap("foo"));
    }

    private void createCluster(TestContext context, ConfigMap clusterCm) {
        // create CM, Service, headless service, statefulset
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        StatefulSetOperations mockSsOps = mock(StatefulSetOperations.class);
        PvcOperations mockPvcOps = mock(PvcOperations.class);

        // Create a CM
        String clusterCmName = clusterCm.getMetadata().getName();
        String clusterCmNamespace = clusterCm.getMetadata().getNamespace();
        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.create(serviceCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StatefulSet> ssCaptor = ArgumentCaptor.forClass(StatefulSet.class);
        when(mockSsOps.create(ssCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaCluster kafkaCluster = KafkaCluster.fromConfigMap(clusterCm);
        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromConfigMap(clusterCm);
        ArgumentCaptor<ConfigMap> metricsCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.create(metricsCaptor.capture())).thenReturn(Future.succeededFuture());


        KafkaClusterOperations ops = new KafkaClusterOperations(vertx, openShift,
                mockCmOps,
                mockServiceOps, mockSsOps,
                mockPvcOps);

        // Now try to create a KafkaCluster based on this CM
        Async async = context.async();
        ops.create(clusterCmNamespace, clusterCmName, createResult -> {
            if (createResult.failed()) {
                createResult.cause().printStackTrace();
            }
            context.assertTrue(createResult.succeeded());

            // No metrics config  => no CMs created
            Set<String> metricsNames = new HashSet<>();
            if (kafkaCluster.isMetricsEnabled()) {
                metricsNames.add(KafkaCluster.metricConfigsName(clusterCmName));
            }
            if (zookeeperCluster.isMetricsEnabled()) {
                metricsNames.add(ZookeeperCluster.zookeeperMetricsName(clusterCmName));
            }
            context.assertEquals(metricsNames, metricsCaptor.getAllValues().stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet()),
                    "Unexpected metrics ConfigMaps");

            // We expect a headless and headful service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(4, capturedServices.size());
            context.assertEquals(set(KafkaCluster.kafkaClusterName(clusterCmName),
                    KafkaCluster.headlessName(clusterCmName),
                    ZookeeperCluster.zookeeperClusterName(clusterCmName),
                    ZookeeperCluster.zookeeperHeadlessName(clusterCmName)), capturedServices.stream().map(svc -> svc.getMetadata().getName()).collect(Collectors.toSet()));

            // Assertions on the statefulset
            List<StatefulSet> capturedSs = ssCaptor.getAllValues();
            // We expect a statefulSet for kafka and zookeeper...
            context.assertEquals(set(KafkaCluster.kafkaClusterName(clusterCmName), ZookeeperCluster.zookeeperClusterName(clusterCmName)),
                    capturedSs.stream().map(ss->ss.getMetadata().getName()).collect(Collectors.toSet()));

            // PvcOperations only used for deletion
            verifyNoMoreInteractions(mockPvcOps);
            async.complete();
        });
    }

    private void deleteCluster(TestContext context, ConfigMap clusterCm) {

        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromConfigMap(clusterCm);
        KafkaCluster kafkaCluster = KafkaCluster.fromConfigMap(clusterCm);
        // create CM, Service, headless service, statefulset
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        StatefulSetOperations mockSsOps = mock(StatefulSetOperations.class);
        PvcOperations mockPvcOps = mock(PvcOperations.class);

        String clusterCmName = clusterCm.getMetadata().getName();
        String clusterCmNamespace = clusterCm.getMetadata().getNamespace();
        StatefulSet kafkaSs = KafkaCluster.fromConfigMap(clusterCm).generateStatefulSet(true);

        StatefulSet zkSs = zookeeperCluster.generateStatefulSet(true);
        when(mockSsOps.get(clusterCmNamespace, KafkaCluster.kafkaClusterName(clusterCmName))).thenReturn(kafkaSs);
        when(mockSsOps.get(clusterCmNamespace, ZookeeperCluster.zookeeperClusterName(clusterCmName))).thenReturn(zkSs);

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ArgumentCaptor<String> serviceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<StatefulSet> ssCaptor = ArgumentCaptor.forClass(StatefulSet.class);
        when(mockSsOps.create(ssCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> metricsCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.delete(eq(clusterCmNamespace), metricsCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.delete(eq(clusterCmNamespace), serviceCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockSsOps.delete(anyString(), anyString())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> pvcCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPvcOps.delete(eq(clusterCmNamespace), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaClusterOperations ops = new KafkaClusterOperations(vertx, openShift,
                mockCmOps,
                mockServiceOps, mockSsOps,
                mockPvcOps);

        // Now try to create a KafkaCluster based on this CM
        Async async = context.async();
        ops.delete(clusterCmNamespace, clusterCmName, createResult -> {
            context.assertTrue(createResult.succeeded());

            Set<String> metricsNames = new HashSet<>();
            if (kafkaCluster.isMetricsEnabled()) {
                metricsNames.add(KafkaCluster.metricConfigsName(clusterCmName));
            }
            if (zookeeperCluster.isMetricsEnabled()) {
                metricsNames.add(ZookeeperCluster.zookeeperMetricsName(clusterCmName));
            }
            context.assertEquals(metricsNames, new HashSet(metricsCaptor.getAllValues()));
            verify(mockSsOps).delete(eq(clusterCmNamespace), eq(ZookeeperCluster.zookeeperClusterName(clusterCmName)));

            context.assertEquals(set(
                    ZookeeperCluster.zookeeperHeadlessName(clusterCmName),
                    ZookeeperCluster.zookeeperClusterName(clusterCmName),
                    KafkaCluster.kafkaClusterName(clusterCmName),
                    KafkaCluster.headlessName(clusterCmName)),
                    new HashSet(serviceCaptor.getAllValues()));

            // PvcOperations only used for deletion
            context.assertEquals(deleteClaim ? set("zookeeper-storage-" + clusterCmName + "-zookeeper-0",
                    "kafka-storage-" + clusterCmName + "-kafka-0") : set(), new HashSet(pvcCaptor.getAllValues()));
            async.complete();
        });
    }

    private ConfigMap getConfigMap(String clusterCmName) {
        String clusterCmNamespace = "test";
        int replicas = 1;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;
        String metricsCmJson = metrics ?  "{\"foo\":\"bar\"}" : null;
        return ResourceUtils.createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay, healthTimeout, metricsCmJson, storage);
    }

    private static <T> Set<T> set(T... elements) {
        return new HashSet<>(asList(elements));
    }


    @Test
    public void testDeleteCluster(TestContext context) {
        ConfigMap clusterCm = getConfigMap("baz");
        createCluster(context, clusterCm);
        deleteCluster(context, clusterCm);
    }


}
