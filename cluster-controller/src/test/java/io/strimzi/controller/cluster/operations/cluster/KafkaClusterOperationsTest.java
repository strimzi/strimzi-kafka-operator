/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ClusterController;
import io.strimzi.controller.cluster.ClusterControllerConfig;
import io.strimzi.controller.cluster.ResourceUtils;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.EndpointOperations;
import io.strimzi.controller.cluster.operations.resource.PodOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.strimzi.controller.cluster.resources.AbstractCluster;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Labels;
import io.strimzi.controller.cluster.resources.Storage;
import io.strimzi.controller.cluster.resources.TopicController;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class KafkaClusterOperationsTest {

    public static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";
    private final boolean openShift;
    private final boolean metrics;
    private final String storage;
    private final String tcConfig;
    private final boolean deleteClaim;

    public static class Params {
        private final boolean openShift;
        private final boolean metrics;
        private final String storage;
        private final String tcConfig;

        public Params(boolean openShift, boolean metrics, String storage, String tcConfig) {
            this.openShift = openShift;
            this.metrics = metrics;
            this.storage = storage;
            this.tcConfig = tcConfig;
        }

        public String toString() {
            return "openShift=" + openShift +
                    ",metrics=" + metrics +
                    ",storage=" + storage +
                    ",tcConfig=" + tcConfig;
        }
    }

    @Parameterized.Parameters(name = "{0}")
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
        String[] tcConfigs = {
            null,
            "{ }",
            "{\"reconciliationInterval\": \"10 minutes\", " +
                    "\"zookeeperSessionTimeout\": \"10 seconds\"}"
        };
        List<Params> result = new ArrayList();
        for (boolean shift: shiftiness) {
            for (boolean metric: metrics) {
                for (String storage: storageConfigs) {
                    for (String tcConfig: tcConfigs) {
                        result.add(new Params(shift, metric, storage, tcConfig));
                    }
                }
            }
        }
        return result;
    }

    public KafkaClusterOperationsTest(Params params) {
        this.openShift = params.openShift;
        this.metrics = params.metrics;
        this.storage = params.storage;
        this.tcConfig = params.tcConfig;
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
        // create CM, Service, headless service, statefulset and so on
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        StatefulSetOperations mockSsOps = mock(StatefulSetOperations.class);
        PvcOperations mockPvcOps = mock(PvcOperations.class);
        PodOperations mockPodOps = mock(PodOperations.class);
        EndpointOperations mockEndpointOps = mock(EndpointOperations.class);
        DeploymentOperations mockDepOps = mock(DeploymentOperations.class);

        // Create a CM
        String clusterCmName = clusterCm.getMetadata().getName();
        String clusterCmNamespace = clusterCm.getMetadata().getNamespace();
        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.create(serviceCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StatefulSet> ssCaptor = ArgumentCaptor.forClass(StatefulSet.class);
        when(mockSsOps.create(ssCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.create(depCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockSsOps.readiness(any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockEndpointOps.readiness(any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        KafkaCluster kafkaCluster = KafkaCluster.fromConfigMap(clusterCm);
        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromConfigMap(clusterCm);
        TopicController topicController = TopicController.fromConfigMap(clusterCm);
        ArgumentCaptor<ConfigMap> metricsCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.create(metricsCaptor.capture())).thenReturn(Future.succeededFuture());


        KafkaClusterOperations ops = new KafkaClusterOperations(vertx, openShift,
                ClusterControllerConfig.DEFAULT_OPERATION_TIMEOUT_MS,
                mockCmOps,
                mockServiceOps, mockSsOps,
                mockPvcOps, mockPodOps, mockEndpointOps, mockDepOps);

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
            Map<String, ConfigMap> cmsByName = metricsCaptor.getAllValues().stream().collect(Collectors.toMap(cm -> cm.getMetadata().getName(), Function.identity()));
            context.assertEquals(metricsNames, cmsByName.keySet(),
                    "Unexpected metrics ConfigMaps");
            if (kafkaCluster.isMetricsEnabled()) {
                ConfigMap kafkaMetricsCm = cmsByName.get(KafkaCluster.metricConfigsName(clusterCmName));
                context.assertEquals(ResourceUtils.labels(Labels.STRIMZI_TYPE_LABEL, "kafka",
                        Labels.STRIMZI_CLUSTER_LABEL, clusterCmName,
                        "my-user-label", "cromulent"), kafkaMetricsCm.getMetadata().getLabels());
            }
            if (zookeeperCluster.isMetricsEnabled()) {
                ConfigMap zookeeperMetricsCm = cmsByName.get(ZookeeperCluster.zookeeperMetricsName(clusterCmName));
                context.assertEquals(ResourceUtils.labels(Labels.STRIMZI_TYPE_LABEL, "zookeeper",
                        Labels.STRIMZI_CLUSTER_LABEL, clusterCmName,
                        "my-user-label", "cromulent"), zookeeperMetricsCm.getMetadata().getLabels());
            }


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
                    capturedSs.stream().map(ss -> ss.getMetadata().getName()).collect(Collectors.toSet()));

            // Verify that we wait for readiness
            verify(mockEndpointOps, times(4)).readiness(any(), any(), anyLong(), anyLong());
            verify(mockSsOps, times(2)).readiness(any(), any(), anyLong(), anyLong());
            verify(mockPodOps, times(zookeeperCluster.getReplicas() + kafkaCluster.getReplicas()))
                    .readiness(any(), any(), anyLong(), anyLong());

            // if topic controller configuration was defined in the CM
            if (topicController != null) {
                List<Deployment> capturedDeps = depCaptor.getAllValues();
                context.assertEquals(1, capturedDeps.size());
                context.assertEquals(TopicController.topicControllerName(clusterCmName), capturedDeps.get(0).getMetadata().getName());
            }

            // PvcOperations only used for deletion
            verifyNoMoreInteractions(mockPvcOps);
            async.complete();
        });
    }

    private void deleteCluster(TestContext context, ConfigMap clusterCm) {

        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromConfigMap(clusterCm);
        KafkaCluster kafkaCluster = KafkaCluster.fromConfigMap(clusterCm);
        TopicController topicController = TopicController.fromConfigMap(clusterCm);
        // create CM, Service, headless service, statefulset
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        StatefulSetOperations mockSsOps = mock(StatefulSetOperations.class);
        PvcOperations mockPvcOps = mock(PvcOperations.class);
        PodOperations mockPodOps = mock(PodOperations.class);
        EndpointOperations mockEndpointOps = mock(EndpointOperations.class);
        DeploymentOperations mockDepOps = mock(DeploymentOperations.class);

        String clusterCmName = clusterCm.getMetadata().getName();
        String clusterCmNamespace = clusterCm.getMetadata().getNamespace();
        StatefulSet kafkaSs = kafkaCluster.generateStatefulSet(true);

        StatefulSet zkSs = zookeeperCluster.generateStatefulSet(true);
        when(mockSsOps.get(clusterCmNamespace, KafkaCluster.kafkaClusterName(clusterCmName))).thenReturn(kafkaSs);
        when(mockSsOps.get(clusterCmNamespace, ZookeeperCluster.zookeeperClusterName(clusterCmName))).thenReturn(zkSs);

        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ArgumentCaptor<String> serviceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> ssCaptor = ArgumentCaptor.forClass(String.class);

        ArgumentCaptor<String> metricsCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.delete(eq(clusterCmNamespace), metricsCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.delete(eq(clusterCmNamespace), serviceCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockSsOps.delete(anyString(), ssCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> pvcCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPvcOps.delete(eq(clusterCmNamespace), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> depCaptor = ArgumentCaptor.forClass(String.class);
        when(mockDepOps.delete(eq(clusterCmNamespace), depCaptor.capture())).thenReturn(Future.succeededFuture());
        if (topicController != null) {
            Deployment tcDep = topicController.generateDeployment();
            when(mockDepOps.get(clusterCmNamespace, TopicController.topicControllerName(clusterCmName))).thenReturn(tcDep);
        }

        KafkaClusterOperations ops = new KafkaClusterOperations(vertx, openShift,
                ClusterControllerConfig.DEFAULT_OPERATION_TIMEOUT_MS,
                mockCmOps,
                mockServiceOps, mockSsOps,
                mockPvcOps,
                mockPodOps, mockEndpointOps, mockDepOps);

        // Now try to delete a KafkaCluster based on this CM
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
            context.assertEquals(metricsNames, captured(metricsCaptor));
            verify(mockSsOps).delete(eq(clusterCmNamespace), eq(ZookeeperCluster.zookeeperClusterName(clusterCmName)));

            context.assertEquals(set(
                    ZookeeperCluster.zookeeperHeadlessName(clusterCmName),
                    ZookeeperCluster.zookeeperClusterName(clusterCmName),
                    KafkaCluster.kafkaClusterName(clusterCmName),
                    KafkaCluster.headlessName(clusterCmName)),
                    captured(serviceCaptor));

            // verify deleted Statefulsets
            context.assertEquals(set(zookeeperCluster.getName(), kafkaCluster.getName()), captured(ssCaptor));

            // PvcOperations only used for deletion
            Set<String> expectedPvcDeletions = new HashSet<>();
            for (int i = 0; deleteClaim && i < kafkaCluster.getReplicas(); i++) {
                expectedPvcDeletions.add("data-" + clusterCmName + "-kafka-" + i);
            }
            for (int i = 0; deleteClaim && i < zookeeperCluster.getReplicas(); i++) {
                expectedPvcDeletions.add("data-" + clusterCmName + "-zookeeper-" + i);
            }
            context.assertEquals(expectedPvcDeletions, captured(pvcCaptor));

            // if topic controller configuration was defined in the CM
            if (topicController != null) {
                Set<String> expectedDepNames = new HashSet<>();
                expectedDepNames.add(TopicController.topicControllerName(clusterCmName));
                context.assertEquals(expectedDepNames, captured(depCaptor));
            }

            async.complete();
        });
    }

    private ConfigMap getConfigMap(String clusterCmName) {
        String clusterCmNamespace = "test";
        int replicas = 3;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;
        String metricsCmJson = metrics ? METRICS_CONFIG : null;
        return ResourceUtils.createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay, healthTimeout, metricsCmJson, storage, tcConfig);
    }

    private static <T> Set<T> set(T... elements) {
        return new HashSet<>(asList(elements));
    }

    private static <T> Set<T> captured(ArgumentCaptor<T> captor) {
        return new HashSet<>(captor.getAllValues());
    }

    @Test
    public void testDeleteCluster(TestContext context) {
        ConfigMap clusterCm = getConfigMap("baz");
        deleteCluster(context, clusterCm);
    }

    @Test
    public void testUpdateClusterNoop(TestContext context) {
        ConfigMap clusterCm = getConfigMap("bar");
        updateCluster(context, getConfigMap("bar"), clusterCm);
    }

    @Test
    public void testUpdateKafkaClusterChangeImage(TestContext context) {
        ConfigMap clusterCm = getConfigMap("bar");
        clusterCm.getData().put(KafkaCluster.KEY_IMAGE, "a-changed-image");
        updateCluster(context, getConfigMap("bar"), clusterCm);
    }

    @Test
    public void testUpdateZookeeperClusterChangeImage(TestContext context) {
        ConfigMap clusterCm = getConfigMap("bar");
        clusterCm.getData().put(ZookeeperCluster.KEY_IMAGE, "a-changed-image");
        updateCluster(context, getConfigMap("bar"), clusterCm);
    }

    @Test
    public void testUpdateKafkaClusterScaleUp(TestContext context) {
        ConfigMap clusterCm = getConfigMap("bar");
        clusterCm.getData().put(KafkaCluster.KEY_REPLICAS, "4");
        updateCluster(context, getConfigMap("bar"), clusterCm);
    }

    @Test
    public void testUpdateKafkaClusterScaleDown(TestContext context) {
        ConfigMap clusterCm = getConfigMap("bar");
        clusterCm.getData().put(KafkaCluster.KEY_REPLICAS, "2");
        updateCluster(context, getConfigMap("bar"), clusterCm);
    }

    @Test
    public void testUpdateZookeeperClusterScaleUp(TestContext context) {
        ConfigMap clusterCm = getConfigMap("bar");
        clusterCm.getData().put(ZookeeperCluster.KEY_REPLICAS, "4");
        updateCluster(context, getConfigMap("bar"), clusterCm);
    }

    @Test
    public void testUpdateZookeeperClusterScaleDown(TestContext context) {
        ConfigMap clusterCm = getConfigMap("bar");
        clusterCm.getData().put(ZookeeperCluster.KEY_REPLICAS, "2");
        updateCluster(context, getConfigMap("bar"), clusterCm);
    }

    @Test
    public void testUpdateClusterMetricsConfig(TestContext context) {
        ConfigMap clusterCm = getConfigMap("bar");
        clusterCm.getData().put(KafkaCluster.KEY_METRICS_CONFIG, "{\"something\":\"changed\"}");
        updateCluster(context, getConfigMap("bar"), clusterCm);
    }

    @Test
    public void testUpdateTopicControllerConfig(TestContext context) {
        ConfigMap clusterCm = getConfigMap("bar");
        if (tcConfig != null) {
            clusterCm.getData().put(TopicController.KEY_CONFIG, "{\"something\":\"changed\"}");
            updateCluster(context, getConfigMap("bar"), clusterCm);
        }
    }

    private void updateCluster(TestContext context, ConfigMap originalCm, ConfigMap clusterCm) {

        KafkaCluster originalKafkaCluster = KafkaCluster.fromConfigMap(originalCm);
        KafkaCluster updatedKafkaCluster = KafkaCluster.fromConfigMap(clusterCm);
        ZookeeperCluster originalZookeeperCluster = ZookeeperCluster.fromConfigMap(originalCm);
        ZookeeperCluster updatedZookeeperCluster = ZookeeperCluster.fromConfigMap(clusterCm);
        TopicController originalTopicController = TopicController.fromConfigMap(originalCm);
        TopicController updatedTopicController = TopicController.fromConfigMap(clusterCm);

        ClusterDiffResult kafkaDiff = updatedKafkaCluster.diff(
                originalKafkaCluster.isMetricsEnabled() ? originalKafkaCluster.generateMetricsConfigMap() : null,
                originalKafkaCluster.generateStatefulSet(openShift));
        ClusterDiffResult zkDiff = updatedZookeeperCluster.diff(
                originalZookeeperCluster.isMetricsEnabled() ? originalZookeeperCluster.generateMetricsConfigMap() : null,
                originalZookeeperCluster.generateStatefulSet(openShift));
        ClusterDiffResult tcDiff = ((originalTopicController != null) && (updatedTopicController != null)) ?
                updatedTopicController.diff(originalTopicController.generateDeployment()) : new ClusterDiffResult();

        // create CM, Service, headless service, statefulset and so on
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        StatefulSetOperations mockSsOps = mock(StatefulSetOperations.class);
        PvcOperations mockPvcOps = mock(PvcOperations.class);
        PodOperations mockPodOps = mock(PodOperations.class);
        EndpointOperations mockEndpointOps = mock(EndpointOperations.class);
        DeploymentOperations mockDepOps = mock(DeploymentOperations.class);

        String clusterCmName = clusterCm.getMetadata().getName();
        String clusterCmNamespace = clusterCm.getMetadata().getNamespace();

        // Mock CM get
        when(mockCmOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaCluster.metricConfigsName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractCluster.METRICS_CONFIG_FILE, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(clusterCmNamespace, KafkaCluster.metricConfigsName(clusterCmName))).thenReturn(metricsCm);
        ConfigMap zkMetricsCm = new ConfigMapBuilder().withNewMetadata()
                .withName(ZookeeperCluster.zookeeperMetricsName(clusterCmName))
                .withNamespace(clusterCmNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractCluster.METRICS_CONFIG_FILE, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(clusterCmNamespace, ZookeeperCluster.zookeeperMetricsName(clusterCmName))).thenReturn(zkMetricsCm);


        // Mock Service gets
        when(mockServiceOps.get(clusterCmNamespace, KafkaCluster.kafkaClusterName(clusterCmName))).thenReturn(
                originalKafkaCluster.generateService()
        );
        when(mockServiceOps.get(clusterCmNamespace, KafkaCluster.headlessName(clusterCmName))).thenReturn(
                originalKafkaCluster.generateHeadlessService()
        );
        when(mockServiceOps.get(clusterCmNamespace, ZookeeperCluster.zookeeperClusterName(clusterCmName))).thenReturn(
                originalKafkaCluster.generateService()
        );
        when(mockServiceOps.get(clusterCmNamespace, ZookeeperCluster.zookeeperHeadlessName(clusterCmName))).thenReturn(
                originalZookeeperCluster.generateHeadlessService()
        );

        // Mock StatefulSet get
        when(mockSsOps.get(clusterCmNamespace, KafkaCluster.kafkaClusterName(clusterCmName))).thenReturn(
                originalKafkaCluster.generateStatefulSet(openShift)
        );
        when(mockSsOps.get(clusterCmNamespace, ZookeeperCluster.zookeeperClusterName(clusterCmName))).thenReturn(
                originalZookeeperCluster.generateStatefulSet(openShift)
        );

        // Mock Deployment get
        if (originalTopicController != null) {
            when(mockDepOps.get(clusterCmNamespace, TopicController.topicControllerName(clusterCmName))).thenReturn(
                    originalTopicController.generateDeployment()
            );
        }

        // Mock CM patch
        Set<String> metricsCms = set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).patch(eq(clusterCmNamespace), anyString(), any());

        // Mock Service patch (both service and headless service
        ArgumentCaptor<String> patchedServicesCaptor = ArgumentCaptor.forClass(String.class);
        when(mockServiceOps.patch(eq(clusterCmNamespace), patchedServicesCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        // Mock StatefulSet patch
        when(mockSsOps.patch(anyString(), anyString(), anyBoolean(), any())).thenReturn(Future.succeededFuture());
        // Mock StatefulSet rollingUpdate
        Set<String> rollingRestarts = set();
        doAnswer(invocation -> {
            rollingRestarts.add(invocation.getArgument(1));
            ((Handler<AsyncResult<Void>>) invocation.getArgument(2)).handle(Future.succeededFuture());
            return null;
        }).when(mockSsOps).rollingUpdate(eq(clusterCmNamespace), anyString(), any());
        // Mock StatefulSet scaleUp
        ArgumentCaptor<String> scaledUpCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSsOps.scaleUp(anyString(), scaledUpCaptor.capture(), anyInt())).thenReturn(
                Future.succeededFuture()
        );
        // Mock StatefulSet scaleDown
        ArgumentCaptor<String> scaledDownCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSsOps.scaleDown(anyString(), scaledDownCaptor.capture(), anyInt())).thenReturn(
                Future.succeededFuture()
        );

        // Mock Deployment patch
        ArgumentCaptor<String> depCaptor = ArgumentCaptor.forClass(String.class);
        when(mockDepOps.patch(anyString(), depCaptor.capture(), any())).thenReturn(Future.succeededFuture());

        KafkaClusterOperations ops = new KafkaClusterOperations(vertx, openShift,
                ClusterControllerConfig.DEFAULT_OPERATION_TIMEOUT_MS,
                mockCmOps,
                mockServiceOps, mockSsOps,
                mockPvcOps, mockPodOps, mockEndpointOps, mockDepOps);

        // Now try to update a KafkaCluster based on this CM
        Async async = context.async();
        ops.update(clusterCmNamespace, clusterCmName, createResult -> {
            if (createResult.failed()) createResult.cause().printStackTrace();
            context.assertTrue(createResult.succeeded());

            // cm patch iff metrics changed
            Set<String> expectedMetricsCms = set();
            if (kafkaDiff.isMetricsChanged()) {
                expectedMetricsCms.add(KafkaCluster.metricConfigsName(clusterCmName));
            }
            if (zkDiff.isMetricsChanged()) {
                expectedMetricsCms.add(ZookeeperCluster.zookeeperMetricsName(clusterCmName));
            }
            context.assertEquals(expectedMetricsCms, metricsCms);

            // patch services
            Set<String> expectedPatchedServices = set();
            if (kafkaDiff.isDifferent()) {
                expectedPatchedServices.add(originalKafkaCluster.getName());
                expectedPatchedServices.add(originalKafkaCluster.getHeadlessName());
            }
            if (zkDiff.isDifferent()) {
                expectedPatchedServices.add(originalZookeeperCluster.getName());
                expectedPatchedServices.add(originalZookeeperCluster.getHeadlessName());
            }
            context.assertEquals(expectedPatchedServices, captured(patchedServicesCaptor));

            // rolling restart
            Set<String> expectedRollingRestarts = set();
            if (kafkaDiff.isRollingUpdate()) {
                expectedRollingRestarts.add(originalKafkaCluster.getName());
            }
            if (zkDiff.isRollingUpdate()) {
                expectedRollingRestarts.add(originalZookeeperCluster.getName());
            }
            context.assertEquals(expectedRollingRestarts, rollingRestarts);

            // scale down
            Set<String> expectedScaleDown = set();
            if (kafkaDiff.isScaleDown()) {
                expectedScaleDown.add(originalKafkaCluster.getName());
            }
            if (zkDiff.isScaleDown()) {
                expectedScaleDown.add(originalZookeeperCluster.getName());
            }
            context.assertEquals(expectedScaleDown, captured(scaledDownCaptor));

            // scale up
            Set<String> expectedScaleUp = set();
            if (kafkaDiff.isScaleUp()) {
                expectedScaleUp.add(originalKafkaCluster.getName());
            }
            if (zkDiff.isScaleUp()) {
                expectedScaleUp.add(originalZookeeperCluster.getName());
            }
            context.assertEquals(expectedScaleUp, captured(scaledUpCaptor));


            if ((originalTopicController != null) && (updatedTopicController != null)) {
                Set<String> expectedDeps = set();
                if (tcDiff.isDifferent()) {
                    expectedDeps.add(originalTopicController.getName());
                }
                context.assertEquals(expectedDeps, captured(depCaptor));
            }

            // No metrics config  => no CMs created
            verify(mockCmOps, never()).create(any());
            verifyNoMoreInteractions(mockPvcOps);
            async.complete();
        });
    }

    @Test
    public void testReconcile(TestContext context) {
        Async async = context.async(3);

        // create CM, Service, headless service, statefulset
        ConfigMapOperations mockCmOps = mock(ConfigMapOperations.class);
        ServiceOperations mockServiceOps = mock(ServiceOperations.class);
        StatefulSetOperations mockSsOps = mock(StatefulSetOperations.class);
        PvcOperations mockPvcOps = mock(PvcOperations.class);
        PodOperations mockPodOps = mock(PodOperations.class);
        EndpointOperations mockEndpointOps = mock(EndpointOperations.class);
        DeploymentOperations mockDepOps = mock(DeploymentOperations.class);

        String clusterCmNamespace = "myNamespace";

        ConfigMap foo = getConfigMap("foo");
        ConfigMap bar = getConfigMap("bar");
        ConfigMap baz = getConfigMap("baz");
        when(mockCmOps.list(eq(clusterCmNamespace), any())).thenReturn(
            asList(foo, bar)
        );
        // when requested ConfigMap for a specific Kafka cluster
        when(mockCmOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockCmOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);


        // providing the list of ALL StatefulSets for all the Kafka clusters
        Labels newLabels = Labels.type("kafka");
        when(mockSsOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaCluster.fromConfigMap(bar).generateStatefulSet(openShift),
                        KafkaCluster.fromConfigMap(baz).generateStatefulSet(openShift))
        );

        // providing the list StatefulSets for already "existing" Kafka clusters
        Labels barLabels = Labels.cluster("bar");
        when(mockSsOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaCluster.fromConfigMap(bar).generateStatefulSet(openShift))
        );

        Labels bazLabels = Labels.cluster("baz");
        when(mockSsOps.list(eq(clusterCmNamespace), eq(bazLabels))).thenReturn(
                asList(KafkaCluster.fromConfigMap(baz).generateStatefulSet(openShift))
        );


        Set<String> created = new HashSet<>();
        Set<String> updated = new HashSet<>();
        Set<String> deleted = new HashSet<>();

        KafkaClusterOperations ops = new KafkaClusterOperations(vertx, openShift,
                ClusterControllerConfig.DEFAULT_OPERATION_TIMEOUT_MS,
                mockCmOps,
                mockServiceOps, mockSsOps,
                mockPvcOps, mockPodOps, mockEndpointOps, mockDepOps) {
            @Override
            public void create(String namespace, String name, Handler h) {
                created.add(name);
                async.countDown();
                h.handle(Future.succeededFuture());
            }
            @Override
            public void update(String namespace, String name, Handler h) {
                updated.add(name);
                async.countDown();
                h.handle(Future.succeededFuture());
            }
            @Override
            public void delete(String namespace, String name, Handler h) {
                deleted.add(name);
                async.countDown();
                h.handle(Future.succeededFuture());
            }
        };

        // Now try to reconcile all the Kafka clusters
        ops.reconcileAll(clusterCmNamespace, Labels.EMPTY);

        async.await();

        context.assertEquals(singleton("foo"), created);
        context.assertEquals(singleton("bar"), updated);
        context.assertEquals(singleton("baz"), deleted);
    }
}
