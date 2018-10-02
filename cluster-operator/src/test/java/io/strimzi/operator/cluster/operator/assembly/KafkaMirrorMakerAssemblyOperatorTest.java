/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpecBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaMirrorMakerCluster;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaMirrorMakerAssemblyOperatorTest {

    protected static Vertx vertx;
    public static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";
    public static final String LOGGING_CONFIG = "#Do not change this generated file. Logging can be configured in the corresponding kubernetes/openshift resource.\n" +
            "\n" +
            "log4j.rootLogger=${mirrormaker.root.logger}, CONSOLE\n" +
            "mirrormaker.root.logger=INFO\n" +
            "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
            "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
            "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n";
    private final String producerBootstrapServers = "foo-kafka:9092";
    private final String consumerBootstrapServers = "bar-kafka:9092";
    private final String groupId = "my-group-id";
    private final int numStreams = 2;
    private final String whitelist = ".*";
    private final String image = "my-image:latest";

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
        CrdOperator mockMirrorOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentOperator mockDcOps = mock(DeploymentOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator mockPolicyOps = mock(NetworkPolicyOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockMirrorOps,
                mockSecretOps,
                mockCmOps,
                mockPolicyOps,
                mockDcOps,
                mockServiceOps);

        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.MIRRORMAKER, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(mirror.getServiceName(), service.getMetadata().getName());
            context.assertEquals(mirror.generateService(), service, "Services are not equal");

            // No metrics config  => no CMs created
            Set<String> metricsNames = new HashSet<>();
            if (mirror.isMetricsEnabled()) {
                metricsNames.add(KafkaMirrorMakerCluster.logAndMetricsConfigName(clusterCmName));
            }

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            Deployment dc = capturedDc.get(0);
            context.assertEquals(mirror.getName(), dc.getMetadata().getName());
            Map annotations = new HashMap();
            annotations.put("strimzi.io/logging", LOGGING_CONFIG);
            context.assertEquals(mirror.generateDeployment(annotations), dc, "Deployments are not equal");

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterNoDiff(TestContext context) {
        CrdOperator mockMirrorOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentOperator mockDcOps = mock(DeploymentOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator mockPolicyOps = mock(NetworkPolicyOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);

        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm);
        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateService());
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>()));

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(eq(clusterCmNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(clusterCmNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(clusterCmNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(clusterCmNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockMirrorOps,
                mockSecretOps,
                mockCmOps,
                mockPolicyOps,
                mockDcOps,
                mockServiceOps);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.MIRRORMAKER, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());

            // Verify Deployment Config
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());

            // Verify scaleDown / scaleUp were not called
            context.assertEquals(1, dcScaleDownNameCaptor.getAllValues().size());
            context.assertEquals(1, dcScaleUpNameCaptor.getAllValues().size());

            async.complete();
        });
    }

    @Test
    public void testUpdateCluster(TestContext context) {
        CrdOperator mockMirrorOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentOperator mockDcOps = mock(DeploymentOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator mockPolicyOps = mock(NetworkPolicyOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCmP = new HashMap<>();
        metricsCmP.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCmP);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateService());
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>()));

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(eq(clusterCmNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(clusterCmNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(clusterCmNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(clusterCmNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        // Mock CM get
        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaMirrorMakerCluster.logAndMetricsConfigName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(clusterCmNamespace, KafkaMirrorMakerCluster.logAndMetricsConfigName(clusterCmName))).thenReturn(metricsCm);

        ConfigMap loggingCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaMirrorMakerCluster.logAndMetricsConfigName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                    .endMetadata()
                    .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG, LOGGING_CONFIG))
                    .build();

        when(mockCmOps.get(clusterCmNamespace, KafkaMirrorMakerCluster.logAndMetricsConfigName(clusterCmName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(clusterCmNamespace), anyString(), any());

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockMirrorOps,
                mockSecretOps,
                mockCmOps,
                mockPolicyOps,
                mockDcOps,
                mockServiceOps);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.MIRRORMAKER, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            KafkaMirrorMakerCluster compareTo = KafkaMirrorMakerCluster.fromCrd(clusterCm);

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(compareTo.getServiceName(), service.getMetadata().getName());
            context.assertEquals(compareTo.generateService(), service, "Services are not equal");

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            Deployment dc = capturedDc.get(0);
            context.assertEquals(compareTo.getName(), dc.getMetadata().getName());
            Map<String, String> annotations = new HashMap();
            annotations.put("strimzi.io/logging", loggingCm.getData().get(compareTo.ANCILLARY_CM_KEY_LOG_CONFIG));
            context.assertEquals(compareTo.generateDeployment(annotations), dc, "Deployments are not equal");

            // Verify scaleDown / scaleUp were not called
            context.assertEquals(1, dcScaleDownNameCaptor.getAllValues().size());
            context.assertEquals(1, dcScaleUpNameCaptor.getAllValues().size());

            // No metrics config  => no CMs created
            verify(mockCmOps, never()).createOrUpdate(any());
            async.complete();
        });
    }

    @Test
    public void testUpdateClusterFailure(TestContext context) {
        CrdOperator mockMirrorOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentOperator mockDcOps = mock(DeploymentOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator mockPolicyOps = mock(NetworkPolicyOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateService());
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>()));

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.failedFuture("Failed"));

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockMirrorOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockMirrorOps,
                mockSecretOps,
                mockCmOps,
                mockPolicyOps,
                mockDcOps,
                mockServiceOps);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.MIRRORMAKER, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertFalse(createResult.succeeded());

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterScaleUp(TestContext context) {
        final int scaleTo = 4;

        CrdOperator mockMirrorOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentOperator mockDcOps = mock(DeploymentOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator mockPolicyOps = mock(NetworkPolicyOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateService());
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>()));

        when(mockServiceOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, mirror.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, mirror.getName(), scaleTo);

        when(mockMirrorOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockMirrorOps,
                mockSecretOps,
                mockCmOps,
                mockPolicyOps,
                mockDcOps,
                mockServiceOps);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.MIRRORMAKER, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            verify(mockDcOps).scaleUp(clusterCmNamespace, mirror.getName(), scaleTo);

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterScaleDown(TestContext context) {
        int scaleTo = 2;

        CrdOperator mockMirrorOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentOperator mockDcOps = mock(DeploymentOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator mockPolicyOps = mock(NetworkPolicyOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateService());
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>()));

        when(mockServiceOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, mirror.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, mirror.getName(), scaleTo);

        when(mockMirrorOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockMirrorOps,
                mockSecretOps,
                mockCmOps,
                mockPolicyOps,
                mockDcOps,
                mockServiceOps);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.MIRRORMAKER, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            verify(mockDcOps).scaleUp(clusterCmNamespace, mirror.getName(), scaleTo);

            async.complete();
        });
    }

    @Test
    public void testReconcile(TestContext context) {
        CrdOperator mockMirrorOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        DeploymentOperator mockDcOps = mock(DeploymentOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator mockPolicyOps = mock(NetworkPolicyOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);

        String clusterCmNamespace = "test";


        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");

        KafkaMirrorMaker foo = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, "foo", image, producer, consumer, whitelist, metricsCm);
        KafkaMirrorMaker bar = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, "bar", image, producer, consumer, whitelist, metricsCm);

        when(mockMirrorOps.list(eq(clusterCmNamespace), any())).thenReturn(asList(foo, bar));
        // when requested ConfigMap for a specific Kafka Mirror Maker cluster
        when(mockMirrorOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockMirrorOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);

        // providing the list of ALL Deployments for all the Kafka Mirror Maker clusters
        Labels newLabels = Labels.forKind(KafkaMirrorMaker.RESOURCE_KIND);
        when(mockDcOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaMirrorMakerCluster.fromCrd(bar).generateDeployment(new HashMap<String, String>())));

        // providing the list Deployments for already "existing" Kafka Mirror Maker clusters
        Labels barLabels = Labels.forCluster("bar");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaMirrorMakerCluster.fromCrd(bar).generateDeployment(new HashMap<String, String>()))
        );

        when(mockSecretOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();
        Set<String> deleted = new CopyOnWriteArraySet<>();

        Async async = context.async(2);

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockMirrorOps,
                mockSecretOps,
                mockCmOps,
                mockPolicyOps,
                mockDcOps,
                mockServiceOps) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker kafkaMirrorMakerAssembly) {
                createdOrUpdated.add(kafkaMirrorMakerAssembly.getMetadata().getName());
                async.countDown();
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka Mirror Maker clusters
        ops.reconcileAll("test", clusterCmNamespace);

        async.await();

        context.assertEquals(new HashSet(asList("foo", "bar")), createdOrUpdated);
    }

}
