/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaConnectS2ICluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentConfigOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
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

import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaConnectS2IAssemblyOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef"),
            emptyMap(), emptyMap(), singletonMap("2.0.0", "strimzi/kafka-connect:latest-kafka-2.0.0"),
            emptyMap()) { };
    protected static Vertx vertx;
    private static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";
    private static final String LOGGING_CONFIG = AbstractModel.getOrderedProperties("kafkaConnectDefaultLoggingProperties")
            .asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding kubernetes/openshift resource.");

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
        CrdOperator mockConnectOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentConfigOperator mockDcOps = mock(DeploymentConfigOperator.class);
        BuildConfigOperator mockBcOps = mock(BuildConfigOperator.class);
        ImageStreamOperator mockIsOps = mock(ImageStreamOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator networkPolicyOperator = mock(NetworkPolicyOperator.class);
        PodDisruptionBudgetOperator mockPdbOps = mock(PodDisruptionBudgetOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.reconcile(anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));

        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.reconcile(anyString(), anyString(), isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(anyString(), anyString(), bcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> pdbNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> pdbNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(pdbNamespaceCaptor.capture(), pdbNameCaptor.capture(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockConnectOps,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps, mockSecretOps, networkPolicyOperator, mockPdbOps, supplier, VERSIONS, null);

        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.CONNECT_S2I, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(connect.getServiceName(), service.getMetadata().getName());
            context.assertEquals(connect.generateService(), service, "Services are not equal");

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            DeploymentConfig dc = capturedDc.get(0);
            context.assertEquals(connect.getName(), dc.getMetadata().getName());
            Map annotations = new HashMap();
            annotations.put("strimzi.io/logging", LOGGING_CONFIG);
            context.assertEquals(connect.generateDeploymentConfig(annotations, true, null), dc, "Deployment Configs are not equal");

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.assertEquals(1, capturedBc.size());
            BuildConfig bc = capturedBc.get(0);
            context.assertEquals(connect.getName(), dc.getMetadata().getName());
            context.assertEquals(connect.generateBuildConfig(), bc, "Build Configs are not equal");

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.assertEquals(1, capturedPdb.size());
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.assertEquals(connect.getName(), pdb.getMetadata().getName());
            context.assertEquals(connect.generatePodDisruptionBudget(), pdb, "PodDisruptionBudgets are not equal");

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.assertEquals(2, capturedIs.size());
            int sisIndex = (connect.getSourceImageStreamName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;
            int tisIndex = (connect.getName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;

            ImageStream sis = capturedIs.get(sisIndex);
            context.assertEquals(connect.getSourceImageStreamName(), sis.getMetadata().getName());
            context.assertEquals(connect.generateSourceImageStream(), sis, "Source Image Streams are not equal");

            ImageStream tis = capturedIs.get(tisIndex);
            context.assertEquals(connect.getName(), tis.getMetadata().getName());
            context.assertEquals(connect.generateTargetImageStream(), tis, "Target Image Streams are not equal");

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterNoDiff(TestContext context) {
        CrdOperator mockConnectOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentConfigOperator mockDcOps = mock(DeploymentConfigOperator.class);
        BuildConfigOperator mockBcOps = mock(BuildConfigOperator.class);
        ImageStreamOperator mockIsOps = mock(ImageStreamOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator networkPolicyOperator = mock(NetworkPolicyOperator.class);
        PodDisruptionBudgetOperator mockPdbOps = mock(PodDisruptionBudgetOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null));
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.reconcile(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> isNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> isNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.reconcile(isNamespaceCaptor.capture(), isNameCaptor.capture(), isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> bcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> bcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(bcNamespaceCaptor.capture(), bcNameCaptor.capture(), bcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> pdbNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> pdbNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(pdbNamespaceCaptor.capture(), pdbNameCaptor.capture(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockConnectOps,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps, mockSecretOps, networkPolicyOperator, mockPdbOps, supplier, VERSIONS, null);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.CONNECT_S2I, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.assertEquals(1, capturedBc.size());

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.assertEquals(1, capturedPdb.size());

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.assertEquals(2, capturedIs.size());

            // Verify scaleDown / scaleUp were not called
            context.assertEquals(1, dcScaleDownNameCaptor.getAllValues().size());
            context.assertEquals(1, dcScaleUpNameCaptor.getAllValues().size());

            async.complete();
        });
    }

    @Test
    public void testUpdateCluster(TestContext context) {
        CrdOperator mockConnectOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentConfigOperator mockDcOps = mock(DeploymentConfigOperator.class);
        BuildConfigOperator mockBcOps = mock(BuildConfigOperator.class);
        ImageStreamOperator mockIsOps = mock(ImageStreamOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator networkPolicyOperator = mock(NetworkPolicyOperator.class);
        PodDisruptionBudgetOperator mockPdbOps = mock(PodDisruptionBudgetOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null));
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.reconcile(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> isNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> isNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.reconcile(isNamespaceCaptor.capture(), isNameCaptor.capture(), isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> bcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> bcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(bcNamespaceCaptor.capture(), bcNameCaptor.capture(), bcCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        ArgumentCaptor<String> pdbNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> pdbNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(pdbNamespaceCaptor.capture(), pdbNameCaptor.capture(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock CM get
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaConnectS2ICluster.logAndMetricsConfigName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(clusterCmNamespace, KafkaConnectS2ICluster.logAndMetricsConfigName(clusterCmName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(clusterCmNamespace), anyString(), any());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockConnectOps,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps, mockSecretOps, networkPolicyOperator, mockPdbOps, supplier, VERSIONS, null);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.CONNECT_S2I, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            KafkaConnectS2ICluster compareTo = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(1, capturedServices.size());
            Service service = capturedServices.get(0);
            context.assertEquals(compareTo.getServiceName(), service.getMetadata().getName());
            context.assertEquals(compareTo.generateService(), service, "Services are not equal");

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.assertEquals(1, capturedDc.size());
            DeploymentConfig dc = capturedDc.get(0);
            context.assertEquals(compareTo.getName(), dc.getMetadata().getName());
            Map annotations = new HashMap();
            annotations.put("strimzi.io/logging", LOGGING_CONFIG);
            context.assertEquals(compareTo.generateDeploymentConfig(annotations, true, null), dc, "Deployment Configs are not equal");

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.assertEquals(1, capturedBc.size());
            BuildConfig bc = capturedBc.get(0);
            context.assertEquals(compareTo.getName(), bc.getMetadata().getName());
            context.assertEquals(compareTo.generateBuildConfig(), bc, "Build Configs are not equal");

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.assertEquals(1, capturedPdb.size());
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.assertEquals(compareTo.getName(), pdb.getMetadata().getName());
            context.assertEquals(compareTo.generatePodDisruptionBudget(), pdb, "PodDisruptionBudgets are not equal");

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.assertEquals(2, capturedIs.size());
            int sisIndex = (compareTo.getSourceImageStreamName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;
            int tisIndex = (compareTo.getName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;

            ImageStream sis = capturedIs.get(sisIndex);
            context.assertEquals(compareTo.getSourceImageStreamName(), sis.getMetadata().getName());
            context.assertEquals(compareTo.generateSourceImageStream(), sis, "Source Image Streams are not equal");

            ImageStream tis = capturedIs.get(tisIndex);
            context.assertEquals(compareTo.getName(), tis.getMetadata().getName());
            context.assertEquals(compareTo.generateTargetImageStream(), tis, "Target Image Streams are not equal");

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterFailure(TestContext context) {
        CrdOperator mockConnectOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentConfigOperator mockDcOps = mock(DeploymentConfigOperator.class);
        BuildConfigOperator mockBcOps = mock(BuildConfigOperator.class);
        ImageStreamOperator mockIsOps = mock(ImageStreamOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator networkPolicyOperator = mock(NetworkPolicyOperator.class);
        PodDisruptionBudgetOperator mockPdbOps = mock(PodDisruptionBudgetOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null));
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.reconcile(dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.failedFuture("Failed"));

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> isNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> isNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ImageStream> isCaptor = ArgumentCaptor.forClass(ImageStream.class);
        when(mockIsOps.reconcile(isNamespaceCaptor.capture(), isNameCaptor.capture(), isCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> bcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> bcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BuildConfig> bcCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(bcNamespaceCaptor.capture(), bcNameCaptor.capture(), bcCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockPdbOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockConnectOps,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps, mockSecretOps, networkPolicyOperator, mockPdbOps, supplier, VERSIONS, null);


        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.CONNECT_S2I, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertFalse(createResult.succeeded());

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterScaleUp(TestContext context) {
        int scaleTo = 4;

        CrdOperator mockConnectOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentConfigOperator mockDcOps = mock(DeploymentConfigOperator.class);
        BuildConfigOperator mockBcOps = mock(BuildConfigOperator.class);
        ImageStreamOperator mockIsOps = mock(ImageStreamOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator networkPolicyOperator = mock(NetworkPolicyOperator.class);
        PodDisruptionBudgetOperator mockPdbOps = mock(PodDisruptionBudgetOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null));
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());

        when(mockServiceOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, connect.getName(), scaleTo);

        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockIsOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockBcOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockConnectOps,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps, mockSecretOps, networkPolicyOperator, mockPdbOps, supplier, VERSIONS, null);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.CONNECT_S2I, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            verify(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

            async.complete();
        });
    }

    @Test
    public void testUpdateClusterScaleDown(TestContext context) {
        int scaleTo = 2;

        CrdOperator mockConnectOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentConfigOperator mockDcOps = mock(DeploymentConfigOperator.class);
        BuildConfigOperator mockBcOps = mock(BuildConfigOperator.class);
        ImageStreamOperator mockIsOps = mock(ImageStreamOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator networkPolicyOperator = mock(NetworkPolicyOperator.class);
        PodDisruptionBudgetOperator mockPdbOps = mock(PodDisruptionBudgetOperator.class);

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null));
        when(mockIsOps.get(clusterCmNamespace, connect.getSourceImageStreamName())).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());

        when(mockServiceOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), 2);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, connect.getName(), scaleTo);

        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockIsOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockBcOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockConnectOps,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps, mockSecretOps, networkPolicyOperator, mockPdbOps, supplier, VERSIONS, null);

        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.CONNECT_S2I, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.assertTrue(createResult.succeeded());

            // Verify ScaleDown
            verify(mockDcOps).scaleDown(clusterCmNamespace, connect.getName(), scaleTo);

            async.complete();
        });
    }

    @Test
    public void testReconcile(TestContext context) {
        CrdOperator mockConnectOps = mock(CrdOperator.class);
        ConfigMapOperator mockCmOps = mock(ConfigMapOperator.class);
        ServiceOperator mockServiceOps = mock(ServiceOperator.class);
        DeploymentConfigOperator mockDcOps = mock(DeploymentConfigOperator.class);
        BuildConfigOperator mockBcOps = mock(BuildConfigOperator.class);
        ImageStreamOperator mockIsOps = mock(ImageStreamOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        NetworkPolicyOperator networkPolicyOperator = mock(NetworkPolicyOperator.class);
        PodDisruptionBudgetOperator mockPdbOps = mock(PodDisruptionBudgetOperator.class);

        String clusterCmNamespace = "test";

        KafkaConnectS2I foo = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, "foo");
        KafkaConnectS2I bar = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, "bar");
        when(mockConnectOps.list(eq(clusterCmNamespace), any())).thenReturn(asList(foo, bar));
        // when requested ConfigMap for a specific Kafka Connect S2I cluster
        when(mockConnectOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockConnectOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);

        // providing the list of ALL DeploymentConfigs for all the Kafka Connect S2I clusters
        Labels newLabels = Labels.forKind(KafkaConnectS2I.RESOURCE_KIND);
        when(mockDcOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaConnectS2ICluster.fromCrd(bar, VERSIONS).generateDeploymentConfig(new HashMap<String, String>(), true, null)));

        // providing the list DeploymentConfigs for already "existing" Kafka Connect S2I clusters
        Labels barLabels = Labels.forCluster("bar");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaConnectS2ICluster.fromCrd(bar, VERSIONS).generateDeploymentConfig(new HashMap<String, String>(), true, null))
        );

        when(mockSecretOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        Async async = context.async(2);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, true,
                new MockCertManager(),
                mockConnectOps,
                mockCmOps, mockDcOps, mockServiceOps, mockIsOps, mockBcOps, mockSecretOps, networkPolicyOperator, mockPdbOps, supplier, VERSIONS, null) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnectS2I kafkaConnectS2IAssembly) {
                createdOrUpdated.add(kafkaConnectS2IAssembly.getMetadata().getName());
                async.countDown();
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka Connect S2I clusters
        ops.reconcileAll("test", clusterCmNamespace);

        async.await();

        context.assertEquals(new HashSet(asList("foo", "bar")), createdOrUpdated);
    }

}
