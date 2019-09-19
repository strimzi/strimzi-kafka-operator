/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaConnectS2ICluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
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
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaConnectS2IAssemblyOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    protected static Vertx vertx;
    private static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";
    private static final String LOGGING_CONFIG = AbstractModel.getOrderedProperties("kafkaConnectDefaultLoggingProperties")
            .asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding kubernetes/openshift resource.");

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_9;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectS2IOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(supplier.connectOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<DeploymentConfig> dcCaptor = ArgumentCaptor.forClass(DeploymentConfig.class);
        when(mockDcOps.reconcile(anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

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

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);

        ArgumentCaptor<KafkaConnectS2I> connectCaptor = ArgumentCaptor.forClass(KafkaConnectS2I.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedServices.size(), is(1)));
            Service service = capturedServices.get(0);
            context.verify(() -> assertThat(service.getMetadata().getName(), is(connect.getServiceName())));
            context.verify(() -> assertThat("Services are not equal", service, is(connect.generateService())));

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedDc.size(), is(1)));
            DeploymentConfig dc = capturedDc.get(0);
            context.verify(() -> assertThat(dc.getMetadata().getName(), is(connect.getName())));
            Map annotations = new HashMap();
            annotations.put("strimzi.io/logging", LOGGING_CONFIG);
            context.verify(() -> assertThat("Deployment Configs are not equal", dc, is(connect.generateDeploymentConfig(annotations, true, null, null))));

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedBc.size(), is(1)));
            BuildConfig bc = capturedBc.get(0);
            context.verify(() -> assertThat(dc.getMetadata().getName(), is(connect.getName())));
            context.verify(() -> assertThat("Build Configs are not equal", bc, is(connect.generateBuildConfig())));

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.verify(() -> assertThat(capturedPdb.size(), is(1)));
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.verify(() -> assertThat(pdb.getMetadata().getName(), is(connect.getName())));
            context.verify(() -> assertThat("PodDisruptionBudgets are not equal", pdb, is(connect.generatePodDisruptionBudget())));

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.verify(() -> assertThat(capturedIs.size(), is(2)));
            int sisIndex = (KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster())).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;
            int tisIndex = (connect.getName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;

            ImageStream sis = capturedIs.get(sisIndex);
            context.verify(() -> assertThat(sis.getMetadata().getName(), is(KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))));
            context.verify(() -> assertThat("Source Image Streams are not equal", sis, is(connect.generateSourceImageStream())));

            ImageStream tis = capturedIs.get(tisIndex);
            context.verify(() -> assertThat(tis.getMetadata().getName(), is(connect.getName())));
            context.verify(() -> assertThat("Target Image Streams are not equal", tis, is(connect.generateTargetImageStream())));

            // Verify status
            List<KafkaConnectS2I> capturedConnects = connectCaptor.getAllValues();
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getUrl(), is("http://foo-connect-api.test.svc:8083")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("Ready")));

            async.flag();
        });
    }

    @Test
    public void testUpdateClusterNoDiff(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectS2IOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(supplier.connectOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(clusterCmNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
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

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedServices.size(), is(1)));

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedDc.size(), is(1)));

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedBc.size(), is(1)));

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.verify(() -> assertThat(capturedPdb.size(), is(1)));

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.verify(() -> assertThat(capturedIs.size(), is(2)));

            // Verify scaleDown / scaleUp were not called
            context.verify(() -> assertThat(dcScaleDownNameCaptor.getAllValues().size(), is(1)));
            context.verify(() -> assertThat(dcScaleUpNameCaptor.getAllValues().size(), is(1)));

            async.flag();
        });
    }

    @SuppressWarnings("checkstyle:JavaNCSS")
    @Test
    public void testUpdateCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectS2IOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(supplier.connectOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(clusterCmNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
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
                    .withName(KafkaConnectS2IResources.metricsAndLogConfigMapName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(clusterCmNamespace, KafkaConnectS2IResources.metricsAndLogConfigMapName(clusterCmName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(clusterCmNamespace), anyString(), any());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            KafkaConnectS2ICluster compareTo = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedServices.size(), is(1)));
            Service service = capturedServices.get(0);
            context.verify(() -> assertThat(service.getMetadata().getName(), is(compareTo.getServiceName())));
            context.verify(() -> assertThat("Services are not equal", service, is(compareTo.generateService())));

            // Verify Deployment Config
            List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedDc.size(), is(1)));
            DeploymentConfig dc = capturedDc.get(0);
            context.verify(() -> assertThat(dc.getMetadata().getName(), is(compareTo.getName())));
            Map annotations = new HashMap();
            annotations.put("strimzi.io/logging", LOGGING_CONFIG);
            context.verify(() -> assertThat("Deployment Configs are not equal", dc, is(compareTo.generateDeploymentConfig(annotations, true, null, null))));

            // Verify Build Config
            List<BuildConfig> capturedBc = bcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedBc.size(), is(1)));
            BuildConfig bc = capturedBc.get(0);
            context.verify(() -> assertThat(bc.getMetadata().getName(), is(compareTo.getName())));
            context.verify(() -> assertThat("Build Configs are not equal", bc, is(compareTo.generateBuildConfig())));

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.verify(() -> assertThat(capturedPdb.size(), is(1)));
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.verify(() -> assertThat(pdb.getMetadata().getName(), is(compareTo.getName())));
            context.verify(() -> assertThat("PodDisruptionBudgets are not equal", pdb, is(compareTo.generatePodDisruptionBudget())));

            // Verify Image Streams
            List<ImageStream> capturedIs = isCaptor.getAllValues();
            context.verify(() -> assertThat(capturedIs.size(), is(2)));
            int sisIndex = (KafkaConnectS2IResources.sourceImageStreamName(compareTo.getCluster())).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;
            int tisIndex = (compareTo.getName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;

            ImageStream sis = capturedIs.get(sisIndex);
            context.verify(() -> assertThat(sis.getMetadata().getName(), is(KafkaConnectS2IResources.sourceImageStreamName(compareTo.getCluster()))));
            context.verify(() -> assertThat("Source Image Streams are not equal", sis, is(compareTo.generateSourceImageStream())));

            ImageStream tis = capturedIs.get(tisIndex);
            context.verify(() -> assertThat(tis.getMetadata().getName(), is(compareTo.getName())));
            context.verify(() -> assertThat("Target Image Streams are not equal", tis, is(compareTo.generateTargetImageStream())));

            async.flag();
        });
    }

    @Test
    public void testUpdateClusterFailure(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectS2IOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator networkPolicyOperator = supplier.networkPolicyOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(clusterCmNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(clusterCmNamespace, KafkaConnectS2IResources.targetImageStreamName(connect.getCluster()))).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(clusterCmNamespace, KafkaConnectS2IResources.buildConfigName(connect.getCluster()))).thenReturn(connect.generateBuildConfig());
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
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockPdbOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(false)));

            async.flag();
        });
    }

    @Test
    public void testUpdateClusterScaleUp(VertxTestContext context) {
        int scaleTo = 4;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectS2IOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator networkPolicyOperator = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(supplier.connectOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(clusterCmNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
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

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            verify(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

            async.flag();
        });
    }

    @Test
    public void testUpdateClusterScaleDown(VertxTestContext context) {
        int scaleTo = 2;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectS2IOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator networkPolicyOperator = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(supplier.connectOperator.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(clusterCmNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
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

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            // Verify ScaleDown
            verify(mockDcOps).scaleDown(clusterCmNamespace, connect.getName(), scaleTo);

            async.flag();
        });
    }

    @Test
    public void testReconcile(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectS2IOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator networkPolicyOperator = supplier.networkPolicyOperator;

        String clusterCmNamespace = "test";

        KafkaConnectS2I foo = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, "foo");
        KafkaConnectS2I bar = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, "bar");
        when(mockConnectOps.listAsync(eq(clusterCmNamespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        // when requested ConfigMap for a specific Kafka Connect S2I cluster
        when(mockConnectOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockConnectOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(bar));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());

        // providing the list of ALL DeploymentConfigs for all the Kafka Connect S2I clusters
        Labels newLabels = Labels.forKind(KafkaConnectS2I.RESOURCE_KIND);
        when(mockDcOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaConnectS2ICluster.fromCrd(bar, VERSIONS).generateDeploymentConfig(new HashMap<String, String>(), true, null, null)));

        // providing the list DeploymentConfigs for already "existing" Kafka Connect S2I clusters
        Labels barLabels = Labels.forCluster("bar");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaConnectS2ICluster.fromCrd(bar, VERSIONS).generateDeploymentConfig(new HashMap<String, String>(), true, null, null))
        );
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        CountDownLatch async = new CountDownLatch(2);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnectS2I kafkaConnectS2IAssembly) {
                createdOrUpdated.add(kafkaConnectS2IAssembly.getMetadata().getName());
                async.countDown();
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka Connect S2I clusters
        ops.reconcileAll("test", clusterCmNamespace, ignored -> { });

        async.await(60, TimeUnit.SECONDS);
        context.verify(() -> assertThat(createdOrUpdated, is(new HashSet(asList("foo", "bar")))));
        context.completeNow();
    }

    @Test
    public void testCreateClusterStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectS2IOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        String failureMessage = "failure";

        KafkaConnectS2I clusterCm = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockServiceOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMessage));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockBcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);

        ArgumentCaptor<KafkaConnectS2I> connectCaptor = ArgumentCaptor.forClass(KafkaConnectS2I.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(false)));

            // Verify status
            List<KafkaConnectS2I> capturedConnects = connectCaptor.getAllValues();
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getUrl(), is("http://foo-connect-api.test.svc:8083")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("NotReady")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getMessage(), is(failureMessage)));

            async.flag();
        });
    }

}
