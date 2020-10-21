/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.connect.ConnectorPluginBuilder;
import io.strimzi.api.kafka.model.status.KafkaConnectS2IStatus;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaConnectS2ICluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
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
            .asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.");

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_9;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    public void createCluster(VertxTestContext context, KafkaConnectS2I kcs2i, boolean connectorOperator) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectS2IOps.get(kcs2i.getMetadata().getNamespace(), kcs2i.getMetadata().getName())).thenReturn(kcs2i);
        when(mockConnectS2IOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kcs2i));

        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));

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

        when(mockConnectS2IOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnectS2I())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(eq(kcs2i.getMetadata().getNamespace()), eq(KafkaConnectS2IResources.deploymentName(kcs2i.getMetadata().getName())), npCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);

        ArgumentCaptor<KafkaConnectS2I> connectCaptor = ArgumentCaptor.forClass(KafkaConnectS2I.class);
        when(mockConnectS2IOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(kcs2i, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, kcs2i.getMetadata().getNamespace(), kcs2i.getMetadata().getName()), kcs2i)
            .onComplete(context.succeeding(v -> context.verify(() -> {

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(connect.getServiceName()));
                assertThat(service, is(connect.generateService()));

                // Verify Deployment Config
                List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                DeploymentConfig dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(connect.getName()));
                Map annotations = new HashMap();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH, Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(LOGGING_CONFIG)));
                assertThat(dc, is(connect.generateDeploymentConfig(annotations, true, null, null)));

                // Verify Build Config
                List<BuildConfig> capturedBc = bcCaptor.getAllValues();
                assertThat(capturedBc, hasSize(1));
                BuildConfig bc = capturedBc.get(0);
                assertThat(dc.getMetadata().getName(), is(connect.getName()));
                assertThat(bc, is(connect.generateBuildConfig()));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(connect.getName()));
                assertThat(pdb, is(connect.generatePodDisruptionBudget()));

                // Verify Image Streams
                List<ImageStream> capturedIs = isCaptor.getAllValues();
                assertThat(capturedIs, hasSize(2));
                int sourceImageStreamIndex = (KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster())).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;
                int targetImageStreamIndex = (connect.getName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;

                ImageStream source = capturedIs.get(sourceImageStreamIndex);
                assertThat(source.getMetadata().getName(), is(KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster())));
                assertThat(source, is(connect.generateSourceImageStream()));

                ImageStream target = capturedIs.get(targetImageStreamIndex);
                assertThat(target.getMetadata().getName(), is(connect.getName()));
                assertThat(target, is(connect.generateTargetImageStream()));

                // Verify status
                List<KafkaConnectS2I> capturedConnects = connectCaptor.getAllValues();
                KafkaConnectS2IStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getUrl(), is("http://foo-connect-api.test.svc:8083"));
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                if (connectorOperator) {
                    assertThat(npCaptor.getValue(), is(notNullValue()));
                    assertThat(connectStatus.getConnectorPlugins(), hasSize(1));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getConnectorClass(), is("io.strimzi.MyClass"));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getType(), is("sink"));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getVersion(), is("1.0.0"));
                } else {
                    assertThat(npCaptor.getValue(), is(nullValue()));
                    assertThat(connectStatus.getConnectorPlugins(), nullValue());
                }

                async.flag();
            })));
    }

    @Test
    public void testCreateClusterWithoutConnectorOperator(VertxTestContext context) {
        String kcs2iName = "foo";
        String kcs2iNamespace = "test";
        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);

        createCluster(context, kcs2i, false);
    }

    @Test
    public void testCreateClusterWithConnectorOperator(VertxTestContext context) {
        String kcs2iName = "foo";
        String kcs2iNamespace = "test";
        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);
        kcs2i.getMetadata().getAnnotations().put(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true");

        createCluster(context, kcs2i, true);
    }

    @Test
    public void testCreateOrUpdateDoesNotUpdateWithNoDiff(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcs2iName = "foo";
        String kcs2iNamespace = "test";

        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(kcs2i, VERSIONS);
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectS2IOps.get(kcs2iNamespace, kcs2iName)).thenReturn(kcs2i);
        when(mockConnectS2IOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kcs2i));
        when(mockConnectS2IOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(kcs2iNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());

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

        when(mockConnectS2IOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnectS2I())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(eq(kcs2i.getMetadata().getNamespace()), eq(KafkaConnectS2IResources.deploymentName(kcs2i.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, kcs2iNamespace, kcs2iName), kcs2i)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));

                // Verify Deployment Config
                List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));

                // Verify Build Config
                List<BuildConfig> capturedBc = bcCaptor.getAllValues();
                assertThat(capturedBc, hasSize(1));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));

                // Verify Image Streams
                List<ImageStream> capturedIs = isCaptor.getAllValues();
                assertThat(capturedIs, hasSize(2));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                async.flag();
            })));
    }

    @SuppressWarnings({"checkstyle:JavaNCSS", "checkstyle:MethodLength"})
    @Test
    public void testUpdateCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcs2iName = "foo";
        String kcs2iNamespace = "test";

        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(kcs2i, VERSIONS);
        kcs2i.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectS2IOps.get(kcs2iNamespace, kcs2iName)).thenReturn(kcs2i);
        when(mockConnectS2IOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kcs2i));
        when(mockConnectS2IOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(kcs2iNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());

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

        when(mockNetPolOps.reconcile(eq(kcs2i.getMetadata().getNamespace()), eq(KafkaConnectS2IResources.deploymentName(kcs2i.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockConnectS2IOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnectS2I())));

        ArgumentCaptor<String> pdbNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> pdbNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(pdbNamespaceCaptor.capture(), pdbNameCaptor.capture(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock CM get
        when(mockConnectS2IOps.get(kcs2iNamespace, kcs2iName)).thenReturn(kcs2i);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaConnectS2IResources.metricsAndLogConfigMapName(kcs2iName))
                    .withNamespace(kcs2iNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(kcs2iNamespace, KafkaConnectS2IResources.metricsAndLogConfigMapName(kcs2iName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(kcs2iNamespace), anyString(), any());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, kcs2iNamespace, kcs2iName), kcs2i)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                KafkaConnectS2ICluster compareTo = KafkaConnectS2ICluster.fromCrd(kcs2i, VERSIONS);

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(compareTo.getServiceName()));
                assertThat(service, is(compareTo.generateService()));

                // Verify Deployment Config
                List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                DeploymentConfig dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(compareTo.getName()));
                Map annotations = new HashMap();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH, Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(LOGGING_CONFIG)));
                assertThat(dc, is(compareTo.generateDeploymentConfig(annotations, true, null, null)));

                // Verify Build Config
                List<BuildConfig> capturedBc = bcCaptor.getAllValues();
                assertThat(capturedBc, hasSize(1));
                BuildConfig bc = capturedBc.get(0);
                assertThat(bc.getMetadata().getName(), is(compareTo.getName()));
                assertThat(bc, is(compareTo.generateBuildConfig()));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(compareTo.getName()));
                assertThat(pdb, is(compareTo.generatePodDisruptionBudget()));

                // Verify Image Streams
                List<ImageStream> capturedIs = isCaptor.getAllValues();
                assertThat(capturedIs, hasSize(2));
                int sourceImageStreamIndex = (KafkaConnectS2IResources.sourceImageStreamName(compareTo.getCluster())).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;
                int targetImageStreamIndex = (compareTo.getName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;

                ImageStream source = capturedIs.get(sourceImageStreamIndex);
                assertThat(source.getMetadata().getName(), is(KafkaConnectS2IResources.sourceImageStreamName(compareTo.getCluster())));
                assertThat(source, is(compareTo.generateSourceImageStream()));

                ImageStream target = capturedIs.get(targetImageStreamIndex);
                assertThat(target.getMetadata().getName(), is(compareTo.getName()));
                assertThat(target, is(compareTo.generateTargetImageStream()));

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateFailsWhenDeploymentUpdateFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        String kcs2iName = "foo";
        String kcs2iNamespace = "test";

        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(kcs2i, VERSIONS);
        kcs2i.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectS2IOps.get(kcs2iNamespace, kcs2iName)).thenReturn(kcs2i);
        when(mockServiceOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(kcs2iNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(kcs2iNamespace, KafkaConnectS2IResources.targetImageStreamName(connect.getCluster()))).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(kcs2iNamespace, KafkaConnectS2IResources.buildConfigName(connect.getCluster()))).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());
        when(mockConnectOps.getAsync(kcs2iNamespace, kcs2iName)).thenReturn(Future.succeededFuture(null));

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

        when(mockConnectS2IOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnectS2I())));
        when(mockConnectS2IOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kcs2i));
        when(mockConnectS2IOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockNetPolOps.reconcile(eq(kcs2i.getMetadata().getNamespace()), eq(KafkaConnectS2IResources.deploymentName(kcs2i.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, kcs2iNamespace, kcs2iName), kcs2i)
            .onComplete(context.failing(v -> context.verify(() -> async.flag())));
    }

    @Test
    public void testUpdateClusterScaleUp(VertxTestContext context) {
        int scaleTo = 4;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcs2iName = "foo";
        String kcs2iNamespace = "test";

        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(kcs2i, VERSIONS);
        kcs2i.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectS2IOps.get(kcs2iNamespace, kcs2iName)).thenReturn(kcs2i);
        when(mockConnectS2IOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kcs2i));
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));
        when(mockConnectS2IOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(kcs2iNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());

        when(mockServiceOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(kcs2iNamespace, connect.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(kcs2iNamespace, connect.getName(), scaleTo);

        when(mockConnectS2IOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnectS2I())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockIsOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockBcOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockNetPolOps.reconcile(eq(kcs2i.getMetadata().getNamespace()), eq(KafkaConnectS2IResources.deploymentName(kcs2i.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, kcs2iNamespace, kcs2iName), kcs2i)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(kcs2iNamespace, connect.getName(), scaleTo);
                async.flag();
            })));
    }

    @Test
    public void testUpdateClusterScaleDown(VertxTestContext context) {
        int scaleTo = 2;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcs2iName = "foo";
        String kcs2iNamespace = "test";

        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);
        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(kcs2i, VERSIONS);
        kcs2i.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectS2IOps.get(kcs2iNamespace, kcs2iName)).thenReturn(kcs2i);
        when(mockConnectS2IOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kcs2i));
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));
        when(mockConnectS2IOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateDeploymentConfig(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.get(kcs2iNamespace, KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster()))).thenReturn(connect.generateSourceImageStream());
        when(mockIsOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateTargetImageStream());
        when(mockBcOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generateBuildConfig());
        when(mockPdbOps.get(kcs2iNamespace, connect.getName())).thenReturn(connect.generatePodDisruptionBudget());

        when(mockServiceOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(kcs2iNamespace, connect.getName(), 2);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(kcs2iNamespace, connect.getName(), scaleTo);

        when(mockConnectS2IOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnectS2I())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockIsOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockBcOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockNetPolOps.reconcile(eq(kcs2i.getMetadata().getNamespace()), eq(KafkaConnectS2IResources.deploymentName(kcs2i.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, kcs2iNamespace, kcs2iName), kcs2i)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify ScaleDown
                verify(mockDcOps).scaleDown(kcs2iNamespace, connect.getName(), scaleTo);
                async.flag();
            })));
    }

    @Test
    public void testReconcile(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kcs2iNamespace = "test";

        KafkaConnectS2I foo = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, "foo");
        KafkaConnectS2I bar = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, "bar");
        when(mockConnectS2IOps.listAsync(eq(kcs2iNamespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        // when requested ConfigMap for a specific Kafka Connect S2I cluster
        when(mockConnectS2IOps.get(eq(kcs2iNamespace), eq("foo"))).thenReturn(foo);
        when(mockConnectS2IOps.get(eq(kcs2iNamespace), eq("bar"))).thenReturn(bar);
        when(mockConnectS2IOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(bar));
        when(mockConnectS2IOps.updateStatusAsync(any(KafkaConnectS2I.class))).thenReturn(Future.succeededFuture());

        // providing the list of ALL DeploymentConfigs for all the Kafka Connect S2I clusters
        Labels newLabels = Labels.forStrimziKind(KafkaConnectS2I.RESOURCE_KIND);
        when(mockDcOps.list(eq(kcs2iNamespace), eq(newLabels))).thenReturn(
                asList(KafkaConnectS2ICluster.fromCrd(bar, VERSIONS).generateDeploymentConfig(new HashMap<String, String>(), true, null, null)));

        // providing the list DeploymentConfigs for already "existing" Kafka Connect S2I clusters
        Labels barLabels = Labels.forStrimziCluster("bar");
        when(mockDcOps.list(eq(kcs2iNamespace), eq(barLabels))).thenReturn(
                asList(KafkaConnectS2ICluster.fromCrd(bar, VERSIONS).generateDeploymentConfig(new HashMap<String, String>(), true, null, null))
        );
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.reconcile(eq(kcs2iNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        Checkpoint createOrUpdateAsync = context.checkpoint(2);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnectS2I kafkaConnectS2IAssembly) {
                createdOrUpdated.add(kafkaConnectS2IAssembly.getMetadata().getName());
                createOrUpdateAsync.flag();
                return Future.succeededFuture();
            }
        };

        Checkpoint async = context.checkpoint();
        // Now try to reconcile all the Kafka Connect S2I clusters
        ops.reconcileAll("test", kcs2iNamespace,
            context.succeeding(v -> context.verify(() -> {
                assertThat(createdOrUpdated, is(new HashSet(asList("foo", "bar"))));
                async.flag();
            })));

    }

    @Test
    public void testCreateClusterStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockS2IConnectOps = supplier.connectS2IOperator;
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        String kcs2iName = "foo";
        String kcs2iNamespace = "test";
        String failureMessage = "failure";

        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);
        when(mockS2IConnectOps.get(kcs2iNamespace, kcs2iName)).thenReturn(kcs2i);
        when(mockS2IConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kcs2i));
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMessage));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockIsOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockBcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockS2IConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnectS2I())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(eq(kcs2i.getMetadata().getNamespace()), eq(KafkaConnectS2IResources.deploymentName(kcs2i.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);

        ArgumentCaptor<KafkaConnectS2I> connectCaptor = ArgumentCaptor.forClass(KafkaConnectS2I.class);
        when(mockS2IConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, kcs2iNamespace, kcs2iName), kcs2i)
            .onComplete(context.failing(e -> context.verify(() -> {
                // Verify status
                List<KafkaConnectS2I> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects.get(0).getStatus().getUrl(), is("http://foo-connect-api.test.svc:8083"));
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("NotReady"));
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getMessage(), is(failureMessage));

                async.flag();
            })));
    }

    public void createClusterWithDuplicateOlderConnect(VertxTestContext context, KafkaConnectS2I kcs2i, boolean connectorOperator) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        CrdOperator mockConnectOps = supplier.connectOperator;
        DeploymentConfigOperator mockDcOps = supplier.deploymentConfigOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ImageStreamOperator mockIsOps = supplier.imagesStreamOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        kcs2i.getMetadata().setCreationTimestamp("2020-01-27T19:31:11Z");

        KafkaConnect conflictingConnect = ResourceUtils.createEmptyKafkaConnect(kcs2i.getMetadata().getNamespace(), kcs2i.getMetadata().getName());
        conflictingConnect.getMetadata().setCreationTimestamp("2020-01-27T19:31:12Z");

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectS2IOps.get(kcs2i.getMetadata().getNamespace(), kcs2i.getMetadata().getName())).thenReturn(kcs2i);
        when(mockConnectS2IOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kcs2i));

        when(mockConnectOps.getAsync(kcs2i.getMetadata().getNamespace(), kcs2i.getMetadata().getName())).thenReturn(Future.succeededFuture(conflictingConnect));

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
        when(mockNetPolOps.reconcile(eq(kcs2i.getMetadata().getNamespace()), eq(KafkaConnectS2IResources.deploymentName(kcs2i.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockConnectS2IOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnectS2I())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);

        ArgumentCaptor<KafkaConnectS2I> connectCaptor = ArgumentCaptor.forClass(KafkaConnectS2I.class);
        when(mockConnectS2IOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectS2ICluster connect = KafkaConnectS2ICluster.fromCrd(kcs2i, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, kcs2i.getMetadata().getNamespace(), kcs2i.getMetadata().getName()), kcs2i)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(connect.getServiceName()));
                assertThat(service, is(connect.generateService()));

                // Verify Deployment Config
                List<DeploymentConfig> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                DeploymentConfig dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(connect.getName()));
                Map annotations = new HashMap();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH, Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(LOGGING_CONFIG)));
                assertThat(dc, is(connect.generateDeploymentConfig(annotations, true, null, null)));

                // Verify Build Config
                List<BuildConfig> capturedBc = bcCaptor.getAllValues();
                assertThat(capturedBc, hasSize(1));
                BuildConfig bc = capturedBc.get(0);
                assertThat(dc.getMetadata().getName(), is(connect.getName()));
                assertThat(bc, is(connect.generateBuildConfig()));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(connect.getName()));
                assertThat(pdb, is(connect.generatePodDisruptionBudget()));

                // Verify Image Streams
                List<ImageStream> capturedIs = isCaptor.getAllValues();
                assertThat(capturedIs, hasSize(2));
                int sourceImageStreamIndex = (KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster())).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;
                int targetImageStreamIndex = (connect.getName()).equals(capturedIs.get(0).getMetadata().getName()) ? 0 : 1;

                ImageStream source = capturedIs.get(sourceImageStreamIndex);
                assertThat(source.getMetadata().getName(), is(KafkaConnectS2IResources.sourceImageStreamName(connect.getCluster())));
                assertThat(source, is(connect.generateSourceImageStream()));

                ImageStream target = capturedIs.get(targetImageStreamIndex);
                assertThat(target.getMetadata().getName(), is(connect.getName()));
                assertThat(target, is(connect.generateTargetImageStream()));

                // Verify status
                List<KafkaConnectS2I> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects.get(0).getStatus().getUrl(), is("http://foo-connect-api.test.svc:8083"));
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("Ready"));

                if (connectorOperator) {
                    assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins(), hasSize(1));
                    assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().get(0).getConnectorClass(), is("io.strimzi.MyClass"));
                    assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().get(0).getType(), is("sink"));
                    assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().get(0).getVersion(), is("1.0.0"));
                }

                async.flag();
            })));
    }

    @Test
    public void testCreateClusterWitDuplicateOlderConnectWithoutConnectorOperator(VertxTestContext context) {
        String kcs2iName = "foo";
        String kcs2iNamespace = "test";
        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);

        createClusterWithDuplicateOlderConnect(context, kcs2i, false);
    }

    @Test
    public void testCreateClusterWitDuplicateOlderConnectWithConnectorOperator(VertxTestContext context) {
        String kcs2iName = "foo";
        String kcs2iNamespace = "test";
        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);
        kcs2i.getMetadata().getAnnotations().put("strimzi.io/use-connector-resources", "true");

        createClusterWithDuplicateOlderConnect(context, kcs2i, true);
    }

    @Test
    public void testCreateClusterWithSameNameAsConnectFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        CrdOperator mockConnectOps = supplier.connectOperator;

        String kcs2iName = "foo";
        String kcs2iNamespace = "test";

        KafkaConnectS2I kcs2i = ResourceUtils.createEmptyKafkaConnectS2I(kcs2iNamespace, kcs2iName);
        kcs2i.getMetadata().setCreationTimestamp("2020-01-27T19:31:11Z");

        KafkaConnect conflictingConnect = ResourceUtils.createEmptyKafkaConnect(kcs2iNamespace, kcs2iName);
        conflictingConnect.getMetadata().setCreationTimestamp("2020-01-27T19:31:10Z");

        when(mockConnectS2IOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kcs2i));
        when(mockConnectOps.getAsync(kcs2iNamespace, kcs2iName)).thenReturn(Future.succeededFuture(conflictingConnect));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);

        ArgumentCaptor<KafkaConnectS2I> connectCaptor = ArgumentCaptor.forClass(KafkaConnectS2I.class);
        when(mockConnectS2IOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaConnectS2IAssemblyOperator ops = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnectS2I.RESOURCE_KIND, kcs2iNamespace, kcs2iName), kcs2i)
            .onComplete(context.failing(v -> context.verify(() -> {
                // Verify status
                List<KafkaConnectS2I> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("NotReady"));
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getMessage(),
                        is("Both KafkaConnect and KafkaConnectS2I exist with the same name. KafkaConnect is older and will be used while this custom resource will be ignored."));
                async.flag();
            })));
    }

}
