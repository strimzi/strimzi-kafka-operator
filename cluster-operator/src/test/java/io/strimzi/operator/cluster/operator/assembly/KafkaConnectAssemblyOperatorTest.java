/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.connect.ConnectorPluginBuilder;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaConnectAssemblyOperatorTest {

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

    public void createKafkaConnectCluster(VertxTestContext context, KafkaConnect kc, boolean connectorOperator) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kc.getMetadata().getNamespace(), kc.getMetadata().getName())).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        when(mockConnectS2IOps.getAsync(kc.getMetadata().getNamespace(), kc.getMetadata().getName())).thenReturn(Future.succeededFuture(null));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), npCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // No metrics config  => no CMs created
                Set<String> metricsNames = new HashSet<>();
                if (connect.isMetricsEnabled()) {
                    metricsNames.add(KafkaConnectResources.metricsAndLogConfigMapName(kc.getMetadata().getName()));
                }

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(connect.getServiceName()));
                assertThat(service, is(connect.generateService()));

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(connect.getName()));
                Map annotations = new HashMap();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH, Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(LOGGING_CONFIG)));
                assertThat(dc, is(connect.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb.size(), is(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(connect.getName()));
                assertThat(pdb, is(connect.generatePodDisruptionBudget()));

                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                assertThat(connectStatus.getUrl(), is("http://foo-connect-api.test.svc:8083"));
                assertThat(connectStatus.getReplicas(), is(connect.getReplicas()));
                assertThat(connectStatus.getLabelSelector(), is(connect.getSelectorLabels().toSelectorString()));
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
        String kcName = "foo";
        String kcNamespace = "test";
        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);

        createKafkaConnectCluster(context, kc, false);
    }

    @Test
    public void testCreateClusterWithConnectorOperator(VertxTestContext context) {
        String kcName = "foo";
        String kcNamespace = "test";
        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        kc.getMetadata().getAnnotations().put(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true");

        createKafkaConnectCluster(context, kc, true);
    }

    @Test
    public void testCreateOrUpdateDoesNotUpdateWithNoDiff(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(kcNamespace, kcName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(eq(kcNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(kcNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(kcNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(kcNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockNetPolOps.reconcile(eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.succeeding(v -> context.verify(() -> {

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));

                // Verify Deployment Config
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(connect.getName()));
                assertThat(pdb, is(connect.generatePodDisruptionBudget()));

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateUpdatesCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);
        kc.getSpec().setImage("some/different:image"); // Change the image to generate some diff
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(kcNamespace, kcName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(eq(kcNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(kcNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(kcNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(kcNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockNetPolOps.reconcile(eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock CM get
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaConnectResources.metricsAndLogConfigMapName(kcName))
                    .withNamespace(kcNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(kcNamespace, KafkaConnectResources.metricsAndLogConfigMapName(kcName))).thenReturn(metricsCm);

        ConfigMap loggingCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaConnectResources.metricsAndLogConfigMapName(kcName))
                    .withNamespace(kcNamespace)
                    .endMetadata()
                    .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG, LOGGING_CONFIG))
                    .build();

        when(mockCmOps.get(kcNamespace, KafkaConnectResources.metricsAndLogConfigMapName(kcName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(kcNamespace), anyString(), any());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                KafkaConnectCluster compareTo = KafkaConnectCluster.fromCrd(kc, VERSIONS);

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(compareTo.getServiceName()));
                assertThat(service, is(compareTo.generateService()));

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(compareTo.getName()));
                Map<String, String> annotations = new HashMap();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH, Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(loggingCm.getData().get(compareTo.ANCILLARY_CM_KEY_LOG_CONFIG))));
                assertThat(dc, is(compareTo.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(connect.getName()));
                assertThat(pdb, is(connect.generatePodDisruptionBudget()));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                // No metrics config  => no CMs created
                verify(mockCmOps, never()).createOrUpdate(any());
                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateFailsWhenDeploymentUpdateFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);
        kc.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(kcNamespace, kcName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

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

        when(mockNetPolOps.reconcile(eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnect())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.failing(v -> async.flag()));
    }

    @Test
    public void testUpdateClusterScaleUp(VertxTestContext context) {
        final int scaleTo = 4;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);
        kc.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(kcNamespace, kcName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(kcNamespace, connect.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(kcNamespace, connect.getName(), scaleTo);

        when(mockNetPolOps.reconcile(eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnect())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(kcNamespace, connect.getName(), scaleTo);
                async.flag();
            })));
    }

    @Test
    public void testUpdateClusterScaleDown(VertxTestContext context) {
        int scaleTo = 2;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);
        kc.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(kcNamespace, kcName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(kcNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(kcNamespace, connect.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(kcNamespace, connect.getName(), scaleTo);

        when(mockNetPolOps.reconcile(eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnect())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(kcNamespace, connect.getName(), scaleTo);

                async.flag();
            })));
    }

    @Test
    public void testReconcile(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        String kcNamespace = "test";

        KafkaConnect foo = ResourceUtils.createEmptyKafkaConnect(kcNamespace, "foo");
        KafkaConnect bar = ResourceUtils.createEmptyKafkaConnect(kcNamespace, "bar");
        when(mockConnectOps.listAsync(eq(kcNamespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        // when requested ConfigMap for a specific Kafka Connect cluster
        when(mockConnectOps.get(eq(kcNamespace), eq("foo"))).thenReturn(foo);
        when(mockConnectOps.get(eq(kcNamespace), eq("bar"))).thenReturn(bar);
        when(mockConnectS2IOps.getAsync(kcNamespace, "foo")).thenReturn(Future.succeededFuture(null));
        when(mockConnectS2IOps.getAsync(kcNamespace, "bar")).thenReturn(Future.succeededFuture(null));

        // providing the list of ALL Deployments for all the Kafka Connect clusters
        Labels newLabels = Labels.forStrimziKind(KafkaConnect.RESOURCE_KIND);
        when(mockDcOps.list(eq(kcNamespace), eq(newLabels))).thenReturn(
                asList(KafkaConnectCluster.fromCrd(bar, VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null)));

        // providing the list Deployments for already "existing" Kafka Connect clusters
        Labels barLabels = Labels.forStrimziCluster("bar");
        when(mockDcOps.list(eq(kcNamespace), eq(barLabels))).thenReturn(
                asList(KafkaConnectCluster.fromCrd(bar, VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null))
        );
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockNetPolOps.reconcile(eq(kcNamespace), eq(KafkaConnectResources.deploymentName(bar.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockSecretOps.reconcile(eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        Checkpoint asyncCreated = context.checkpoint(2);
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnectAssembly) {
                createdOrUpdated.add(kafkaConnectAssembly.getMetadata().getName());
                asyncCreated.flag();
                return Future.succeededFuture();
            }
        };


        Checkpoint async = context.checkpoint();
        Promise reconciled = Promise.promise();
        // Now try to reconcile all the Kafka Connect clusters
        ops.reconcileAll("test", kcNamespace, ignored -> reconciled.complete());

        reconciled.future().onComplete(context.succeeding(v -> context.verify(() -> {
            assertThat(createdOrUpdated, is(new HashSet(asList("foo", "bar"))));
            async.flag();
        })));
    }

    @Test
    public void testUpdateClusterWithFailedScaleDownSetsStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        String kcName = "foo";
        String kcNamespace = "test";
        String failureMsg = "failure";
        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);

        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectS2IOps.getAsync(kcNamespace, kcName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any()))
            .thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.failing(v -> context.verify(() -> {
                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getUrl(), is("http://foo-connect-api.test.svc:8083"));
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("NotReady"));
                assertThat(connectStatus.getConditions().get(0).getMessage(), is(failureMsg));
                async.flag();
            })));
    }

    public void assertCreateClusterWitDuplicateOlderConnect(VertxTestContext context, KafkaConnect kc, boolean connectorOperator) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        kc.getMetadata().setCreationTimestamp("2020-01-27T19:31:12Z");

        KafkaConnectS2I conflictingConnectS2I = ResourceUtils.createEmptyKafkaConnectS2I(kc.getMetadata().getNamespace(), kc.getMetadata().getName());
        conflictingConnectS2I.getMetadata().setCreationTimestamp("2020-01-27T19:31:13Z");

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kc.getMetadata().getNamespace(), kc.getMetadata().getName())).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        when(mockConnectS2IOps.getAsync(kc.getMetadata().getNamespace(), kc.getMetadata().getName())).thenReturn(Future.succeededFuture(conflictingConnectS2I));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockNetPolOps.reconcile(eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(anyString(), anyInt(), anyString())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kc.getMetadata().getNamespace(), kc.getMetadata().getName()), kc)
            .onComplete(context.succeeding(v -> context.verify(() -> {

                // No metrics config  => no CMs created
                Set<String> metricsNames = new HashSet<>();
                if (connect.isMetricsEnabled()) {
                    metricsNames.add(KafkaConnectResources.metricsAndLogConfigMapName(kc.getMetadata().getName()));
                }

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices.size(), is(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(connect.getServiceName()));
                assertThat(service, is(connect.generateService()));

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc.size(), is(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(connect.getName()));
                Map annotations = new HashMap();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH, Util.stringHash(Util.getLoggingDynamicallyUnmodifiableEntries(LOGGING_CONFIG)));
                assertThat(dc, is(connect.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(connect.getName()));
                assertThat(pdb, is(connect.generatePodDisruptionBudget()));

                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getUrl(), is("http://foo-connect-api.test.svc:8083"));
                assertThat(connectStatus.getReplicas(), is(connect.getReplicas()));
                assertThat(connectStatus.getLabelSelector(), is(connect.getSelectorLabels().toSelectorString()));
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));
                if (connectorOperator) {
                    assertThat(connectStatus.getConnectorPlugins(), hasSize(1));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getConnectorClass(), is("io.strimzi.MyClass"));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getType(), is("sink"));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getVersion(), is("1.0.0"));
                }

                async.flag();
            })));
    }

    @Test
    public void testCreateClusterWitDuplicateOlderConnectWithoutConnectorOperator(VertxTestContext context) {
        String kcName = "foo";
        String kcNamespace = "test";
        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);

        assertCreateClusterWitDuplicateOlderConnect(context, kc, false);
    }

    @Test
    public void testCreateClusterWitDuplicateOlderConnectWithConnectorOperator(VertxTestContext context) {
        String kcName = "foo";
        String kcNamespace = "test";
        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        kc.getMetadata().getAnnotations().put("strimzi.io/use-connector-resources", "true");

        assertCreateClusterWitDuplicateOlderConnect(context, kc, true);
    }

    @Test
    public void testCreateClusterWitDuplicateNeverConnectHasNotReadyStatus(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        kc.getMetadata().setCreationTimestamp("2020-01-27T19:31:12Z");

        KafkaConnectS2I conflictingConnectS2I = ResourceUtils.createEmptyKafkaConnectS2I(kcNamespace, kcName);
        conflictingConnectS2I.getMetadata().setCreationTimestamp("2020-01-27T19:31:11Z");

        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        when(mockConnectS2IOps.getAsync(kcNamespace, kcName)).thenReturn(Future.succeededFuture(conflictingConnectS2I));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.failing(error -> context.verify(() -> {
                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("NotReady"));
                assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getMessage(),
                        is("Both KafkaConnect and KafkaConnectS2I exist with the same name. KafkaConnectS2I is older and will be used while this custom resource will be ignored."));

                async.flag();
            })));
    }

    @Test
    public void testIsOlderOrAlone()    {
        KafkaConnectS2I conflictingConnectS2I = ResourceUtils.createEmptyKafkaConnectS2I("foo", "bar");
        conflictingConnectS2I.getMetadata().setCreationTimestamp("2020-01-27T19:31:13Z");

        assertThat(AbstractConnectOperator.isOlderOrAlone("2020-01-27T19:31:11Z", null), is(true));
        assertThat(AbstractConnectOperator.isOlderOrAlone("2020-01-27T19:31:12Z", conflictingConnectS2I), is(true));
        assertThat(AbstractConnectOperator.isOlderOrAlone("2020-01-27T19:31:13Z", conflictingConnectS2I), is(true));
        assertThat(AbstractConnectOperator.isOlderOrAlone("2020-01-27T19:31:14Z", conflictingConnectS2I), is(false));
    }
}
