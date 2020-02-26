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
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
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

    public void testCreateCluster(VertxTestContext context, KafkaConnect clusterCm, boolean connectorOperator) {
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
        when(mockConnectOps.get(clusterCm.getMetadata().getNamespace(), clusterCm.getMetadata().getName())).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));

        when(mockConnectS2IOps.getAsync(clusterCm.getMetadata().getNamespace(), clusterCm.getMetadata().getName())).thenReturn(Future.succeededFuture(null));

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
        when(mockNetPolOps.reconcile(eq(clusterCm.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(clusterCm.getMetadata().getName())), npCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

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

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCm.getMetadata().getNamespace(), clusterCm.getMetadata().getName()), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            // No metrics config  => no CMs created
            Set<String> metricsNames = new HashSet<>();
            if (connect.isMetricsEnabled()) {
                metricsNames.add(KafkaConnectResources.metricsAndLogConfigMapName(clusterCm.getMetadata().getName()));
            }

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedServices.size(), is(1)));
            Service service = capturedServices.get(0);
            context.verify(() -> assertThat(service.getMetadata().getName(), is(connect.getServiceName())));
            context.verify(() -> assertThat("Services are not equal", service, is(connect.generateService())));

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedDc.size(), is(1)));
            Deployment dc = capturedDc.get(0);
            context.verify(() -> assertThat(dc.getMetadata().getName(), is(connect.getName())));
            Map annotations = new HashMap();
            annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, LOGGING_CONFIG);
            context.verify(() -> assertThat("Deployments are not equal", dc, is(connect.generateDeployment(annotations, true, null, null))));

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.verify(() -> assertThat(capturedPdb.size(), is(1)));
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.verify(() -> assertThat(pdb.getMetadata().getName(), is(connect.getName())));
            context.verify(() -> assertThat("PodDisruptionBudgets are not equal", pdb, is(connect.generatePodDisruptionBudget())));

            // Verify status
            List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getUrl(), is("http://foo-connect-api.test.svc:8083")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("Ready")));

            if (connectorOperator) {
                context.verify(() -> assertThat(npCaptor.getValue(), is(notNullValue())));

                context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().size(), is(1)));
                context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().get(0).getConnectorClass(), is("io.strimzi.MyClass")));
                context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().get(0).getType(), is("sink")));
                context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().get(0).getVersion(), is("1.0.0")));
            } else {
                context.verify(() -> assertThat(npCaptor.getValue(), is(nullValue())));
                context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins(), nullValue()));
            }

            async.flag();
        });
    }

    @Test
    public void testCreateClusterWithoutConnectorOperator(VertxTestContext context) {
        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);

        testCreateCluster(context, clusterCm, false);
    }

    @Test
    public void testCreateClusterWithConnectorOperator(VertxTestContext context) {
        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        clusterCm.getMetadata().getAnnotations().put("strimzi.io/use-connector-resources", "true");

        testCreateCluster(context, clusterCm, true);
    }

    @Test
    public void testUpdateClusterNoDiff(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(clusterCmNamespace, clusterCmName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

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

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockNetPolOps.reconcile(eq(clusterCm.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(clusterCm.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

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

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedServices.size(), is(1)));

            // Verify Deployment Config
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedDc.size(), is(1)));

            // Verify scaleDown / scaleUp were not called
            context.verify(() -> assertThat(dcScaleDownNameCaptor.getAllValues().size(), is(1)));
            context.verify(() -> assertThat(dcScaleUpNameCaptor.getAllValues().size(), is(1)));

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.verify(() -> assertThat(capturedPdb.size(), is(1)));
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.verify(() -> assertThat(pdb.getMetadata().getName(), is(connect.getName())));
            context.verify(() -> assertThat("PodDisruptionBudgets are not equal", pdb, is(connect.generatePodDisruptionBudget())));

            async.flag();
        });
    }

    @Test
    public void testUpdateCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(clusterCmNamespace, clusterCmName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

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

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockNetPolOps.reconcile(eq(clusterCm.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(clusterCm.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock CM get
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaConnectResources.metricsAndLogConfigMapName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(clusterCmNamespace, KafkaConnectResources.metricsAndLogConfigMapName(clusterCmName))).thenReturn(metricsCm);

        ConfigMap loggingCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaConnectResources.metricsAndLogConfigMapName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                    .endMetadata()
                    .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG, LOGGING_CONFIG))
                    .build();

        when(mockCmOps.get(clusterCmNamespace, KafkaConnectResources.metricsAndLogConfigMapName(clusterCmName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(clusterCmNamespace), anyString(), any());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            KafkaConnectCluster compareTo = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedServices.size(), is(1)));
            Service service = capturedServices.get(0);
            context.verify(() -> assertThat(service.getMetadata().getName(), is(compareTo.getServiceName())));
            context.verify(() -> assertThat("Services are not equal", service, is(compareTo.generateService())));

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedDc.size(), is(1)));
            Deployment dc = capturedDc.get(0);
            context.verify(() -> assertThat(dc.getMetadata().getName(), is(compareTo.getName())));
            Map<String, String> annotations = new HashMap();
            annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, loggingCm.getData().get(compareTo.ANCILLARY_CM_KEY_LOG_CONFIG));
            context.verify(() -> assertThat("Deployments are not equal", dc, is(compareTo.generateDeployment(annotations, true, null, null))));

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.verify(() -> assertThat(capturedPdb.size(), is(1)));
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.verify(() -> assertThat(pdb.getMetadata().getName(), is(connect.getName())));
            context.verify(() -> assertThat("PodDisruptionBudgets are not equal", pdb, is(connect.generatePodDisruptionBudget())));

            // Verify scaleDown / scaleUp were not called
            context.verify(() -> assertThat(dcScaleDownNameCaptor.getAllValues().size(), is(1)));
            context.verify(() -> assertThat(dcScaleUpNameCaptor.getAllValues().size(), is(1)));

            // No metrics config  => no CMs created
            verify(mockCmOps, never()).createOrUpdate(any());
            async.flag();
        });
    }

    @Test
    public void testUpdateClusterFailure(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(clusterCmNamespace, clusterCmName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
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

        when(mockNetPolOps.reconcile(eq(clusterCm.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(clusterCm.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockConnectOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnect())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(false)));

            async.flag();
        });
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

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(clusterCmNamespace, clusterCmName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, connect.getName(), scaleTo);

        when(mockNetPolOps.reconcile(eq(clusterCm.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(clusterCm.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
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

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            verify(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

            async.flag();
        });
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

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectOps.updateStatusAsync(any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockConnectS2IOps.getAsync(clusterCmNamespace, clusterCmName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateService());
        when(mockDcOps.get(clusterCmNamespace, connect.getName())).thenReturn(connect.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, connect.getName(), scaleTo);

        when(mockNetPolOps.reconcile(eq(clusterCm.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(clusterCm.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
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

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            verify(mockDcOps).scaleUp(clusterCmNamespace, connect.getName(), scaleTo);

            async.flag();
        });
    }

    @Test
    public void testReconcile(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        String clusterCmNamespace = "test";

        KafkaConnect foo = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, "foo");
        KafkaConnect bar = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, "bar");
        when(mockConnectOps.listAsync(eq(clusterCmNamespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        // when requested ConfigMap for a specific Kafka Connect cluster
        when(mockConnectOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockConnectOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);
        when(mockConnectS2IOps.getAsync(clusterCmNamespace, "foo")).thenReturn(Future.succeededFuture(null));
        when(mockConnectS2IOps.getAsync(clusterCmNamespace, "bar")).thenReturn(Future.succeededFuture(null));

        // providing the list of ALL Deployments for all the Kafka Connect clusters
        Labels newLabels = Labels.forKind(KafkaConnect.RESOURCE_KIND);
        when(mockDcOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaConnectCluster.fromCrd(bar, VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null)));

        // providing the list Deployments for already "existing" Kafka Connect clusters
        Labels barLabels = Labels.forCluster("bar");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaConnectCluster.fromCrd(bar, VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null))
        );
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockNetPolOps.reconcile(eq(clusterCmNamespace), eq(KafkaConnectResources.deploymentName(bar.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockSecretOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        CountDownLatch async = new CountDownLatch(2);
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnectAssembly) {
                createdOrUpdated.add(kafkaConnectAssembly.getMetadata().getName());
                async.countDown();
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka Connect clusters
        ops.reconcileAll("test", clusterCmNamespace, ignored -> { });

        async.await(60, TimeUnit.SECONDS);
        context.verify(() -> assertThat(createdOrUpdated, is(new HashSet(asList("foo", "bar")))));
        context.completeNow();
    }

    @Test
    public void testCreateClusterStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        String failureMsg = "failure";
        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);

        when(mockConnectOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockConnectS2IOps.getAsync(clusterCmNamespace, clusterCmName)).thenReturn(Future.succeededFuture(null));
        when(mockServiceOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(eq(clusterCm.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(clusterCm.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(false)));

            // Verify status
            List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getUrl(), is("http://foo-connect-api.test.svc:8083")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("NotReady")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getMessage(), is(failureMsg)));
            async.flag();
        });
    }

    public void testCreateClusterWitDuplicateOlderConnect(VertxTestContext context, KafkaConnect clusterCm, boolean connectorOperator) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> mockConnectorOps = supplier.kafkaConnectorOperator;

        clusterCm.getMetadata().setCreationTimestamp("2020-01-27T19:31:12Z");

        KafkaConnectS2I conflictingConnectS2I = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCm.getMetadata().getNamespace(), clusterCm.getMetadata().getName());
        conflictingConnectS2I.getMetadata().setCreationTimestamp("2020-01-27T19:31:13Z");

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(clusterCm.getMetadata().getNamespace(), clusterCm.getMetadata().getName())).thenReturn(clusterCm);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));

        when(mockConnectS2IOps.getAsync(clusterCm.getMetadata().getNamespace(), clusterCm.getMetadata().getName())).thenReturn(Future.succeededFuture(conflictingConnectS2I));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockNetPolOps.reconcile(eq(clusterCm.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(clusterCm.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

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

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(clusterCm, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCm.getMetadata().getNamespace(), clusterCm.getMetadata().getName()), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            // No metrics config  => no CMs created
            Set<String> metricsNames = new HashSet<>();
            if (connect.isMetricsEnabled()) {
                metricsNames.add(KafkaConnectResources.metricsAndLogConfigMapName(clusterCm.getMetadata().getName()));
            }

            // Verify service
            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedServices.size(), is(1)));
            Service service = capturedServices.get(0);
            context.verify(() -> assertThat(service.getMetadata().getName(), is(connect.getServiceName())));
            context.verify(() -> assertThat("Services are not equal", service, is(connect.generateService())));

            // Verify Deployment
            List<Deployment> capturedDc = dcCaptor.getAllValues();
            context.verify(() -> assertThat(capturedDc.size(), is(1)));
            Deployment dc = capturedDc.get(0);
            context.verify(() -> assertThat(dc.getMetadata().getName(), is(connect.getName())));
            Map annotations = new HashMap();
            annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, LOGGING_CONFIG);
            context.verify(() -> assertThat("Deployments are not equal", dc, is(connect.generateDeployment(annotations, true, null, null))));

            // Verify PodDisruptionBudget
            List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
            context.verify(() -> assertThat(capturedPdb.size(), is(1)));
            PodDisruptionBudget pdb = capturedPdb.get(0);
            context.verify(() -> assertThat(pdb.getMetadata().getName(), is(connect.getName())));
            context.verify(() -> assertThat("PodDisruptionBudgets are not equal", pdb, is(connect.generatePodDisruptionBudget())));

            // Verify status
            List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getUrl(), is("http://foo-connect-api.test.svc:8083")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("Ready")));

            if (connectorOperator) {
                context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().size(), is(1)));
                context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().get(0).getConnectorClass(), is("io.strimzi.MyClass")));
                context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().get(0).getType(), is("sink")));
                context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConnectorPlugins().get(0).getVersion(), is("1.0.0")));
            }

            async.flag();
        });
    }

    @Test
    public void testCreateClusterWitDuplicateOlderConnectWithoutConnectorOperator(VertxTestContext context) {
        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);

        testCreateClusterWitDuplicateOlderConnect(context, clusterCm, false);
    }

    @Test
    public void testCreateClusterWitDuplicateOlderConnectWithConnectorOperator(VertxTestContext context) {
        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        clusterCm.getMetadata().getAnnotations().put("strimzi.io/use-connector-resources", "true");

        testCreateClusterWitDuplicateOlderConnect(context, clusterCm, true);
    }

    @Test
    public void testCreateClusterWitDuplicateNeverConnect(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaConnect clusterCm = ResourceUtils.createEmptyKafkaConnectCluster(clusterCmNamespace, clusterCmName);
        clusterCm.getMetadata().setCreationTimestamp("2020-01-27T19:31:12Z");

        KafkaConnectS2I conflictingConnectS2I = ResourceUtils.createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName);
        conflictingConnectS2I.getMetadata().setCreationTimestamp("2020-01-27T19:31:11Z");

        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));

        when(mockConnectS2IOps.getAsync(clusterCmNamespace, clusterCmName)).thenReturn(Future.succeededFuture(conflictingConnectS2I));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(false)));

            // Verify status
            List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getStatus(), is("True")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getType(), is("NotReady")));
            context.verify(() -> assertThat(capturedConnects.get(0).getStatus().getConditions().get(0).getMessage(), is("Both KafkaConnect and KafkaConnectS2I exist with the same name. KafkaConnectS2I is older and will be used while this custom resource will be ignored.")));

            async.flag();
        });
    }

    @Test
    public void testIsOlderOrAlone()    {
        KafkaConnectS2I conflictingConnectS2I = ResourceUtils.createEmptyKafkaConnectS2ICluster("foo", "bar");
        conflictingConnectS2I.getMetadata().setCreationTimestamp("2020-01-27T19:31:13Z");

        assertThat(AbstractConnectOperator.isOlderOrAlone("2020-01-27T19:31:11Z", null), is(true));
        assertThat(AbstractConnectOperator.isOlderOrAlone("2020-01-27T19:31:12Z", conflictingConnectS2I), is(true));
        assertThat(AbstractConnectOperator.isOlderOrAlone("2020-01-27T19:31:13Z", conflictingConnectS2I), is(true));
        assertThat(AbstractConnectOperator.isOlderOrAlone("2020-01-27T19:31:14Z", conflictingConnectS2I), is(false));
    }
}
