/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.RackBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.connect.ConnectorPluginBuilder;
import io.strimzi.api.kafka.model.status.AutoRestartStatusBuilder;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connect.build.BuildBuilder;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.operator.cluster.model.LoggingUtils;
import io.strimzi.platform.KubernetesVersion;
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
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
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
import org.mockito.Mockito;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
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
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "ClassFanOutComplexity"})
@ExtendWith(VertxExtension.class)
public class KafkaConnectAssemblyOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    protected static Vertx vertx;
    private static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";
    private static final String LOGGING_CONFIG = LoggingUtils.defaultLogConfig(Reconciliation.DUMMY_RECONCILIATION, "KafkaConnectCluster")
            .asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.");

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_21;

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
        var mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kc.getMetadata().getNamespace(), kc.getMetadata().getName())).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        when(mockPodOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildPodName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        when(mockBcOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildConfigName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), npCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kc.getMetadata().getNamespace(), kc.getMetadata().getName()))
            .onComplete(context.succeeding(v -> context.verify(() -> {

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
                assertThat(dc.getMetadata().getName(), is(connect.getComponentName()));
                Map<String, String> annotations = new HashMap<>();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH,
                                                         Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(LOGGING_CONFIG)));
                annotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, "0");
                assertThat(dc, is(connect.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb.size(), is(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(connect.getComponentName()));
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
        var mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(), any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kcNamespace, connect.getComponentName())).thenReturn(connect.generateService());
        when(mockDcOps.getAsync(kcNamespace, connect.getComponentName())).thenReturn(Future.succeededFuture(connect.generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(kcNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(kcNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(kcNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(kcNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        when(mockPodOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildPodName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        when(mockBcOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildConfigName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());        
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

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
                assertThat(pdb.getMetadata().getName(), is(connect.getComponentName()));
                assertThat(pdb, is(connect.generatePodDisruptionBudget()));
                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateUpdatesCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);
        kc.getSpec().setImage("some/different:image"); // Change the image to generate some diff
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(), any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kcNamespace, connect.getComponentName())).thenReturn(connect.generateService());
        when(mockDcOps.getAsync(kcNamespace, connect.getComponentName())).thenReturn(Future.succeededFuture(connect.generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(kcNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(kcNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(kcNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(kcNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        when(mockPodOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildPodName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        when(mockBcOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildConfigName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());  
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
        }).when(mockCmOps).reconcile(any(), eq(kcNamespace), anyString(), any());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                KafkaConnectCluster compareTo = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);

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
                assertThat(dc.getMetadata().getName(), is(compareTo.getComponentName()));

                String loggingConfiguration = loggingCm.getData().get(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG);
                String dynamicallyUnmodifiableEntries = Util.getLoggingDynamicallyUnmodifiableEntries(loggingConfiguration);
                String hashedLoggingConf = Util.hashStub(dynamicallyUnmodifiableEntries);
                Map<String, String> annotations = new HashMap<>();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH,
                                                         hashedLoggingConf);
                annotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, "0");

                assertThat(dc, is(compareTo.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(connect.getComponentName()));
                assertThat(pdb, is(connect.generatePodDisruptionBudget()));
                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                // No metrics config  => no CMs created
                verify(mockCmOps, never()).createOrUpdate(any(), any());
                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateFailsWhenDeploymentUpdateFails(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);
        kc.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(), any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kcNamespace, connect.getComponentName())).thenReturn(connect.generateService());
        when(mockDcOps.getAsync(kcNamespace, connect.getComponentName())).thenReturn(Future.succeededFuture(connect.generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.failedFuture("Failed"));

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockConnectOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnect())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

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
        var mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);
        kc.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(), any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kcNamespace, connect.getComponentName())).thenReturn(connect.generateService());
        when(mockDcOps.getAsync(kcNamespace, connect.getComponentName())).thenReturn(Future.succeededFuture(connect.generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(any(), eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(any(), eq(kcNamespace), eq(connect.getComponentName()), eq(scaleTo));

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(any(), eq(kcNamespace), eq(connect.getComponentName()), eq(scaleTo));

        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockPodOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildPodName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockBcOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildConfigName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockConnectOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnect())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(kcNamespace), eq(connect.getComponentName()), eq(scaleTo));
                async.flag();
            })));
    }

    @Test
    public void testUpdateClusterScaleDown(VertxTestContext context) {
        int scaleTo = 2;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);
        kc.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(), any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kcNamespace, connect.getComponentName())).thenReturn(connect.generateService());
        when(mockDcOps.getAsync(kcNamespace, connect.getComponentName())).thenReturn(Future.succeededFuture(connect.generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(any(), eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(any(), eq(kcNamespace), eq(connect.getComponentName()), eq(scaleTo));

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(any(), eq(kcNamespace), eq(connect.getComponentName()), eq(scaleTo));

        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockPodOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildPodName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockBcOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildConfigName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockConnectOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnect())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));

        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(kcNamespace), eq(connect.getComponentName()), eq(scaleTo));

                async.flag();
            })));
    }

    @Test
    public void testReconcile(VertxTestContext context) {
        //Must create all needed checkpoints before flagging any, to avoid premature test success
        Checkpoint asyncCreated = context.checkpoint(2);
        Checkpoint async = context.checkpoint();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        var mockConnectorOps = supplier.kafkaConnectorOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;

        when(mockConnectorOps.listAsync(any(), any(Optional.class))).thenReturn(Future.succeededFuture(List.of()));
        String kcNamespace = "test";

        KafkaConnect foo = ResourceUtils.createEmptyKafkaConnect(kcNamespace, "foo");
        KafkaConnect bar = ResourceUtils.createEmptyKafkaConnect(kcNamespace, "bar");
        when(mockConnectOps.listAsync(eq(kcNamespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        // when requested ConfigMap for a specific Kafka Connect cluster
        when(mockConnectOps.get(eq(kcNamespace), eq("foo"))).thenReturn(foo);
        when(mockConnectOps.get(eq(kcNamespace), eq("bar"))).thenReturn(bar);

        // providing the list of ALL Deployments for all the Kafka Connect clusters
        Labels newLabels = Labels.forStrimziKind(KafkaConnect.RESOURCE_KIND);
        when(mockDcOps.list(eq(kcNamespace), eq(newLabels))).thenReturn(
                List.of(KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar, VERSIONS).generateDeployment(Map.of(), true, null, null)));

        // providing the list Deployments for already "existing" Kafka Connect clusters
        Labels barLabels = Labels.forStrimziCluster("bar");
        when(mockDcOps.list(eq(kcNamespace), eq(barLabels))).thenReturn(
                List.of(KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar, VERSIONS).generateDeployment(Map.of(), true, null, null))
        );
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockNetPolOps.reconcile(any(), eq(kcNamespace), eq(KafkaConnectResources.deploymentName(bar.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockSecretOps.reconcile(any(), eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), eq(kcNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<KafkaConnectStatus> createOrUpdate(Reconciliation reconciliation, KafkaConnect kafkaConnectAssembly) {
                createdOrUpdated.add(kafkaConnectAssembly.getMetadata().getName());
                asyncCreated.flag();
                return Future.succeededFuture();
            }
        };

        Promise<Void> reconciled = Promise.promise();
        // Now try to reconcile all the Kafka Connect clusters
        ops.reconcileAll("test", kcNamespace, ignored -> reconciled.complete());

        reconciled.future().onComplete(context.succeeding(v -> context.verify(() -> {
            assertThat(createdOrUpdated, is(Set.of("foo", "bar")));
            async.flag();
        })));
    }

    @Test
    public void testUpdateClusterWithFailedScaleDownSetsStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kcName = "foo";
        String kcNamespace = "test";
        String failureMsg = "failure";
        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);

        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.getAsync(kcNamespace, connect.getComponentName())).thenReturn(Future.succeededFuture(connect.generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any()))
            .thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockPodOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildPodName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockBcOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildConfigName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName))
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

    @Test
    public void testCreateOrUpdateFailsWhenClusterRoleBindingRightsAreMissingButRequired(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        kc.getSpec().setRack(new RackBuilder().withTopologyKey("some-node-label").build());
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);

        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(), any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kcNamespace, connect.getComponentName())).thenReturn(connect.generateService());
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        when(mockCrbOps.reconcile(any(), any(), any())).thenReturn(Future.failedFuture("Message: Forbidden!"));
        when(mockServiceOps.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), any(), any(), anyInt())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleDown(any(), any(), any(), anyInt())).thenReturn(Future.succeededFuture());
        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockConnectOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnect())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
                .onComplete(context.failing(v -> {
                    assertThat(v.getMessage(), containsString("Message: Forbidden!"));
                    async.flag();
                }));
    }

    @Test
    public void testCreateOrUpdatePassesWhenClusterRoleBindingRightsAreMissingAndNotRequired(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kcName = "foo";
        String kcNamespace = "test";

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);

        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        when(mockConnectOps.updateStatusAsync(any(), any(KafkaConnect.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kcNamespace, connect.getComponentName())).thenReturn(connect.generateService());
        when(mockDcOps.getAsync(kcNamespace, connect.getComponentName())).thenReturn(Future.succeededFuture(connect.generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        when(mockCrbOps.reconcile(any(), any(), any())).thenReturn(Future.failedFuture("Message: Forbidden!"));
        when(mockServiceOps.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), any(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), any(), any(), anyInt())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleDown(any(), any(), any(), anyInt())).thenReturn(Future.succeededFuture());
        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockPodOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildPodName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockBcOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildConfigName(kc.getMetadata().getName())), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockConnectOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaConnect())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName), kc)
                .onComplete(context.succeeding(v -> async.flag()));
    }

    @Test
    public void testDeleteClusterRoleBindings(VertxTestContext context) {
        String kcName = "foo";
        String kcNamespace = "test";

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        ArgumentCaptor<ClusterRoleBinding> desiredCrb = ArgumentCaptor.forClass(ClusterRoleBinding.class);
        when(mockCrbOps.reconcile(any(), eq(KafkaConnectResources.initContainerClusterRoleBindingName(kcName, kcNamespace)), desiredCrb.capture())).thenReturn(Future.succeededFuture());

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockCntrOps = supplier.kafkaConnectorOperator;
        when(mockCntrOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName);

        Checkpoint async = context.checkpoint();

        op.delete(reconciliation)
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(desiredCrb.getValue(), is(nullValue()));
                    Mockito.verify(mockCrbOps, times(1)).reconcile(any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testCreateClusterWithJmxEnabled(VertxTestContext context) {
        String kcName = "foo";
        String kcNamespace = "test";
        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        kc.getMetadata().getAnnotations().put("strimzi.io/use-connector-resources", "true");

        kc.getSpec().setJmxOptions(new KafkaJmxOptionsBuilder()
                .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                .build());

        createKafkaConnectCluster(context, kc, true);
    }

    @Test
    public void testShouldAutoRestartConnector(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
            supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        // Should restart after minute 2 when auto restart count is 1
        var autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(1)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(3).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(op.shouldAutoRestart(autoRestartStatus), is(true));

        // Should not restart before minute 2 when auto restart count is 1
        autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(1)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(1).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(op.shouldAutoRestart(autoRestartStatus), is(false));

        // Should restart after minute 12 when auto restart count is 3
        autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(3)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(13).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(op.shouldAutoRestart(autoRestartStatus), is(true));

        // Should not restart before minute 12 when auto restart count is 3
        autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(3)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(10).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(op.shouldAutoRestart(autoRestartStatus), is(false));

        // Should not restart after 6 attempts
        autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(7)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusDays(1).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(op.shouldAutoRestart(autoRestartStatus), is(false));

        context.completeNow();
    }

    @Test
    public void testShouldResetAutoRestartStatus(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        // Should reset after minute 2 when auto restart count is 1
        var autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(1)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(3).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(op.shouldResetAutoRestartStatus(autoRestartStatus), is(true));

        // Should not reset before minute 2 when auto restart count is 1
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(1)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(1).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(op.shouldResetAutoRestartStatus(autoRestartStatus), is(false));

        // Should reset after minute 12 when auto restart count is 3
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(3)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(13).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(op.shouldResetAutoRestartStatus(autoRestartStatus), is(true));

        // Should not reset before minute 12 when auto restart count is 3
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(3)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(10).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(op.shouldResetAutoRestartStatus(autoRestartStatus), is(false));

        context.completeNow();
    }

    @Test
    public void testAutoRestartWhenDisabled(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of());
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .build();

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(nullValue()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, never()).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndNotFailed(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of());
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(nullValue()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndFailedFirstRestart(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        when(mockConnectApi.restart(any(), anyInt(), any(), anyBoolean(), anyBoolean())).thenReturn(Future.succeededFuture(Map.of()));
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of("connector", Map.of("state", "FAILED")));
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(notNullValue()));
                    assertThat(r.autoRestart.getCount(), is(1));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(notNullValue()));

                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndFailedSecondRestart(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        when(mockConnectApi.restart(any(), anyInt(), any(), anyBoolean(), anyBoolean())).thenReturn(Future.succeededFuture(Map.of()));
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of("connector", Map.of("state", "FAILED")));
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                    .withNewAutoRestart()
                        .withCount(1)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(3).format(DateTimeFormatter.ISO_INSTANT))
                    .endAutoRestart()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(notNullValue()));
                    assertThat(r.autoRestart.getCount(), is(2));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(notNullValue()));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(not(connector.getStatus().getAutoRestart().getLastRestartTimestamp()))); // Timestamp changed

                    verify(mockConnectApi, times(1)).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartWhenEnabledAndFailedTooEarlyForSecondRestart(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of("connector", Map.of("state", "FAILED")));
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                    .withNewAutoRestart()
                        .withCount(1)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(1).format(DateTimeFormatter.ISO_INSTANT))
                    .endAutoRestart()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(notNullValue()));
                    assertThat(r.autoRestart.getCount(), is(1));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(notNullValue()));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(connector.getStatus().getAutoRestart().getLastRestartTimestamp())); // Timestamp changed

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartReset(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of());
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                    .withNewAutoRestart()
                        .withCount(2)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(8).format(DateTimeFormatter.ISO_INSTANT))
                    .endAutoRestart()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(nullValue()));

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testAutoRestartTooEarlyForReset(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaConnectApi mockConnectApi = mock(KafkaConnectApi.class);
        AbstractConnectOperator.ConnectorStatusAndConditions statusAndConditions = new AbstractConnectOperator.ConnectorStatusAndConditions(Map.of());
        KafkaConnector connector = new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName("my-connector")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewAutoRestart()
                        .withEnabled()
                    .endAutoRestart()
                    .withClassName("MyClass")
                    .withTasksMax(3)
                    .withConfig(Map.of("topic", "my-topic"))
                .endSpec()
                .withNewStatus()
                    .withNewAutoRestart()
                        .withCount(2)
                        .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(5).format(DateTimeFormatter.ISO_INSTANT))
                    .endAutoRestart()
                .endStatus()
                .build();

        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> connectorOperator = supplier.kafkaConnectorOperator;
        when(connectorOperator.getAsync("my-namespace", "my-connector")).thenReturn(Future.succeededFuture(connector));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint checkpoint = context.checkpoint();

        op.autoRestartFailedConnectorAndTasks(Reconciliation.DUMMY_RECONCILIATION, "my-connect-host", mockConnectApi, "my-connector", connector.getSpec(), statusAndConditions, connector)
                .onComplete(context.succeeding(r -> context.verify(() -> {
                    assertThat(r.autoRestart, is(notNullValue()));
                    assertThat(r.autoRestart.getCount(), is(2));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(notNullValue()));
                    assertThat(r.autoRestart.getLastRestartTimestamp(), is(connector.getStatus().getAutoRestart().getLastRestartTimestamp())); // Timestamp changed

                    verify(mockConnectApi, never()).restart(any(), anyInt(), any(), anyBoolean(), anyBoolean());
                    verify(supplier.kafkaConnectorOperator, times(1)).getAsync(any(), any());

                    checkpoint.flag();
                })));
    }

    @Test
    public void testImageStreamValidation(VertxTestContext context) {
        String kcName = "my-connect", kcNamespace = "test";
        String failureMsg = String.format("The build can't start because there is no image stream with name %s", kcName);

        KafkaConnect kc = ResourceUtils.createEmptyKafkaConnect(kcNamespace, kcName);
        kc.getSpec().setBuild(
            new BuildBuilder()
                .withNewImageStreamOutput()
                    .withImage(kcName + ":latest")
                .endImageStreamOutput()
                .withPlugins(new PluginBuilder()
                                .withName("my-connector")
                                .withArtifacts(new JarArtifactBuilder()
                                                .withUrl("https://example.com/my.jar")
                                                .build())
                                .build())
                .build());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        var mockDcOps = supplier.deploymentOperations;
        var mockNetPolOps = supplier.networkPolicyOperator;
        var mockPodOps = supplier.podOperations;
        var mockBcOps = supplier.buildConfigOperations;
        var mockIsOps = supplier.imageStreamOperations;

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS);
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.getAsync(kcNamespace, connect.getComponentName())).thenReturn(
            Future.succeededFuture(connect.generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.deploymentName(kc.getMetadata().getName())), any()))
            .thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockPodOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildPodName(kc.getMetadata().getName())), eq(null)))
            .thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockBcOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.buildConfigName(kc.getMetadata().getName())), eq(null)))
            .thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockBcOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(mockIsOps.getAsync(kcNamespace, kcName)).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("unit-test", KafkaConnect.RESOURCE_KIND, kcNamespace, kcName))
            .onComplete(context.failing(v -> context.verify(() -> {
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("NotReady"));
                assertThat(connectStatus.getConditions().get(0).getMessage(), is(failureMsg));
                async.flag();
            })));
    }
}
