/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.connect.ConnectorPluginBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectList;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connect.build.BuildBuilder;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ClusterRoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiPredicate;

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
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
@ExtendWith(VertxExtension.class)
public class KafkaConnectAssemblyOperatorPodSetTest {
    private final static String NAME = "my-connect";
    private final static String COMPONENT_NAME = NAME + "-connect";
    private final static String NAMESPACE = "my-namespace";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final KubernetesVersion KUBERNETES_VERSION = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;
    private static final Reconciliation RECONCILIATION = new Reconciliation("test", "KafkaConnect", NAMESPACE, NAME);
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private static final KafkaConnect CONNECT = new KafkaConnectBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
            .endSpec()
            .build();
    private static final KafkaConnectCluster CLUSTER = KafkaConnectCluster.fromCrd(RECONCILIATION, CONNECT, VERSIONS, SHARED_ENV_PROVIDER);

    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateCluster(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT).build();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), npCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect REST API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    // Verify services => one regular and one headless
                    List<Service> capturedServices = serviceCaptor.getAllValues();
                    assertThat(capturedServices, hasSize(2));

                    Service service = capturedServices.get(0);
                    assertThat(service.getMetadata().getName(), is(KafkaConnectResources.serviceName(NAME)));
                    assertThat(service.getSpec().getType(), is("ClusterIP"));
                    assertThat(service.getSpec().getClusterIP(), is(not("None")));

                    Service headlessService = capturedServices.get(1);
                    assertThat(headlessService.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(headlessService.getSpec().getType(), is("ClusterIP"));
                    assertThat(headlessService.getSpec().getClusterIP(), is("None"));

                    // Verify Deployment => one getAsync call
                    verify(mockDepOps, times(1)).getAsync(eq(NAMESPACE), eq(COMPONENT_NAME));

                    // Verify PDB => should be set up for custom controller
                    List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                    assertThat(capturedPdb.size(), is(1));
                    PodDisruptionBudget pdb = capturedPdb.get(0);
                    assertThat(pdb.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(2));

                    // NetworkPolicies => No ConnectorOperator, no policy
                    assertThat(npCaptor.getValue(), is(nullValue()));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    assertThat(connectStatus.getConnectorPlugins(), nullValue());

                    async.flag();
                })));
    }

    @Test
    public void testCreateClusterWithConnectorOperator(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .build();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), npCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connector CRs
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock Connect REST API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    // Verify services => one regular and one headless
                    List<Service> capturedServices = serviceCaptor.getAllValues();
                    assertThat(capturedServices, hasSize(2));

                    Service service = capturedServices.get(0);
                    assertThat(service.getMetadata().getName(), is(KafkaConnectResources.serviceName(NAME)));
                    assertThat(service.getSpec().getType(), is("ClusterIP"));
                    assertThat(service.getSpec().getClusterIP(), is(not("None")));

                    Service headlessService = capturedServices.get(1);
                    assertThat(headlessService.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(headlessService.getSpec().getType(), is("ClusterIP"));
                    assertThat(headlessService.getSpec().getClusterIP(), is("None"));

                    // Verify Deployment => one getAsync call
                    verify(mockDepOps, times(1)).getAsync(eq(NAMESPACE), eq(COMPONENT_NAME));

                    // Verify PDB => should be set up for custom controller
                    List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                    assertThat(capturedPdb.size(), is(1));
                    PodDisruptionBudget pdb = capturedPdb.get(0);
                    assertThat(pdb.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(pdb.getSpec().getMinAvailable().getIntVal(), is(2));

                    // NetworkPolicies => Connector Operator, policy is created
                    assertThat(npCaptor.getValue(), is(notNullValue()));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    assertThat(connectStatus.getConnectorPlugins(), hasSize(1));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getConnectorClass(), is("io.strimzi.MyClass"));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getType(), is("sink"));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getVersion(), is("1.0.0"));

                    async.flag();
                })));
    }

    @Test
    public void testScaleUpCluster(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT).build();
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(1, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testScaleDownCluster(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT).build();
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(5, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testScaleClusterToZero(VertxTestContext context)  {
        KafkaConnect newConnect = new KafkaConnectBuilder(CONNECT)
                .editSpec()
                    .withReplicas(0)
                .endSpec()
                .build();

        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(newConnect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(newConnect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(0));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is(nullValue()));
                    assertThat(connectStatus.getReplicas(), is(0));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testUpdateClusterNoDiff(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT).build();
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testUpdateCluster(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT)
                .editSpec()
                    .withImage("some/different:image")
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("Memory", new Quantity("1Gi"))).build())
                .endSpec()
                .build();
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), npCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened
                    verify(mockPodOps, times(3)).deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false));

                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));
                    PodSetUtils.podSetToPods(podSet).forEach(pod -> {
                        assertThat(pod.getSpec().getContainers().get(0).getImage(), is("some/different:image"));
                        assertThat(pod.getSpec().getContainers().get(0).getResources(), is(new ResourceRequirementsBuilder().withRequests(Map.of("Memory", new Quantity("1Gi"))).build()));
                    });

                    // NetworkPolicies => No Connector Operator, no policy is created
                    assertThat(npCaptor.getValue(), is(Matchers.nullValue()));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    assertThat(connectStatus.getConnectorPlugins(), nullValue());

                    async.flag();
                })));
    }

    @Test
    public void testUpdateClusterWithConnectors(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editSpec()
                    .withImage("some/different:image")
                    .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("Memory", new Quantity("1Gi"))).build())
                .endSpec()
                .build();
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), npCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connector CRs
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock Connect REST API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        ConnectorPlugin plugin1 = new ConnectorPluginBuilder()
                .withConnectorClass("io.strimzi.MyClass")
                .withType("sink")
                .withVersion("1.0.0")
                .build();
        when(mockConnectClient.listConnectorPlugins(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(singletonList(plugin1)));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened
                    verify(mockPodOps, times(3)).deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false));

                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));
                    PodSetUtils.podSetToPods(podSet).forEach(pod -> {
                        assertThat(pod.getSpec().getContainers().get(0).getImage(), is("some/different:image"));
                        assertThat(pod.getSpec().getContainers().get(0).getResources(), is(new ResourceRequirementsBuilder().withRequests(Map.of("Memory", new Quantity("1Gi"))).build()));
                    });

                    // NetworkPolicies => Connector Operator, policy is created
                    assertThat(npCaptor.getValue(), is(notNullValue()));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    assertThat(connectStatus.getConnectorPlugins(), hasSize(1));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getConnectorClass(), is("io.strimzi.MyClass"));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getType(), is("sink"));
                    assertThat(connectStatus.getConnectorPlugins().get(0).getVersion(), is("1.0.0"));

                    async.flag();
                })));
    }

    @Test
    public void testUpdateClusterWithFailure(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT).build();
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.failedFuture("Test failure"));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), npCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v.getMessage(), is("Test failure"));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("NotReady"));

                    async.flag();
                })));
    }

    @Test
    public void testUpdateClusterWithoutRequiredRBACRights(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT)
                .editSpec()
                    .withNewRack()
                        .withTopologyKey("my-zone-key")
                    .endRack()
                .endSpec()
                .build();
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), npCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock ClusterRoleBindings
        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        when(mockCrbOps.reconcile(any(), any(), any())).thenReturn(Future.failedFuture(new KubernetesClientException("Forbidden!", 403, null)));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v.getMessage(), containsString("Forbidden!"));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("NotReady"));

                    assertThat(connectStatus.getConnectorPlugins(), nullValue());

                    async.flag();
                })));
    }

    @Test
    public void testUpdateClusterWithoutOptionalRBACRights(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT).build();
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> npCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), npCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock ClusterRoleBindings
        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        when(mockCrbOps.reconcile(any(), any(), any())).thenReturn(Future.failedFuture(new KubernetesClientException("Forbidden!", 403, null)));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened
                    verify(mockPodOps, times(3)).deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false));

                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    // NetworkPolicies => No Connector Operator, no policy is created
                    assertThat(npCaptor.getValue(), is(Matchers.nullValue()));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    assertThat(connectStatus.getConnectorPlugins(), nullValue());

                    async.flag();
                })));
    }

    @Test
    public void testReconcileAll(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        var mockConnectorOps = supplier.kafkaConnectorOperator;

        when(mockConnectorOps.listAsync(any(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(List.of()));
        String kcNamespace = "test";

        KafkaConnect foo = ResourceUtils.createEmptyKafkaConnect(kcNamespace, "foo");
        KafkaConnect bar = ResourceUtils.createEmptyKafkaConnect(kcNamespace, "bar");
        when(mockConnectOps.listAsync(eq(kcNamespace), isNull(LabelSelector.class))).thenReturn(Future.succeededFuture(List.of(foo, bar)));
        when(mockConnectOps.getAsync(eq(kcNamespace), eq("foo"))).thenReturn(Future.succeededFuture(foo));
        when(mockConnectOps.getAsync(eq(kcNamespace), eq("bar"))).thenReturn(Future.succeededFuture(bar));

        Checkpoint asyncCreated = context.checkpoint(2);
        Checkpoint async = context.checkpoint();

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KUBERNETES_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig()) {

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
    @SuppressWarnings("unchecked")
    public void testBuildChangedCluster(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT)
                .editSpec()
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage("my-image:latest")
                            .withPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(new PluginBuilder()
                                .withName("plugin1")
                                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                                .build())
                    .endBuild()
                .endSpec()
                .build();
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);
        Pod terminatedBuildPod = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withContainerStatuses(new ContainerStatusBuilder()
                        .withName(KafkaConnectResources.buildPodName(NAME))
                        .withNewState()
                            .withNewTerminated()
                                .withExitCode(0)
                                .withMessage("my-connect-build@sha256:blablabla")
                            .endTerminated()
                        .endState()
                        .build())
                .endStatus()
                .build();


        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.waitFor(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            if (KafkaConnectResources.buildPodName(NAME).equals(i.getArgument(1)))  {
                return Future.succeededFuture(terminatedBuildPod);
            } else {
                Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
                return Future.succeededFuture(pod);
            }
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());



        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened
                    verify(mockPodOps, times(3)).deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false));

                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getMetadata().getAnnotations().get(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION), is("28987d00751944b0"));
                    assertThat(podSet.getMetadata().getAnnotations().get(Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE), is("my-connect-build@sha256:blablabla"));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    for (Pod pod : PodSetUtils.podSetToPods(podSet))  {
                        assertThat(pod.getMetadata().getAnnotations().get(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION), is("28987d00751944b0"));
                        assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                    }

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testClusterMigrationToPodSets(VertxTestContext context)  {
        KafkaConnect connect = new KafkaConnectBuilder(CONNECT).build();

        Deployment deployment = new DeploymentBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.componentName(NAME))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(deployment));
        when(mockDepOps.scaleDown(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyInt(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyBoolean())).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(null));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(connect);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(connect));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check migration happened
                    verify(mockDepOps, times(1)).deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(true));
                    verify(mockDepOps, times(2)).scaleDown(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyInt(), anyLong());

                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(4));

                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(1));

                    podSet = capturesPodSets.get(1);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(2));

                    podSet = capturesPodSets.get(2);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    podSet = capturesPodSets.get(3);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();

                    assertThat(connectStatus.getUrl(), is("http://my-connect-connect-api.my-namespace.svc:8083"));
                    assertThat(connectStatus.getReplicas(), is(3));
                    assertThat(connectStatus.getLabelSelector(), is("strimzi.io/cluster=my-connect,strimzi.io/name=my-connect-connect,strimzi.io/kind=KafkaConnect"));
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
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
        when(mockCntrOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        KafkaConnectAssemblyOperator op = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
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
                        .withArtifacts(new JarArtifactBuilder().withUrl("https://example.com/my.jar").build())
                        .build())
                .build());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockConnectOps = supplier.connectOperator;
        var mockDcOps = supplier.deploymentOperations;
        var mockPodSetOps = supplier.strimziPodSetOperator;
        var mockNetPolOps = supplier.networkPolicyOperator;
        var mockBcOps = supplier.buildConfigOperations;
        var mockIsOps = supplier.imageStreamOperations;
        var mockPodOps = supplier.podOperations;

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, SHARED_ENV_PROVIDER);
        when(mockConnectOps.get(kcNamespace, kcName)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.getAsync(kcNamespace, connect.getComponentName())).thenReturn(Future.succeededFuture(null));
        when(mockPodSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture());
        when(mockNetPolOps.reconcile(any(), eq(kc.getMetadata().getNamespace()), eq(KafkaConnectResources.componentName(kc.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockBcOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(mockIsOps.getAsync(kcNamespace, kcName)).thenReturn(Future.succeededFuture());
        when(mockPodOps.listAsync(eq(kcNamespace), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());

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

    @Test
    public void testManualRollingUpdate(VertxTestContext context)  {
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        oldPodSet.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"); // We want the pods to roll manually
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(new KafkaConnectBuilder(CONNECT).build());
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(new KafkaConnectBuilder(CONNECT).build()));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened => it should happen 6 times:
                    //    * First for the manual rolling update
                    //    * Then the regular rolling update (caused by the mock being imperfect)
                    verify(mockPodOps, times(6)).deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));

                    async.flag();
                })));
    }

    @Test
    public void testManualRollingUpdateAtScaleUp(VertxTestContext context)  {
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(1, null, null, false, null, null, null);
        oldPodSet.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"); // We want the pods to roll manually
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(new KafkaConnectBuilder(CONNECT).build());
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(new KafkaConnectBuilder(CONNECT).build()));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened => it should happen 2 times:
                    //    * First for the one Pod that exists before the scale-up goes through manual rolling update
                    //    * Then the one Pod that exists before the scale-up goes through regular rolling update (caused by the mock being imperfect)
                    //    * The scaled-up Pods are not rolled
                    verify(mockPodOps, times(2)).deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME + "-0"), eq(false));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));

                    async.flag();
                })));
    }

    @Test
    public void testManualRollingUpdatePerPod(VertxTestContext context)  {
        StrimziPodSet oldPodSet = CLUSTER.generatePodSet(3, null, null, false, null, null, null);
        List<Pod> oldPods = PodSetUtils.podSetToPods(oldPodSet);
        oldPods.get(1).getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"); // We want the pod to roll manually

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock deployment
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture(oldPodSet));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Network Policies
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(oldPods));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenAnswer(i -> {
            Pod pod = oldPods.stream().filter(p -> i.getArgument(1).equals(p.getMetadata().getName())).findFirst().orElse(null);
            return Future.succeededFuture(pod);
        });
        when(mockPodOps.deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock Connect CRs
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(new KafkaConnectBuilder(CONNECT).build());
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(new KafkaConnectBuilder(CONNECT).build()));
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig()
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened => Should happen once as a regular rolling update to all pods and once more for the annotated pod
                    verify(mockPodOps, times(1)).deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME + "-0"), eq(false));
                    verify(mockPodOps, times(2)).deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME + "-1"), eq(false));
                    verify(mockPodOps, times(1)).deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME + "-2"), eq(false));

                    // Verify CR status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));

                    async.flag();
                })));
    }
}
