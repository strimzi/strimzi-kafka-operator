/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster;
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
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
public class KafkaMirrorMaker2AssemblyOperatorPodSetTest {
    private final static String NAME = "my-mm2";
    private final static String COMPONENT_NAME = NAME + "-mirrormaker2";
    private final static String NAMESPACE = "my-namespace";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final KubernetesVersion KUBERNETES_VERSION = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;
    private static final Reconciliation RECONCILIATION = new Reconciliation("test", "KafkaMirrorMaker2", NAMESPACE, NAME);
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private static final KafkaMirrorMaker2 MM2 = new KafkaMirrorMaker2Builder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withClusters(List.of())
                .withMirrors(List.of())
            .endSpec()
            .build();
    private static final KafkaMirrorMaker2Cluster CLUSTER = KafkaMirrorMaker2Cluster.fromCrd(RECONCILIATION, MM2, VERSIONS, SHARED_ENV_PROVIDER);

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
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2).build();
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
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());

        // Mock Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.jmxSecretName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(mm2);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(mm2));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
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
                    assertThat(service.getMetadata().getName(), is(KafkaMirrorMaker2Resources.serviceName(NAME)));
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

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));
                    KafkaMirrorMaker2Status mm2Status = capturedMm2Statuses.get(0).getStatus();

                    assertThat(mm2Status.getUrl(), is("http://my-mm2-mirrormaker2-api.my-namespace.svc:8083"));
                    assertThat(mm2Status.getReplicas(), is(3));
                    assertThat(mm2Status.getLabelSelector(), is("strimzi.io/cluster=my-mm2,strimzi.io/name=my-mm2-mirrormaker2,strimzi.io/kind=KafkaMirrorMaker2"));
                    assertThat(mm2Status.getConditions().get(0).getStatus(), is("True"));
                    assertThat(mm2Status.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testScaleUpCluster(VertxTestContext context)  {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2).build();
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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(mm2);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(mm2));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));
                    KafkaMirrorMaker2Status mm2Status = capturedMm2Statuses.get(0).getStatus();

                    assertThat(mm2Status.getUrl(), is("http://my-mm2-mirrormaker2-api.my-namespace.svc:8083"));
                    assertThat(mm2Status.getReplicas(), is(3));
                    assertThat(mm2Status.getLabelSelector(), is("strimzi.io/cluster=my-mm2,strimzi.io/name=my-mm2-mirrormaker2,strimzi.io/kind=KafkaMirrorMaker2"));
                    assertThat(mm2Status.getConditions().get(0).getStatus(), is("True"));
                    assertThat(mm2Status.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testScaleDownCluster(VertxTestContext context)  {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2).build();
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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(mm2);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(mm2));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(3));

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));
                    KafkaMirrorMaker2Status mm2Status = capturedMm2Statuses.get(0).getStatus();

                    assertThat(mm2Status.getUrl(), is("http://my-mm2-mirrormaker2-api.my-namespace.svc:8083"));
                    assertThat(mm2Status.getReplicas(), is(3));
                    assertThat(mm2Status.getLabelSelector(), is("strimzi.io/cluster=my-mm2,strimzi.io/name=my-mm2-mirrormaker2,strimzi.io/kind=KafkaMirrorMaker2"));
                    assertThat(mm2Status.getConditions().get(0).getStatus(), is("True"));
                    assertThat(mm2Status.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testScaleClusterToZero(VertxTestContext context)  {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2)
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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(mm2);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(mm2));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSets
                    List<StrimziPodSet> capturesPodSets = podSetCaptor.getAllValues();
                    assertThat(capturesPodSets.size(), is(1));
                    StrimziPodSet podSet = capturesPodSets.get(0);
                    assertThat(podSet.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(podSet.getSpec().getPods().size(), is(0));

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));
                    KafkaMirrorMaker2Status mm2Status = capturedMm2Statuses.get(0).getStatus();

                    assertThat(mm2Status.getUrl(), is(nullValue()));
                    assertThat(mm2Status.getReplicas(), is(0));
                    assertThat(mm2Status.getLabelSelector(), is("strimzi.io/cluster=my-mm2,strimzi.io/name=my-mm2-mirrormaker2,strimzi.io/kind=KafkaMirrorMaker2"));
                    assertThat(mm2Status.getConditions().get(0).getStatus(), is("True"));
                    assertThat(mm2Status.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testUpdateClusterNoDiff(VertxTestContext context)  {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2).build();
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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(mm2);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(mm2));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened
                    verify(mockPodOps, times(3)).deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false));

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
                    assertThat(service.getMetadata().getName(), is(KafkaMirrorMaker2Resources.serviceName(NAME)));
                    assertThat(service.getSpec().getType(), is("ClusterIP"));
                    assertThat(service.getSpec().getClusterIP(), is(not("None")));

                    Service headlessService = capturedServices.get(1);
                    assertThat(headlessService.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(headlessService.getSpec().getType(), is("ClusterIP"));
                    assertThat(headlessService.getSpec().getClusterIP(), is("None"));

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));
                    KafkaMirrorMaker2Status mm2Status = capturedMm2Statuses.get(0).getStatus();

                    assertThat(mm2Status.getUrl(), is("http://my-mm2-mirrormaker2-api.my-namespace.svc:8083"));
                    assertThat(mm2Status.getReplicas(), is(3));
                    assertThat(mm2Status.getLabelSelector(), is("strimzi.io/cluster=my-mm2,strimzi.io/name=my-mm2-mirrormaker2,strimzi.io/kind=KafkaMirrorMaker2"));
                    assertThat(mm2Status.getConditions().get(0).getStatus(), is("True"));
                    assertThat(mm2Status.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testUpdateCluster(VertxTestContext context)  {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2)
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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(mm2);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(mm2));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
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

                    // Verify services => one regular and one headless
                    List<Service> capturedServices = serviceCaptor.getAllValues();
                    assertThat(capturedServices, hasSize(2));

                    Service service = capturedServices.get(0);
                    assertThat(service.getMetadata().getName(), is(KafkaMirrorMaker2Resources.serviceName(NAME)));
                    assertThat(service.getSpec().getType(), is("ClusterIP"));
                    assertThat(service.getSpec().getClusterIP(), is(not("None")));

                    Service headlessService = capturedServices.get(1);
                    assertThat(headlessService.getMetadata().getName(), is(COMPONENT_NAME));
                    assertThat(headlessService.getSpec().getType(), is("ClusterIP"));
                    assertThat(headlessService.getSpec().getClusterIP(), is("None"));

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));
                    KafkaMirrorMaker2Status mm2Status = capturedMm2Statuses.get(0).getStatus();

                    assertThat(mm2Status.getUrl(), is("http://my-mm2-mirrormaker2-api.my-namespace.svc:8083"));
                    assertThat(mm2Status.getReplicas(), is(3));
                    assertThat(mm2Status.getLabelSelector(), is("strimzi.io/cluster=my-mm2,strimzi.io/name=my-mm2-mirrormaker2,strimzi.io/kind=KafkaMirrorMaker2"));
                    assertThat(mm2Status.getConditions().get(0).getStatus(), is("True"));
                    assertThat(mm2Status.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testUpdateWithFailure(VertxTestContext context)  {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2).build();
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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(mm2);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(mm2));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(v.getMessage(), is("Test failure"));

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));
                    KafkaMirrorMaker2Status mm2Status = capturedMm2Statuses.get(0).getStatus();

                    assertThat(mm2Status.getUrl(), is("http://my-mm2-mirrormaker2-api.my-namespace.svc:8083"));
                    assertThat(mm2Status.getReplicas(), is(3));
                    assertThat(mm2Status.getLabelSelector(), is("strimzi.io/cluster=my-mm2,strimzi.io/name=my-mm2-mirrormaker2,strimzi.io/kind=KafkaMirrorMaker2"));
                    assertThat(mm2Status.getConditions().get(0).getStatus(), is("True"));
                    assertThat(mm2Status.getConditions().get(0).getType(), is("NotReady"));

                    async.flag();
                })));
    }

    @Test
    public void testReconcileAll(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;

        String kmm2Namespace = "test";

        KafkaMirrorMaker2 foo = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, "foo");
        KafkaMirrorMaker2 bar = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, "bar");
        when(mockMirrorMaker2Ops.listAsync(eq(kmm2Namespace), isNull(LabelSelector.class))).thenReturn(Future.succeededFuture(List.of(foo, bar)));
        when(mockMirrorMaker2Ops.getAsync(eq(kmm2Namespace), eq("foo"))).thenReturn(Future.succeededFuture(foo));
        when(mockMirrorMaker2Ops.getAsync(eq(kmm2Namespace), eq("bar"))).thenReturn(Future.succeededFuture(bar));

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        Checkpoint createOrUpdateAsync = context.checkpoint(2);
        Checkpoint async = context.checkpoint();

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KUBERNETES_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig()) {

            @Override
            public Future<KafkaMirrorMaker2Status> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2Assembly) {
                createdOrUpdated.add(kafkaMirrorMaker2Assembly.getMetadata().getName());
                createOrUpdateAsync.flag();
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka MirrorMaker 2 clusters
        ops.reconcileAll("test", kmm2Namespace,
            context.succeeding(v -> context.verify(() -> {
                assertThat(createdOrUpdated, is(Set.of("foo", "bar")));
                async.flag();
            })));

    }

    @Test
    public void testClusterMigrationToPodSets(VertxTestContext context)  {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder(MM2).build();

        Deployment deployment = new DeploymentBuilder()
                .withNewMetadata()
                    .withName(KafkaMirrorMaker2Resources.componentName(NAME))
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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(mm2);
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(mm2));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
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
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));
                    KafkaMirrorMaker2Status mm2Status = capturedMm2Statuses.get(0).getStatus();

                    assertThat(mm2Status.getUrl(), is("http://my-mm2-mirrormaker2-api.my-namespace.svc:8083"));
                    assertThat(mm2Status.getReplicas(), is(3));
                    assertThat(mm2Status.getLabelSelector(), is("strimzi.io/cluster=my-mm2,strimzi.io/name=my-mm2-mirrormaker2,strimzi.io/kind=KafkaMirrorMaker2"));
                    assertThat(mm2Status.getConditions().get(0).getStatus(), is("True"));
                    assertThat(mm2Status.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @Test
    public void testTopicsGroupsExclude(VertxTestContext context) {
        String kmm2Name = "foo";
        String sourceNamespace = "source-ns";
        String targetNamespace = "target-ns";
        String sourceClusterAlias = "my-cluster-src";
        String targetClusterAlias = "my-cluster-tgt";
        String excludedTopicList = "excludedTopic0,excludedTopic1";
        String excludedGroupList = "excludedGroup0,excludedGroup1";

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(targetNamespace, kmm2Name);
        ArgumentCaptor<KafkaMirrorMaker2> mirrorMaker2Captor = createMirrorMaker2CaptorMock(targetNamespace, kmm2Name, kmm2, supplier);
        KafkaConnectApi mockConnectClient = createConnectClientMock();

        KafkaMirrorMaker2ClusterSpec sourceCluster =
                new KafkaMirrorMaker2ClusterSpecBuilder(true)
                        .withAlias(sourceClusterAlias)
                        .withBootstrapServers(sourceClusterAlias + "." + sourceNamespace + ".svc:9092")
                        .build();
        KafkaMirrorMaker2ClusterSpec targetCluster =
                new KafkaMirrorMaker2ClusterSpecBuilder(true)
                        .withAlias(targetClusterAlias)
                        .withBootstrapServers(targetClusterAlias + "." + targetNamespace + ".svc:9092")
                        .build();
        kmm2.getSpec().setClusters(List.of(sourceCluster, targetCluster));
        kmm2.getSpec().setConnectCluster(targetClusterAlias);

        KafkaMirrorMaker2MirrorSpec deprecatedMirrorConnector = new KafkaMirrorMaker2MirrorSpecBuilder()
                .withSourceCluster(sourceClusterAlias)
                .withTargetCluster(targetClusterAlias)
                .withTopicsExcludePattern(excludedTopicList)
                .withGroupsExcludePattern(excludedGroupList)
                .build();
        kmm2.getSpec().setMirrors(List.of(deprecatedMirrorConnector));

        KafkaMirrorMaker2AssemblyOperator mm2AssemblyOperator = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS, supplier.sharedEnvironmentProvider);
        mm2AssemblyOperator.reconcile(new Reconciliation("test-exclude", KafkaMirrorMaker2.RESOURCE_KIND, targetNamespace, kmm2Name))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaMirrorMaker2MirrorSpec capturedMirrorConnector = mirrorMaker2Captor.getAllValues().get(0).getSpec().getMirrors().get(0);
                    assertThat(capturedMirrorConnector.getTopicsExcludePattern(), is(excludedTopicList));
                    assertThat(capturedMirrorConnector.getGroupsExcludePattern(), is(excludedGroupList));
                    async.flag();
                })));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testTopicsGroupsBlacklist(VertxTestContext context) {
        String kmm2Name = "foo";
        String sourceNamespace = "source-ns";
        String targetNamespace = "target-ns";
        String sourceClusterAlias = "my-cluster-src";
        String targetClusterAlias = "my-cluster-tgt";
        String excludedTopicList = "excludedTopic0,excludedTopic1";
        String excludedGroupList = "excludedGroup0,excludedGroup1";

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(targetNamespace, kmm2Name);
        ArgumentCaptor<KafkaMirrorMaker2> mirrorMaker2Captor = createMirrorMaker2CaptorMock(targetNamespace, kmm2Name, kmm2, supplier);
        KafkaConnectApi mockConnectClient = createConnectClientMock();

        KafkaMirrorMaker2ClusterSpec sourceCluster =
                new KafkaMirrorMaker2ClusterSpecBuilder(true)
                        .withAlias(sourceClusterAlias)
                        .withBootstrapServers(sourceClusterAlias + "." + sourceNamespace + ".svc:9092")
                        .build();
        KafkaMirrorMaker2ClusterSpec targetCluster =
                new KafkaMirrorMaker2ClusterSpecBuilder(true)
                        .withAlias(targetClusterAlias)
                        .withBootstrapServers(targetClusterAlias + "." + targetNamespace + ".svc:9092")
                        .build();
        kmm2.getSpec().setClusters(List.of(sourceCluster, targetCluster));
        kmm2.getSpec().setConnectCluster(targetClusterAlias);

        KafkaMirrorMaker2MirrorSpec deprecatedMirrorConnector = new KafkaMirrorMaker2MirrorSpecBuilder()
                .withSourceCluster(sourceClusterAlias)
                .withTargetCluster(targetClusterAlias)
                .withTopicsBlacklistPattern(excludedTopicList)
                .withGroupsBlacklistPattern(excludedGroupList)
                .build();
        kmm2.getSpec().setMirrors(List.of(deprecatedMirrorConnector));

        KafkaMirrorMaker2AssemblyOperator mm2AssemblyOperator = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig(), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS, supplier.sharedEnvironmentProvider);
        mm2AssemblyOperator.reconcile(new Reconciliation("test-blacklist", KafkaMirrorMaker2.RESOURCE_KIND, targetNamespace, kmm2Name))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    KafkaMirrorMaker2MirrorSpec capturedMirrorConnector = mirrorMaker2Captor.getAllValues().get(0).getSpec().getMirrors().get(0);
                    assertThat(capturedMirrorConnector.getTopicsBlacklistPattern(), is(excludedTopicList));
                    assertThat(capturedMirrorConnector.getGroupsBlacklistPattern(), is(excludedGroupList));
                    async.flag();
                })));
    }

    @Test
    public void testDeleteClusterRoleBindings(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock CRB resources
        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        ArgumentCaptor<ClusterRoleBinding> desiredCrb = ArgumentCaptor.forClass(ClusterRoleBinding.class);
        when(mockCrbOps.reconcile(any(), eq(KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(NAME, NAMESPACE)), desiredCrb.capture())).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator op = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier, ResourceUtils.dummyClusterOperatorConfig());
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.delete(reconciliation)
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(desiredCrb.getValue(), is(nullValue()));
                    Mockito.verify(mockCrbOps, times(1)).reconcile(any(), eq(KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(NAME, NAMESPACE)), any());

                    async.flag();
                })));
    }

    private ArgumentCaptor<KafkaMirrorMaker2> createMirrorMaker2CaptorMock(String targetNamespace, String kmm2Name, KafkaMirrorMaker2 kmm2, ResourceOperatorSupplier supplier) {
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodOperator mockPodOps = supplier.podOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        when(mockMirrorMaker2Ops.get(targetNamespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));

        when(mockServiceOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.componentName(
                kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        when(mockSecretOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockPodSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture());
        when(mockPodSetOps.reconcile(any(), anyString(), anyString(), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));
        when(mockPodSetOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(any(), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaMirrorMaker2> mirrorMaker2Captor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), mirrorMaker2Captor.capture())).thenReturn(Future.succeededFuture());

        return mirrorMaker2Captor;
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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(new KafkaMirrorMaker2Builder(MM2).build());
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(new KafkaMirrorMaker2Builder(MM2).build()));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened => it should happen 6 times:
                    //    * First for the manual rolling update
                    //    * Then the regular rolling update (caused by the mock being imperfect)
                    verify(mockPodOps, times(6)).deleteAsync(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), eq(false));

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));

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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(new KafkaMirrorMaker2Builder(MM2).build());
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(new KafkaMirrorMaker2Builder(MM2).build()));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened => it should happen 2 times:
                    //    * First for the one Pod that exists before the scale-up goes through manual rolling update
                    //    * Then the one Pod that exists before the scale-up goes through regular rolling update (caused by the mock being imperfect)
                    //    * The scaled-up Pods are not rolled
                    verify(mockPodOps, times(2)).deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME + "-0"), eq(false));

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));

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

        // Mock KafkaMirrorMaker2 CRs
        CrdOperator<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> mockConnectOps = supplier.mirrorMaker2Operator;
        when(mockConnectOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(new KafkaMirrorMaker2Builder(MM2).build());
        when(mockConnectOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(new KafkaMirrorMaker2Builder(MM2).build()));
        ArgumentCaptor<KafkaMirrorMaker2> mm2StatusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockConnectOps.updateStatusAsync(any(), mm2StatusCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock Connect API
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(
                vertx,
                new PlatformFeaturesAvailability(false, KUBERNETES_VERSION),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(),
                x -> mockConnectClient
        );

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Check rolling happened => Should happen once as a regular rolling update to all pods and once more for the annotated pod
                    verify(mockPodOps, times(1)).deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME + "-0"), eq(false));
                    verify(mockPodOps, times(2)).deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME + "-1"), eq(false));
                    verify(mockPodOps, times(1)).deleteAsync(any(), eq(NAMESPACE), eq(COMPONENT_NAME + "-2"), eq(false));

                    // Verify CR status
                    List<KafkaMirrorMaker2> capturedMm2Statuses = mm2StatusCaptor.getAllValues();
                    assertThat(capturedMm2Statuses, hasSize(1));

                    async.flag();
                })));
    }

    private KafkaConnectApi createConnectClientMock() {
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(any(), anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());
        return mockConnectClient;
    }
}
