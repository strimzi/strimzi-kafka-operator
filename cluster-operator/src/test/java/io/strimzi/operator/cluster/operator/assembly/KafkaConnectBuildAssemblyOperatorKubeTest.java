/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectList;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaConnectBuild;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.BuildConfigOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaConnectBuildAssemblyOperatorKubeTest {
    private static final String NAMESPACE = "my-ns";
    private static final String NAME = "my-connect";
    private final static String COMPONENT_NAME = NAME + "-connect";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static final String OUTPUT_IMAGE = "my-connect-build:latest";
    private static final String OUTPUT_IMAGE_HASH_STUB = Util.hashStub(OUTPUT_IMAGE);

    protected static Vertx vertx;
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;

    private final SharedEnvironmentProvider sharedEnvironmentProvider = new MockSharedEnvironmentProvider();

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testBuildOnKube(VertxTestContext context) {
        Plugin plugin1 = new PluginBuilder()
                .withName("plugin1")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                .build();

        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers("my-cluster-kafka-bootstrap:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage(OUTPUT_IMAGE)
                            .withPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock Secret ops
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock StrimziPodSet ops
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture SA ops
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), anyString(), anyString(), saCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ServiceAccount())));

        // Mock and capture Pod ops
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod terminatedPod = new PodBuilder()
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
        when(mockPodOps.waitFor(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(null), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify PodSet
                List<StrimziPodSet> capturedSps = podSetCaptor.getAllValues();
                assertThat(capturedSps, hasSize(1));
                StrimziPodSet sps = capturedSps.get(0);
                assertThat(sps.getMetadata().getName(), is(connect.getComponentName()));
                assertThat(Annotations.stringAnnotation(sps, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                PodSetUtils.podSetToPods(sps).forEach(pod -> {
                    assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                    assertThat(Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                });

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(1));
                ConfigMap dockerfileCm = capturedCms.get(0);
                assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                // Verify Service Account
                List<ServiceAccount> capturedSas = saCaptor.getAllValues();
                assertThat(capturedSas, hasSize(2));
                ServiceAccount sa = capturedSas.get(0);
                assertThat(sa.getMetadata().getName(), is(KafkaConnectResources.serviceAccountName(NAME)));
                sa = capturedSas.get(1);
                assertThat(sa.getMetadata().getName(), is(KafkaConnectResources.buildServiceAccountName(NAME)));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(2));
                assertThat(capturedBuilderPods.stream().filter(Objects::nonNull).collect(Collectors.toList()), hasSize(1));

                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                async.flag();
            })));
    }

    @Test
    public void testBuildFailureOnKube(VertxTestContext context) {
        Plugin plugin1 = new PluginBuilder()
                .withName("plugin1")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                .build();

        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers("my-cluster-kafka-bootstrap:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage(OUTPUT_IMAGE)
                            .withPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock deployment ops
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock Secret ops
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock StrimziPodSet ops
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod terminatedPod = new PodBuilder()
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
                                .withExitCode(1)
                            .endTerminated()
                        .endState()
                        .build())
                .endStatus()
                .build();
        when(mockPodOps.waitFor(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(null), Future.succeededFuture(terminatedPod));
        // Kafka Connect (not builder) Pods
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.failing(v -> context.verify(() -> {
                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(1));
                assertThat(capturedBuilderPods.stream().filter(Objects::nonNull).collect(Collectors.toList()), hasSize(1));

                async.flag();
            })));
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testUpdateWithPluginChangeOnKube(VertxTestContext context) {
        Plugin plugin1 = new PluginBuilder()
                .withName("plugin1")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                .build();

        Plugin plugin2 = new PluginBuilder()
                .withName("plugin2")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my2.jar").build())
                .build();

        KafkaConnect oldKc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers("my-cluster-kafka-bootstrap:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage(OUTPUT_IMAGE)
                            .withPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster oldConnect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);
        KafkaConnectBuild oldBuild = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);

        KafkaConnect kc = new KafkaConnectBuilder(oldKc)
                .editSpec()
                    .editBuild()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock Secret ops
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock StrimziPodSet ops
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenAnswer(i -> {
            StrimziPodSet sps = oldConnect
                    .generatePodSet(
                            3,
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub() + Util.hashStub(oldBuild.getBuild().getOutput().getImage()), Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:olddigest"),
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub() + Util.hashStub(oldBuild.getBuild().getOutput().getImage()), Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:olddigest"),
                            false,
                            null,
                            null,
                            null);

            return Future.succeededFuture(sps);
        });
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod terminatedPod = new PodBuilder()
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
        when(mockPodOps.waitFor(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(null), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify PodSet
                List<StrimziPodSet> capturedSps = podSetCaptor.getAllValues();
                assertThat(capturedSps, hasSize(1));
                StrimziPodSet sps = capturedSps.get(0);
                assertThat(sps.getMetadata().getName(), is(connect.getComponentName()));
                assertThat(Annotations.stringAnnotation(sps, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                PodSetUtils.podSetToPods(sps).forEach(pod -> {
                    assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                    assertThat(Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                });

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(1));
                ConfigMap dockerfileCm = capturedCms.get(0);
                assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(2));
                assertThat(capturedBuilderPods.stream().filter(Objects::nonNull).collect(Collectors.toList()), hasSize(1));

                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                async.flag();
            })));
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testUpdateWithBuildImageChangeOnKube(VertxTestContext context) {
        Plugin plugin1 = new PluginBuilder()
                .withName("plugin1")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                .build();

        KafkaConnect oldKc = new KafkaConnectBuilder()
                .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .withReplicas(1)
                .withBootstrapServers("my-cluster-kafka-bootstrap:9092")
                .withNewBuild()
                .withNewDockerOutput()
                .withImage(OUTPUT_IMAGE)
                .withPushSecret("my-docker-credentials")
                .endDockerOutput()
                .withPlugins(plugin1)
                .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster oldConnect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);
        KafkaConnectBuild oldBuild = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);

        KafkaConnect kc = new KafkaConnectBuilder(oldKc)
                .editSpec()
                .editBuild()
                .withNewDockerOutput()
                .withImage("my-connect-build-2:blah")
                .withPushSecret("my-docker-credentials")
                .endDockerOutput()
                .withPlugins(plugin1)
                .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock Secret ops
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock StrimziPodSet ops
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenAnswer(i -> {
            StrimziPodSet sps = oldConnect
                    .generatePodSet(
                            3,
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub() + Util.hashStub(oldBuild.getBuild().getOutput().getImage()), Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build-2@sha256:olddigest"),
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub() + Util.hashStub(oldBuild.getBuild().getOutput().getImage()), Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build-2@sha256:olddigest"),
                            false,
                            null,
                            null,
                            null);

            return Future.succeededFuture(sps);
        });
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Pod> connectPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), connectPodCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod terminatedPod = new PodBuilder()
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
                            .withMessage("my-connect-build-2@sha256:blablabla")
                        .endTerminated()
                    .endState()
                    .build())
                .endStatus()
                .build();
        when(mockPodOps.waitFor(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(null), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // Verify PodSet
                    List<StrimziPodSet> capturedSps = podSetCaptor.getAllValues();
                    assertThat(capturedSps, hasSize(1));
                    StrimziPodSet sps = capturedSps.get(0);
                    assertThat(sps.getMetadata().getName(), is(connect.getComponentName()));
                    assertThat(Annotations.stringAnnotation(sps, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + Util.hashStub(build.getBuild().getOutput().getImage())));
                    PodSetUtils.podSetToPods(sps).forEach(pod -> {
                        assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-connect-build-2@sha256:blablabla"));
                        assertThat(Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + Util.hashStub(build.getBuild().getOutput().getImage())));
                    });

                    // Verify ConfigMap
                    List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                    assertThat(capturedCms, hasSize(1));
                    ConfigMap dockerfileCm = capturedCms.get(0);
                    assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                    assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                    // Verify builder Pod
                    List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                    assertThat(capturedBuilderPods, hasSize(2));
                    assertThat(capturedBuilderPods.stream().filter(Objects::nonNull).collect(Collectors.toList()), hasSize(1));

                    // Verify status
                    List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                    assertThat(capturedConnects, hasSize(1));
                    KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                    assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                    async.flag();
                })));
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testContinueWithPreviousBuildOnKube(VertxTestContext context) {
        Plugin plugin1 = new PluginBuilder()
                .withName("plugin1")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                .build();

        Plugin plugin2 = new PluginBuilder()
                .withName("plugin2")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my2.jar").build())
                .build();

        KafkaConnect oldKc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers("my-cluster-kafka-bootstrap:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage(OUTPUT_IMAGE)
                            .withPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster oldConnect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);
        KafkaConnectBuild oldBuild = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);

        KafkaConnect kc = new KafkaConnectBuilder(oldKc)
                .editSpec()
                    .editBuild()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock Secret ops
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock StrimziPodSet ops
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenAnswer(i -> {
            StrimziPodSet sps = oldConnect
                    .generatePodSet(
                            3,
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, "oldhashstub", Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:olddigest"),
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, "oldhashstub", Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:olddigest"),
                            false,
                            null,
                            null,
                            null);

            return Future.succeededFuture(sps);
        });
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod runningBuild = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .build();

        Pod terminatedPod = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub()))
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
        when(mockPodOps.waitFor(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(runningBuild), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify PodSet
                List<StrimziPodSet> capturedSps = podSetCaptor.getAllValues();
                assertThat(capturedSps, hasSize(1));
                StrimziPodSet sps = capturedSps.get(0);
                assertThat(sps.getMetadata().getName(), is(connect.getComponentName()));
                assertThat(Annotations.stringAnnotation(sps, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                PodSetUtils.podSetToPods(sps).forEach(pod -> {
                    assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                    assertThat(Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                });

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(0));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(1));
                assertThat(capturedBuilderPods.stream().filter(Objects::nonNull).collect(Collectors.toList()), hasSize(0));

                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                async.flag();
            })));
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testRestartPreviousBuildOnKube(VertxTestContext context) {
        Plugin plugin1 = new PluginBuilder()
                .withName("plugin1")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                .build();

        Plugin plugin2 = new PluginBuilder()
                .withName("plugin2")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my2.jar").build())
                .build();

        KafkaConnect oldKc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers("my-cluster-kafka-bootstrap:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage(OUTPUT_IMAGE)
                            .withPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster oldConnect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);
        KafkaConnectBuild oldBuild = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);

        KafkaConnect kc = new KafkaConnectBuilder(oldKc)
                .editSpec()
                    .editBuild()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock Secret ops
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock StrimziPodSet ops
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenAnswer(i -> {
            StrimziPodSet sps = oldConnect
                    .generatePodSet(
                            3,
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, "oldhashstub", Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:olddigest"),
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, "oldhashstub", Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:olddigest"),
                            false,
                            null,
                            null,
                            null);

            return Future.succeededFuture(sps);
        });
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod runningBuild = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub()))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .build();

        Pod terminatedPod = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub()))
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

        when(mockPodOps.waitFor(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(runningBuild), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify PodSet
                List<StrimziPodSet> capturedSps = podSetCaptor.getAllValues();
                assertThat(capturedSps, hasSize(1));
                StrimziPodSet sps = capturedSps.get(0);
                assertThat(sps.getMetadata().getName(), is(connect.getComponentName()));
                assertThat(Annotations.stringAnnotation(sps, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                PodSetUtils.podSetToPods(sps).forEach(pod -> {
                    assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                    assertThat(Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                });

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(1));
                ConfigMap dockerfileCm = capturedCms.get(0);
                assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(3));
                assertThat(capturedBuilderPods.stream().filter(Objects::nonNull).collect(Collectors.toList()), hasSize(1));

                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                async.flag();
            })));
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testRestartPreviousBuildDueToFailureOnKube(VertxTestContext context) {
        Plugin plugin1 = new PluginBuilder()
                .withName("plugin1")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                .build();

        Plugin plugin2 = new PluginBuilder()
                .withName("plugin2")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my2.jar").build())
                .build();

        KafkaConnect oldKc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers("my-cluster-kafka-bootstrap:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage(OUTPUT_IMAGE)
                            .withPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster oldConnect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);
        KafkaConnectBuild oldBuild = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldKc, VERSIONS, sharedEnvironmentProvider);

        KafkaConnect kc = new KafkaConnectBuilder(oldKc)
                .editSpec()
                    .editBuild()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock Secret ops
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock StrimziPodSet ops
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenAnswer(i -> {
            StrimziPodSet sps = oldConnect
                    .generatePodSet(
                            3,
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, "oldhashstub", Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:olddigest"),
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, "oldhashstub", Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:olddigest"),
                            false,
                            null,
                            null,
                            null);

            return Future.succeededFuture(sps);
        });
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod runningBuild = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub()))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withContainerStatuses(new ContainerStatusBuilder().withNewState().withNewTerminated().withExitCode(1).endTerminated().endState().build())
                .endStatus()
                .build();

        Pod terminatedPod = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub()))
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
        when(mockPodOps.waitFor(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(runningBuild), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify PodSet
                List<StrimziPodSet> capturedSps = podSetCaptor.getAllValues();
                assertThat(capturedSps, hasSize(1));
                StrimziPodSet sps = capturedSps.get(0);
                assertThat(sps.getMetadata().getName(), is(connect.getComponentName()));
                assertThat(Annotations.stringAnnotation(sps, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                PodSetUtils.podSetToPods(sps).forEach(pod -> {
                    assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                    assertThat(Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                });

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(1));
                ConfigMap dockerfileCm = capturedCms.get(0);
                assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(3));
                assertThat(capturedBuilderPods.stream().filter(Objects::nonNull).collect(Collectors.toList()), hasSize(1));

                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                async.flag();
            })));
    }

    @Test
    public void testUpdateWithoutRebuildOnKube(VertxTestContext context) {
        Plugin plugin1 = new PluginBuilder()
                .withName("plugin1")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                .build();

        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers("my-cluster-kafka-bootstrap:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage(OUTPUT_IMAGE)
                            .withPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock Secret ops
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock StrimziPodSet ops
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenAnswer(i -> {
            StrimziPodSet sps = connect
                    .generatePodSet(
                            3,
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB, Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:blablabla"),
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB, Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:blablabla"),
                            false,
                            null,
                            null,
                            null);

            return Future.succeededFuture(sps);
        });
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify PodSet
                List<StrimziPodSet> capturedSps = podSetCaptor.getAllValues();
                assertThat(capturedSps, hasSize(1));
                StrimziPodSet sps = capturedSps.get(0);
                assertThat(sps.getMetadata().getName(), is(connect.getComponentName()));
                assertThat(Annotations.stringAnnotation(sps, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                PodSetUtils.podSetToPods(sps).forEach(pod -> {
                    assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                    assertThat(Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                });

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(0));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(0));

                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                async.flag();
            })));
    }

    @Test
    @SuppressWarnings({"checkstyle:MethodLength"})
    public void testUpdateWithForcedRebuildOnKube(VertxTestContext context) {
        Plugin plugin1 = new PluginBuilder()
                .withName("plugin1")
                .withArtifacts(new JarArtifactBuilder().withUrl("https://my-domain.tld/my.jar").build())
                .build();

        KafkaConnect kc = new KafkaConnectBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(1)
                    .withBootstrapServers("my-cluster-kafka-bootstrap:9092")
                    .withNewBuild()
                        .withNewDockerOutput()
                            .withImage(OUTPUT_IMAGE)
                            .withPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kc, VERSIONS, sharedEnvironmentProvider);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList> mockConnectOps = supplier.connectOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(LabelSelector.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock Secret ops
        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock StrimziPodSet ops
        when(mockPodSetOps.getAsync(eq(NAMESPACE), eq(COMPONENT_NAME))).thenAnswer(i -> {
            StrimziPodSet sps = connect
                    .generatePodSet(
                            3,
                            Map.of(Annotations.STRIMZI_IO_CONNECT_FORCE_REBUILD, "true", Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB, Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:blablabla"),
                            Map.of(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB, Annotations.STRIMZI_IO_CONNECT_BUILD_IMAGE, "my-connect-build@sha256:blablabla"),
                            false,
                            null,
                            null,
                            null);

            return Future.succeededFuture(sps);
        });
        ArgumentCaptor<StrimziPodSet> podSetCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), eq(COMPONENT_NAME), podSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));
        when(mockPodSetOps.readiness(any(), eq(NAMESPACE), eq(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockPodOps.getAsync(eq(NAMESPACE), startsWith(COMPONENT_NAME))).thenReturn(Future.succeededFuture());
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), any())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), eq(NAMESPACE), startsWith(COMPONENT_NAME), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod terminatedPod = new PodBuilder()
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
                                .withMessage("my-connect-build@sha256:rebuiltblablabla")
                            .endTerminated()
                        .endState()
                        .build())
                .endStatus()
                .build();
        when(mockPodOps.waitFor(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(any(), eq(NAMESPACE), eq(KafkaConnectResources.componentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(any(), connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify PodSet
                List<StrimziPodSet> capturedSps = podSetCaptor.getAllValues();
                assertThat(capturedSps, hasSize(1));
                StrimziPodSet sps = capturedSps.get(0);
                assertThat(sps.getMetadata().getName(), is(connect.getComponentName()));
                assertThat(Annotations.stringAnnotation(sps, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                PodSetUtils.podSetToPods(sps).forEach(pod -> {
                    assertThat(pod.getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:rebuiltblablabla"));
                    assertThat(Annotations.stringAnnotation(pod, Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub() + OUTPUT_IMAGE_HASH_STUB));
                });
                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(1));
                ConfigMap dockerfileCm = capturedCms.get(0);
                assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(3));
                assertThat(capturedBuilderPods.stream().filter(Objects::nonNull).collect(Collectors.toList()), hasSize(1));

                // Verify status
                List<KafkaConnect> capturedConnects = connectCaptor.getAllValues();
                assertThat(capturedConnects, hasSize(1));
                KafkaConnectStatus connectStatus = capturedConnects.get(0).getStatus();
                assertThat(connectStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(connectStatus.getConditions().get(0).getType(), is("Ready"));

                async.flag();
            })));
    }
}