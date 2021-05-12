/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaConnectBuild;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
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
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaConnectBuildAssemblyOperatorKubeTest {
    private static final String NAMESPACE = "my-ns";
    private static final String NAME = "my-connect";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    protected static Vertx vertx;
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_16;

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
                            .withImage("my-connect-build:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(kc, VERSIONS);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
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
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock KafkaConnectS2I ops
        when(mockConnectS2IOps.getAsync(NAMESPACE, NAME)).thenReturn(Future.succeededFuture(null));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(anyString(), anyString(), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(mockDepOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture SA ops
        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(anyString(), anyString(), saCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ServiceAccount())));

        // Mock and capture Pod ops
        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod terminatedPod = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withContainerStatuses(new ContainerStatusBuilder().withNewState().withNewTerminated().withExitCode(0).withMessage("my-connect-build@sha256:blablabla").endTerminated().endState().build())
                .endStatus()
                .build();
        when(mockPodOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(null), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify Deployment
                List<Deployment> capturedDeps = depCaptor.getAllValues();
                assertThat(capturedDeps, hasSize(1));
                Deployment dep = capturedDeps.get(0);
                assertThat(dep.getMetadata().getName(), is(connect.getName()));
                assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                assertThat(Annotations.stringAnnotation(dep.getSpec().getTemplate(), Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub()));

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
                assertThat(capturedBuilderPods.stream().filter(pod -> pod != null).collect(Collectors.toList()), hasSize(1));

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
                            .withImage("my-connect-build:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(kc, VERSIONS);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock KafkaConnectS2I ops
        when(mockConnectS2IOps.getAsync(NAMESPACE, NAME)).thenReturn(Future.succeededFuture(null));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(anyString(), anyString(), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(mockDepOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod terminatedPod = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withContainerStatuses(new ContainerStatusBuilder().withNewState().withNewTerminated().withExitCode(1).endTerminated().endState().build())
                .endStatus()
                .build();
        when(mockPodOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(null), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

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
                assertThat(capturedBuilderPods.stream().filter(pod -> pod != null).collect(Collectors.toList()), hasSize(1));

                async.flag();
            })));
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testUpdateWithRebuildOnKube(VertxTestContext context) {
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
                            .withImage("my-connect-build:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster oldConnect = KafkaConnectCluster.fromCrd(oldKc, VERSIONS);
        KafkaConnectBuild oldBuild = KafkaConnectBuild.fromCrd(oldKc, VERSIONS);

        KafkaConnect kc = new KafkaConnectBuilder(oldKc)
                .editSpec()
                    .editBuild()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(kc, VERSIONS);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock KafkaConnectS2I ops
        when(mockConnectS2IOps.getAsync(NAMESPACE, NAME)).thenReturn(Future.succeededFuture(null));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(anyString(), anyString(), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)))).thenAnswer(inv -> {
            Deployment dep = oldConnect.generateDeployment(emptyMap(), false, null, null);
            dep.getSpec().getTemplate().getMetadata().getAnnotations().put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub());
            dep.getSpec().getTemplate().getSpec().getContainers().get(0).setImage("my-connect-build@sha256:olddigest");
            return Future.succeededFuture(dep);
        });
        when(mockDepOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod terminatedPod = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withContainerStatuses(new ContainerStatusBuilder().withNewState().withNewTerminated().withExitCode(0).withMessage("my-connect-build@sha256:blablabla").endTerminated().endState().build())
                .endStatus()
                .build();
        when(mockPodOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(null), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify Deployment
                List<Deployment> capturedDeps = depCaptor.getAllValues();
                assertThat(capturedDeps, hasSize(1));
                Deployment dep = capturedDeps.get(0);
                assertThat(dep.getMetadata().getName(), is(connect.getName()));
                assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                assertThat(Annotations.stringAnnotation(dep.getSpec().getTemplate(), Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub()));

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(1));
                ConfigMap dockerfileCm = capturedCms.get(0);
                assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(2));
                assertThat(capturedBuilderPods.stream().filter(pod -> pod != null).collect(Collectors.toList()), hasSize(1));

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
                            .withImage("my-connect-build:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster oldConnect = KafkaConnectCluster.fromCrd(oldKc, VERSIONS);
        KafkaConnectBuild oldBuild = KafkaConnectBuild.fromCrd(oldKc, VERSIONS);

        KafkaConnect kc = new KafkaConnectBuilder(oldKc)
                .editSpec()
                    .editBuild()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(kc, VERSIONS);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock KafkaConnectS2I ops
        when(mockConnectS2IOps.getAsync(NAMESPACE, NAME)).thenReturn(Future.succeededFuture(null));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(anyString(), anyString(), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)))).thenAnswer(inv -> {
            Deployment dep = oldConnect.generateDeployment(emptyMap(), false, null, null);
            dep.getSpec().getTemplate().getMetadata().getAnnotations().put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, "oldhashstub");
            dep.getSpec().getTemplate().getSpec().getContainers().get(0).setImage("my-connect-build@sha256:olddigest");
            return Future.succeededFuture(dep);
        });
        when(mockDepOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

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
                    .withContainerStatuses(new ContainerStatusBuilder().withNewState().withNewTerminated().withExitCode(0).withMessage("my-connect-build@sha256:blablabla").endTerminated().endState().build())
                .endStatus()
                .build();
        when(mockPodOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(runningBuild), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify Deployment
                List<Deployment> capturedDeps = depCaptor.getAllValues();
                assertThat(capturedDeps, hasSize(1));
                Deployment dep = capturedDeps.get(0);
                assertThat(dep.getMetadata().getName(), is(connect.getName()));
                assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                assertThat(Annotations.stringAnnotation(dep.getSpec().getTemplate(), Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub()));

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(0));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(1));
                assertThat(capturedBuilderPods.stream().filter(pod -> pod != null).collect(Collectors.toList()), hasSize(0));

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
                            .withImage("my-connect-build:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster oldConnect = KafkaConnectCluster.fromCrd(oldKc, VERSIONS);
        KafkaConnectBuild oldBuild = KafkaConnectBuild.fromCrd(oldKc, VERSIONS);

        KafkaConnect kc = new KafkaConnectBuilder(oldKc)
                .editSpec()
                    .editBuild()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(kc, VERSIONS);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock KafkaConnectS2I ops
        when(mockConnectS2IOps.getAsync(NAMESPACE, NAME)).thenReturn(Future.succeededFuture(null));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(anyString(), anyString(), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)))).thenAnswer(inv -> {
            Deployment dep = oldConnect.generateDeployment(emptyMap(), false, null, null);
            dep.getSpec().getTemplate().getMetadata().getAnnotations().put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, "oldhashstub");
            dep.getSpec().getTemplate().getSpec().getContainers().get(0).setImage("my-connect-build@sha256:olddigest");
            return Future.succeededFuture(dep);
        });
        when(mockDepOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

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
                    .withContainerStatuses(new ContainerStatusBuilder().withNewState().withNewTerminated().withExitCode(0).withMessage("my-connect-build@sha256:blablabla").endTerminated().endState().build())
                .endStatus()
                .build();

        when(mockPodOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(runningBuild), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify Deployment
                List<Deployment> capturedDeps = depCaptor.getAllValues();
                assertThat(capturedDeps, hasSize(1));
                Deployment dep = capturedDeps.get(0);
                assertThat(dep.getMetadata().getName(), is(connect.getName()));
                assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                assertThat(Annotations.stringAnnotation(dep.getSpec().getTemplate(), Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub()));

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(1));
                ConfigMap dockerfileCm = capturedCms.get(0);
                assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(3));
                assertThat(capturedBuilderPods.stream().filter(pod -> pod != null).collect(Collectors.toList()), hasSize(1));

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
                            .withImage("my-connect-build:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster oldConnect = KafkaConnectCluster.fromCrd(oldKc, VERSIONS);
        KafkaConnectBuild oldBuild = KafkaConnectBuild.fromCrd(oldKc, VERSIONS);

        KafkaConnect kc = new KafkaConnectBuilder(oldKc)
                .editSpec()
                    .editBuild()
                        .withPlugins(plugin1, plugin2)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(kc, VERSIONS);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock KafkaConnectS2I ops
        when(mockConnectS2IOps.getAsync(NAMESPACE, NAME)).thenReturn(Future.succeededFuture(null));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(anyString(), anyString(), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)))).thenAnswer(inv -> {
            Deployment dep = oldConnect.generateDeployment(emptyMap(), false, null, null);
            dep.getSpec().getTemplate().getMetadata().getAnnotations().put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, "oldhashstub");
            dep.getSpec().getTemplate().getSpec().getContainers().get(0).setImage("my-connect-build@sha256:olddigest");
            return Future.succeededFuture(dep);
        });
        when(mockDepOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

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
                    .withContainerStatuses(new ContainerStatusBuilder().withNewState().withNewTerminated().withExitCode(0).withMessage("my-connect-build@sha256:blablabla").endTerminated().endState().build())
                .endStatus()
                .build();
        when(mockPodOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(runningBuild), Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify Deployment
                List<Deployment> capturedDeps = depCaptor.getAllValues();
                assertThat(capturedDeps, hasSize(1));
                Deployment dep = capturedDeps.get(0);
                assertThat(dep.getMetadata().getName(), is(connect.getName()));
                assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                assertThat(Annotations.stringAnnotation(dep.getSpec().getTemplate(), Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub()));

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(1));
                ConfigMap dockerfileCm = capturedCms.get(0);
                assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(3));
                assertThat(capturedBuilderPods.stream().filter(pod -> pod != null).collect(Collectors.toList()), hasSize(1));

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
                            .withImage("my-connect-build:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(kc, VERSIONS);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock KafkaConnectS2I ops
        when(mockConnectS2IOps.getAsync(NAMESPACE, NAME)).thenReturn(Future.succeededFuture(null));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(anyString(), anyString(), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)))).thenAnswer(inv -> {
            Deployment dep = connect.generateDeployment(emptyMap(), false, null, null);
            dep.getSpec().getTemplate().getMetadata().getAnnotations().put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub());
            dep.getSpec().getTemplate().getSpec().getContainers().get(0).setImage("my-connect-build@sha256:blablabla");
            return Future.succeededFuture(dep);
        });
        when(mockDepOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify Deployment
                List<Deployment> capturedDeps = depCaptor.getAllValues();
                assertThat(capturedDeps, hasSize(1));
                Deployment dep = capturedDeps.get(0);
                assertThat(dep.getMetadata().getName(), is(connect.getName()));
                assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:blablabla"));
                assertThat(Annotations.stringAnnotation(dep.getSpec().getTemplate(), Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub()));

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
                            .withImage("my-connect-build:latest")
                            .withNewPushSecret("my-docker-credentials")
                        .endDockerOutput()
                        .withPlugins(plugin1)
                    .endBuild()
                .endSpec()
                .build();

        KafkaConnectCluster connect = KafkaConnectCluster.fromCrd(kc, VERSIONS);
        KafkaConnectBuild build = KafkaConnectBuild.fromCrd(kc, VERSIONS);

        // Prepare and get mocks
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockConnectOps = supplier.connectOperator;
        CrdOperator mockConnectS2IOps = supplier.connectS2IOperator;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        PodOperator mockPodOps = supplier.podOperations;
        BuildConfigOperator mockBcOps = supplier.buildConfigOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        CrdOperator<KubernetesClient, KafkaConnector, KafkaConnectorList> mockConnectorOps = supplier.kafkaConnectorOperator;

        // Mock KafkaConnector ops
        when(mockConnectorOps.listAsync(anyString(), any(Optional.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock KafkaConnect ops
        when(mockConnectOps.get(NAMESPACE, NAME)).thenReturn(kc);
        when(mockConnectOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kc));

        // Mock KafkaConnectS2I ops
        when(mockConnectS2IOps.getAsync(NAMESPACE, NAME)).thenReturn(Future.succeededFuture(null));

        // Mock and capture service ops
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock and capture deployment ops
        ArgumentCaptor<Deployment> depCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDepOps.reconcile(anyString(), anyString(), depCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDepOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)))).thenAnswer(inv -> {
            Deployment dep = connect.generateDeployment(emptyMap(), false, null, null);
            dep.getSpec().getTemplate().getMetadata().getAnnotations().put(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub());
            dep.getMetadata().getAnnotations().put(Annotations.STRIMZI_IO_CONNECT_FORCE_REBUILD, "true");
            dep.getSpec().getTemplate().getSpec().getContainers().get(0).setImage("my-connect-build@sha256:blablabla");
            return Future.succeededFuture(dep);
        });
        when(mockDepOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture CM ops
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<ConfigMap> dockerfileCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(anyString(), eq(KafkaConnectResources.dockerFileConfigMapName(NAME)), dockerfileCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock and capture Pod ops
        ArgumentCaptor<Pod> builderPodCaptor = ArgumentCaptor.forClass(Pod.class);
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), builderPodCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Pod terminatedPod = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectResources.buildPodName(NAME))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withContainerStatuses(new ContainerStatusBuilder().withNewState().withNewTerminated().withExitCode(0).withMessage("my-connect-build@sha256:rebuiltblablabla").endTerminated().endState().build())
                .endStatus()
                .build();
        when(mockPodOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture((Void) null));
        when(mockPodOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)))).thenReturn(Future.succeededFuture(terminatedPod));

        // Mock and capture BuildConfig ops
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture NP ops
        when(mockNetPolOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.deploymentName(NAME)), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // Mock and capture PDB ops
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock and capture KafkaConnect ops for status update
        ArgumentCaptor<KafkaConnect> connectCaptor = ArgumentCaptor.forClass(KafkaConnect.class);
        when(mockConnectOps.updateStatusAsync(connectCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock KafkaConnect API client
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);

        // Prepare and run reconciliation
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify Deployment
                List<Deployment> capturedDeps = depCaptor.getAllValues();
                assertThat(capturedDeps, hasSize(1));
                Deployment dep = capturedDeps.get(0);
                assertThat(dep.getMetadata().getName(), is(connect.getName()));
                assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImage(), is("my-connect-build@sha256:rebuiltblablabla"));
                assertThat(Annotations.stringAnnotation(dep.getSpec().getTemplate(), Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, null), is(build.generateDockerfile().hashStub()));

                // Verify ConfigMap
                List<ConfigMap> capturedCms = dockerfileCaptor.getAllValues();
                assertThat(capturedCms, hasSize(1));
                ConfigMap dockerfileCm = capturedCms.get(0);
                assertThat(dockerfileCm.getData().containsKey("Dockerfile"), is(true));
                assertThat(dockerfileCm.getData().get("Dockerfile"), is(build.generateDockerfile().getDockerfile()));

                // Verify builder Pod
                List<Pod> capturedBuilderPods = builderPodCaptor.getAllValues();
                assertThat(capturedBuilderPods, hasSize(3));
                assertThat(capturedBuilderPods.stream().filter(pod -> pod != null).collect(Collectors.toList()), hasSize(1));

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