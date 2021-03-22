/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildBuilder;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildRequest;
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
import io.strimzi.operator.common.operator.resource.BuildOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
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
public class KafkaConnectBuildAssemblyOperatorOpenShiftTest {
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

    @Test
    public void testBuildOnOpenShift(VertxTestContext context) {
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
        BuildOperator mockBuildOps = supplier.buildOperations;
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

        // Mock and capture Pod ops
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture BuildConfig ops
        ArgumentCaptor<BuildConfig> buildConfigCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildConfigCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockBcOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)))).thenReturn(Future.succeededFuture(null));

        Build builder = new BuildBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName("build-1")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Complete")
                    .withNewOutputDockerImageReference("my-connect-build:latest")
                    .withNewOutput()
                        .withNewTo()
                            .withImageDigest("sha256:blablabla")
                        .endTo()
                    .endOutput()
                .endStatus()
                .build();

        ArgumentCaptor<BuildRequest> buildRequestCaptor = ArgumentCaptor.forClass(BuildRequest.class);
        when(mockBcOps.startBuild(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildRequestCaptor.capture())).thenReturn(Future.succeededFuture(builder));

        // Mock and capture Build ops
        when(mockBuildOps.waitFor(eq(NAMESPACE), eq("build-1"), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(mockBuildOps.getAsync(eq(NAMESPACE), eq("build-1"))).thenReturn(Future.succeededFuture(builder));

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
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
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

                // Verify BuildConfig
                List<BuildConfig> capturedBcs = buildConfigCaptor.getAllValues();
                assertThat(capturedBcs, hasSize(1));
                BuildConfig buildConfig = capturedBcs.get(0);
                assertThat(buildConfig.getSpec().getSource().getDockerfile(), is(build.generateDockerfile().getDockerfile()));

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
    public void testBuildFailureOnOpenShift(VertxTestContext context) {
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
        BuildOperator mockBuildOps = supplier.buildOperations;
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

        // Mock and capture Pod ops
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture BuildConfig ops
        ArgumentCaptor<BuildConfig> buildConfigCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildConfigCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        Build builder = new BuildBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName("build-1")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Failed")
                .endStatus()
                .build();

        ArgumentCaptor<BuildRequest> buildRequestCaptor = ArgumentCaptor.forClass(BuildRequest.class);
        when(mockBcOps.startBuild(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildRequestCaptor.capture())).thenReturn(Future.succeededFuture(builder));

        // Mock and capture Build ops
        when(mockBuildOps.waitFor(eq(NAMESPACE), eq("build-1"), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(mockBuildOps.getAsync(eq(NAMESPACE), eq("build-1"))).thenReturn(Future.succeededFuture(builder));

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
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaConnect.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.failing(v -> context.verify(() -> {
                async.flag();
            })));
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @Test
    public void testUpdateWithRebuildOnOpenShift(VertxTestContext context) {
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
        BuildOperator mockBuildOps = supplier.buildOperations;
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

        // Mock and capture Pod ops
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture BuildConfig ops
        ArgumentCaptor<BuildConfig> buildConfigCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildConfigCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockBcOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)))).thenReturn(Future.succeededFuture(null));

        Build builder = new BuildBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName("build-1")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Complete")
                    .withNewOutputDockerImageReference("my-connect-build:latest")
                    .withNewOutput()
                        .withNewTo()
                            .withImageDigest("sha256:blablabla")
                        .endTo()
                    .endOutput()
                .endStatus()
                .build();

        ArgumentCaptor<BuildRequest> buildRequestCaptor = ArgumentCaptor.forClass(BuildRequest.class);
        when(mockBcOps.startBuild(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildRequestCaptor.capture())).thenReturn(Future.succeededFuture(builder));

        // Mock and capture Build ops
        when(mockBuildOps.waitFor(eq(NAMESPACE), eq("build-1"), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(mockBuildOps.getAsync(eq(NAMESPACE), eq("build-1"))).thenReturn(Future.succeededFuture(builder));

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
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
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

                // Verify BuildConfig
                List<BuildConfig> capturedBcs = buildConfigCaptor.getAllValues();
                assertThat(capturedBcs, hasSize(1));
                BuildConfig buildConfig = capturedBcs.get(0);
                assertThat(buildConfig.getSpec().getSource().getDockerfile(), is(build.generateDockerfile().getDockerfile()));

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
    public void testUpdateWithoutRebuildOnOpenShift(VertxTestContext context) {
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
        BuildOperator mockBuildOps = supplier.buildOperations;
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

        // Mock and capture Pod ops
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture BuildConfig ops
        ArgumentCaptor<BuildConfig> buildConfigCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildConfigCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

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
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
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

                // Verify BuildConfig
                List<BuildConfig> capturedBcs = buildConfigCaptor.getAllValues();
                assertThat(capturedBcs, hasSize(0));

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
    public void testUpdateWithForcedRebuildOnOpenShift(VertxTestContext context) {
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
        BuildOperator mockBuildOps = supplier.buildOperations;
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

        // Mock and capture Pod ops
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture BuildConfig ops
        ArgumentCaptor<BuildConfig> buildConfigCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildConfigCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));
        when(mockBcOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)))).thenReturn(Future.succeededFuture(null));

        Build builder = new BuildBuilder()
                .withNewMetadata().withNamespace(NAMESPACE).withName("build-1")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Complete")
                    .withNewOutputDockerImageReference("my-connect-build:latest")
                    .withNewOutput()
                        .withNewTo()
                            .withImageDigest("sha256:rebuiltblablabla")
                        .endTo()
                    .endOutput()
                .endStatus()
                .build();

        ArgumentCaptor<BuildRequest> buildRequestCaptor = ArgumentCaptor.forClass(BuildRequest.class);
        when(mockBcOps.startBuild(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildRequestCaptor.capture())).thenReturn(Future.succeededFuture(builder));

        // Mock and capture Build ops
        when(mockBuildOps.waitFor(eq(NAMESPACE), eq("build-1"), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(mockBuildOps.getAsync(eq(NAMESPACE), eq("build-1"))).thenReturn(Future.succeededFuture(builder));

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
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
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

                // Verify BuildConfig
                List<BuildConfig> capturedBcs = buildConfigCaptor.getAllValues();
                assertThat(capturedBcs, hasSize(1));
                BuildConfig buildConfig = capturedBcs.get(0);
                assertThat(buildConfig.getSpec().getSource().getDockerfile(), is(build.generateDockerfile().getDockerfile()));

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
    public void testContinueWithPreviousBuildOnOpenShift(VertxTestContext context) {
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
        BuildOperator mockBuildOps = supplier.buildOperations;
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

        // Mock and capture Pod ops
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture Build ops
        Build oldBuilder = new BuildBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(KafkaConnectResources.buildName(NAME, 1L))
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub()))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Running")
                .endStatus()
                .build();

        Build newBuilder = new BuildBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(KafkaConnectResources.buildName(NAME, 1L))
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub()))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Complete")
                    .withNewOutputDockerImageReference("my-connect-build:latest")
                    .withNewOutput()
                        .withNewTo()
                            .withImageDigest("sha256:blablabla")
                        .endTo()
                    .endOutput()
                .endStatus()
                .build();

        when(mockBuildOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildName(NAME, 1L)))).thenReturn(Future.succeededFuture(oldBuilder));
        when(mockBuildOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildName(NAME, 1L)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(mockBuildOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildName(NAME, 1L)))).thenReturn(Future.succeededFuture(newBuilder));

        // Mock and capture BuildConfig ops
        ArgumentCaptor<BuildConfig> buildConfigCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildConfigCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        BuildConfig oldBuildConfig = new BuildConfigBuilder(oldBuild.generateBuildConfig(oldBuild.generateDockerfile()))
                .withNewStatus()
                    .withLastVersion(1L)
                .endStatus()
                .build();

        when(mockBcOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)))).thenReturn(Future.succeededFuture(oldBuildConfig));

        ArgumentCaptor<BuildRequest> buildRequestCaptor = ArgumentCaptor.forClass(BuildRequest.class);
        when(mockBcOps.startBuild(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildRequestCaptor.capture())).thenReturn(Future.succeededFuture(newBuilder));


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
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
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

                // Verify BuildConfig
                List<BuildConfig> capturedBcs = buildConfigCaptor.getAllValues();
                assertThat(capturedBcs, hasSize(0));

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
    public void testRestartPreviousBuildOnOpenShift(VertxTestContext context) {
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
        BuildOperator mockBuildOps = supplier.buildOperations;
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

        // Mock and capture Pod ops
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture Build ops
        Build oldBuilder = new BuildBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(KafkaConnectResources.buildName(NAME, 1L))
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, oldBuild.generateDockerfile().hashStub()))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Running")
                .endStatus()
                .build();

        Build newBuilder = new BuildBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(KafkaConnectResources.buildName(NAME, 2L))
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub()))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Complete")
                    .withNewOutputDockerImageReference("my-connect-build:latest")
                    .withNewOutput()
                        .withNewTo()
                            .withImageDigest("sha256:blablabla")
                        .endTo()
                    .endOutput()
                .endStatus()
                .build();

        when(mockBuildOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildName(NAME, 1L)))).thenReturn(Future.succeededFuture(oldBuilder));
        when(mockBuildOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildName(NAME, 2L)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(mockBuildOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildName(NAME, 2L)))).thenReturn(Future.succeededFuture(newBuilder));

        // Mock and capture BuildConfig ops
        ArgumentCaptor<BuildConfig> buildConfigCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildConfigCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        BuildConfig oldBuildConfig = new BuildConfigBuilder(oldBuild.generateBuildConfig(oldBuild.generateDockerfile()))
                .withNewStatus()
                    .withLastVersion(1L)
                .endStatus()
                .build();

        when(mockBcOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)))).thenReturn(Future.succeededFuture(oldBuildConfig));

        ArgumentCaptor<BuildRequest> buildRequestCaptor = ArgumentCaptor.forClass(BuildRequest.class);
        when(mockBcOps.startBuild(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildRequestCaptor.capture())).thenReturn(Future.succeededFuture(newBuilder));


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
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
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

                // Verify BuildConfig
                List<BuildConfig> capturedBcs = buildConfigCaptor.getAllValues();
                assertThat(capturedBcs, hasSize(1));
                BuildConfig buildConfig = capturedBcs.get(0);
                assertThat(buildConfig.getSpec().getSource().getDockerfile(), is(build.generateDockerfile().getDockerfile()));

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
    public void testRestartPreviousBuildDueToFailureOnOpenShift(VertxTestContext context) {
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
        BuildOperator mockBuildOps = supplier.buildOperations;
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

        // Mock and capture Pod ops
        when(mockPodOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildPodName(NAME)), eq(null))).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        // Mock and capture Build ops
        Build oldBuilder = new BuildBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(KafkaConnectResources.buildName(NAME, 1L))
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub()))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Failed")
                .endStatus()
                .build();

        Build newBuilder = new BuildBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(KafkaConnectResources.buildName(NAME, 2L))
                    .withAnnotations(singletonMap(Annotations.STRIMZI_IO_CONNECT_BUILD_REVISION, build.generateDockerfile().hashStub()))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .withPhase("Complete")
                    .withNewOutputDockerImageReference("my-connect-build:latest")
                    .withNewOutput()
                        .withNewTo()
                            .withImageDigest("sha256:blablabla")
                        .endTo()
                    .endOutput()
                .endStatus()
                .build();

        when(mockBuildOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildName(NAME, 1L)))).thenReturn(Future.succeededFuture(oldBuilder));
        when(mockBuildOps.waitFor(eq(NAMESPACE), eq(KafkaConnectResources.buildName(NAME, 2L)), anyString(), anyLong(), anyLong(), any(BiPredicate.class))).thenReturn(Future.succeededFuture());
        when(mockBuildOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildName(NAME, 2L)))).thenReturn(Future.succeededFuture(newBuilder));

        // Mock and capture BuildConfig ops
        ArgumentCaptor<BuildConfig> buildConfigCaptor = ArgumentCaptor.forClass(BuildConfig.class);
        when(mockBcOps.reconcile(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildConfigCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop(null)));

        BuildConfig oldBuildConfig = new BuildConfigBuilder(oldBuild.generateBuildConfig(oldBuild.generateDockerfile()))
                .withNewStatus()
                    .withLastVersion(1L)
                .endStatus()
                .build();

        when(mockBcOps.getAsync(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)))).thenReturn(Future.succeededFuture(oldBuildConfig));

        ArgumentCaptor<BuildRequest> buildRequestCaptor = ArgumentCaptor.forClass(BuildRequest.class);
        when(mockBcOps.startBuild(eq(NAMESPACE), eq(KafkaConnectResources.buildConfigName(NAME)), buildRequestCaptor.capture())).thenReturn(Future.succeededFuture(newBuilder));


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
        KafkaConnectAssemblyOperator ops = new KafkaConnectAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
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

                // Verify BuildConfig
                List<BuildConfig> capturedBcs = buildConfigCaptor.getAllValues();
                assertThat(capturedBcs, hasSize(1));
                BuildConfig buildConfig = capturedBcs.get(0);
                assertThat(buildConfig.getSpec().getSource().getDockerfile(), is(build.generateDockerfile().getDockerfile()));

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