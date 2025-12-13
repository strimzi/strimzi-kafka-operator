/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaBridgeCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ClusterRoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.platform.KubernetesVersion;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaBridgeAssemblyOperatorTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private static final String NAME = "foo";
    private static final String NAMESPACE = "test";
    private static final String BOOTSTRAP_SERVERS = "foo-kafka:9092";
    private static final KafkaBridge BRIDGE = new KafkaBridgeBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .withLabels(Map.of(Labels.KUBERNETES_DOMAIN + "part-of", "tests", "my-user-label", "cromulent"))
            .endMetadata()
            .withNewSpec()
                .withImage("kafka-bridge:latest")
                .withReplicas(1)
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .withNewHttp()
                .endHttp()
                .withNewJmxPrometheusExporterMetricsConfig()
                    .withNewValueFrom()
                        .withNewConfigMapKeyRef("metrics.yaml", "my-metrics-config", false)
                    .endValueFrom()
                .endJmxPrometheusExporterMetricsConfig()
            .endSpec()
            .build();

    protected static Vertx vertx;

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateOrUpdateCreatesCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(BRIDGE));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        // Metrics configuration map
        when(mockCmOps.getAsync(any(), eq("my-metrics-config"))).thenReturn(Future.succeededFuture(new ConfigMapBuilder().withData(Map.of("metrics.yaml", "metrics-config")).build()));

        ArgumentCaptor<KafkaBridge> bridgeCaptor = ArgumentCaptor.forClass(KafkaBridge.class);
        when(mockBridgeOps.updateStatusAsync(any(), bridgeCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, BRIDGE, SHARED_ENV_PROVIDER);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(KafkaBridgeResources.serviceName(NAME)));
                assertThat(service, is(bridge.generateService()));

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(bridge.getComponentName()));
                assertThat(dc, is(bridge.generateDeployment(Map.of(
                        Annotations.ANNO_STRIMZI_AUTH_HASH, "0",
                        Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH, "c8cc2862"
                        ), true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb.size(), is(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(bridge.getComponentName()));
                assertThat(pdb, is(bridge.generatePodDisruptionBudget()));

                // Verify status
                List<KafkaBridge> capturedStatuses = bridgeCaptor.getAllValues();
                assertThat(capturedStatuses.get(0).getStatus().getUrl(), is("http://foo-bridge-service.test.svc:8080"));
                assertThat(capturedStatuses.get(0).getStatus().getReplicas(), is(bridge.getReplicas()));
                List<String> expectedParts = Arrays.asList(bridge.getSelectorLabels().toSelectorString().split(","));
                List<String> actualParts   = Arrays.asList(capturedStatuses.get(0).getStatus().getLabelSelector().split(","));
                assertThat(actualParts, containsInAnyOrder(expectedParts.toArray(new String[0])));
                assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getType(), is("Ready"));

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateWithNoDiffCausesNoChanges(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, BRIDGE, SHARED_ENV_PROVIDER);

        when(mockDcOps.get(NAMESPACE, bridge.getComponentName())).thenReturn(bridge.generateDeployment(Map.of(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(NAMESPACE), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(NAMESPACE), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(NAMESPACE), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(NAMESPACE, bridge.getComponentName())).thenReturn(bridge.generateService());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        // Metrics configuration map
        when(mockCmOps.getAsync(any(), eq("my-metrics-config"))).thenReturn(Future.succeededFuture(new ConfigMapBuilder().withData(Map.of("metrics.yaml", "metrics-config")).build()));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME), BRIDGE)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));

                // Verify Deployment Config
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(bridge.getComponentName()));
                assertThat(pdb, is(bridge.generatePodDisruptionBudget()));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateUpdatesCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        KafkaBridgeCluster oldBridgeCluster = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, BRIDGE, SHARED_ENV_PROVIDER);

        KafkaBridge modifiedBridge = new KafkaBridgeBuilder(BRIDGE)
                .editSpec()
                    .withImage("some/different:image")
                .endSpec()
                .build();

        when(mockBridgeOps.updateStatusAsync(any(), any(KafkaBridge.class))).thenReturn(Future.succeededFuture());

        when(mockDcOps.get(NAMESPACE, oldBridgeCluster.getComponentName())).thenReturn(oldBridgeCluster.generateDeployment(Map.of(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(NAMESPACE), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(NAMESPACE), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(NAMESPACE), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(NAMESPACE, oldBridgeCluster.getComponentName())).thenReturn(oldBridgeCluster.generateService());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        // Metrics configuration map
        when(mockCmOps.getAsync(any(), eq("my-metrics-config"))).thenReturn(Future.succeededFuture(new ConfigMapBuilder().withData(Map.of("metrics.yaml", "metrics-config")).build()));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME), modifiedBridge)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                KafkaBridgeCluster compareTo = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, modifiedBridge, SHARED_ENV_PROVIDER);

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(KafkaBridgeResources.serviceName(NAME)));
                assertThat(service, is(compareTo.generateService()));

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(compareTo.getComponentName()));
                assertThat(dc, is(compareTo.generateDeployment(Map.of(
                        Annotations.ANNO_STRIMZI_AUTH_HASH, "0",
                        Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH, "c8cc2862"
                ), true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(compareTo.getComponentName()));
                assertThat(pdb, is(compareTo.generatePodDisruptionBudget()));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                // No metrics config  => no CMs created
                verify(mockCmOps, never()).createOrUpdate(any(), any());
                async.flag();
            })));
    }

    @Test
    public void testDeleteClusterRoleBindings(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        ArgumentCaptor<ClusterRoleBinding> desiredCrb = ArgumentCaptor.forClass(ClusterRoleBinding.class);
        when(mockCrbOps.reconcile(any(), eq(KafkaBridgeResources.initContainerClusterRoleBindingName(NAME, NAMESPACE)), desiredCrb.capture())).thenReturn(Future.succeededFuture());

        CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> mockCntrOps = supplier.kafkaBridgeOperator;
        when(mockCntrOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        KafkaBridgeAssemblyOperator op = new KafkaBridgeAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                                                                         new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                                                                         supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.delete(reconciliation).onComplete(context.succeeding(c -> context.verify(() -> {
            assertThat(desiredCrb.getValue(), is(nullValue()));
            Mockito.verify(mockCrbOps, times(1)).reconcile(any(), any(), any());
            async.flag();
        })));
    }

    @Test
    public void testCreateOrUpdateThrowsWhenCreateServiceThrows(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        KafkaBridgeCluster oldBridgeCluster = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, BRIDGE, SHARED_ENV_PROVIDER);

        KafkaBridge modifiedBridge = new KafkaBridgeBuilder(BRIDGE)
                .editSpec()
                    .withImage("some/different:image")
                .endSpec()
                .build();

        when(mockDcOps.get(NAMESPACE, oldBridgeCluster.getComponentName())).thenReturn(oldBridgeCluster.generateDeployment(Map.of(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), dcNamespaceCaptor.capture(), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.failedFuture("Failed"));

        ArgumentCaptor<String> dcScaleUpNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), serviceNamespaceCaptor.capture(), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(NAMESPACE, oldBridgeCluster.getComponentName())).thenReturn(oldBridgeCluster.generateService());

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME), modifiedBridge)
            .onComplete(context.failing(e -> async.flag()));
    }

    @Test
    public void testCreateOrUpdateWithReplicasScaleUpToOne(VertxTestContext context) {
        int scaleTo = 1;
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        KafkaBridge oldBridge = new KafkaBridgeBuilder(BRIDGE)
                .editSpec()
                    .withReplicas(0)
                    .withMetricsConfig(null)
                .endSpec()
                .build();
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldBridge, SHARED_ENV_PROVIDER);

        KafkaBridge newBridge = new KafkaBridgeBuilder(BRIDGE)
                .editSpec()
                    .withReplicas(scaleTo)
                    .withMetricsConfig(null)
                .endSpec()
                .build();

        Deployment dep = bridge.generateDeployment(new HashMap<>(), true, null, null);
        when(mockDcOps.get(NAMESPACE, bridge.getComponentName())).thenReturn(dep);
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());
        doAnswer(i -> Future.succeededFuture(scaleTo)).when(mockDcOps).scaleUp(any(), eq(NAMESPACE), eq(bridge.getComponentName()), eq(scaleTo), anyLong());
        doAnswer(i -> Future.succeededFuture(scaleTo)).when(mockDcOps).scaleDown(any(), eq(NAMESPACE), eq(bridge.getComponentName()), eq(scaleTo), anyLong());

        when(mockServiceOps.get(NAMESPACE, bridge.getComponentName())).thenReturn(bridge.generateService());
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME), newBridge)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(NAMESPACE), eq(bridge.getComponentName()), eq(scaleTo), anyLong());

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateWithScaleDown(VertxTestContext context) {
        int scaleTo = 1;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        KafkaBridge oldBridge = new KafkaBridgeBuilder(BRIDGE)
                .editSpec()
                    .withReplicas(3)
                    .withMetricsConfig(null)
                .endSpec()
                .build();
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, oldBridge, SHARED_ENV_PROVIDER);

        KafkaBridge scaledDownCluster = new KafkaBridgeBuilder(BRIDGE)
                .editSpec()
                    .withReplicas(scaleTo)
                    .withMetricsConfig(null)
                .endSpec()
                .build();

        Deployment dep = bridge.generateDeployment(new HashMap<>(), true, null, null);
        when(mockDcOps.get(NAMESPACE, bridge.getComponentName())).thenReturn(dep);
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());
        doAnswer(i -> Future.succeededFuture(scaleTo)).when(mockDcOps).scaleUp(any(), eq(NAMESPACE), eq(bridge.getComponentName()), eq(scaleTo), anyLong());
        doAnswer(i -> Future.succeededFuture(scaleTo)).when(mockDcOps).scaleDown(any(), eq(NAMESPACE), eq(bridge.getComponentName()), eq(scaleTo), anyLong());

        when(mockServiceOps.get(NAMESPACE, bridge.getComponentName())).thenReturn(bridge.generateService());
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME), scaledDownCluster)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(NAMESPACE), eq(bridge.getComponentName()), eq(scaleTo), anyLong());
                verify(mockDcOps).scaleDown(any(), eq(NAMESPACE), eq(bridge.getComponentName()), eq(scaleTo), anyLong());
                async.flag();
            })));
    }

    @Test
    public void testReconcileCallsCreateOrUpdate(VertxTestContext context) {
        // Must create all checkpoints before flagging any to avoid premature test success
        Checkpoint async = context.checkpoint();

        // Should be called twice, once for foo and once for bar
        Checkpoint asyncCreatedOrUpdated = context.checkpoint(2);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

        KafkaBridge foo = new KafkaBridgeBuilder(BRIDGE)
                .editMetadata()
                    .withName("foo")
                .endMetadata()
                .build();
        KafkaBridge bar = new KafkaBridgeBuilder(BRIDGE)
                .editMetadata()
                    .withName("bar")
                .endMetadata()
                .build();

        when(mockBridgeOps.listAsync(eq(NAMESPACE), isNull(LabelSelector.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        when(mockBridgeOps.getAsync(eq(NAMESPACE), eq("foo"))).thenReturn(Future.succeededFuture(foo));
        when(mockBridgeOps.getAsync(eq(NAMESPACE), eq("bar"))).thenReturn(Future.succeededFuture(bar));

        // providing the list of ALL Deployments for all the Kafka Bridge clusters
        Labels newLabels = Labels.forStrimziKind(KafkaBridge.RESOURCE_KIND);
        when(mockDcOps.list(eq(NAMESPACE), eq(newLabels))).thenReturn(
                List.of(KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar, SHARED_ENV_PROVIDER).generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // providing the list Deployments for already "existing" Kafka Bridge clusters
        Labels barLabels = Labels.forStrimziCluster("bar");
        when(mockDcOps.list(eq(NAMESPACE), eq(barLabels))).thenReturn(
                List.of(KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar, SHARED_ENV_PROVIDER).generateDeployment(Map.of(), true, null, null))
        );

        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<KafkaBridgeStatus> createOrUpdate(Reconciliation reconciliation, KafkaBridge kafkaBridgeAssembly) {
                createdOrUpdated.add(kafkaBridgeAssembly.getMetadata().getName());
                asyncCreatedOrUpdated.flag();
                return Future.succeededFuture();
            }
        };

        Promise<Void> reconciled = Promise.promise();
        // Now try to reconcile all the Kafka Bridge clusters
        ops.reconcileAll("test", NAMESPACE, v -> reconciled.complete());

        reconciled.future().onComplete(context.succeeding(v -> context.verify(() -> {
            assertThat(createdOrUpdated, is(Set.of("foo", "bar")));
            async.flag();
        })));
    }

    @Test
    public void testCreateClusterStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(BRIDGE));
        ArgumentCaptor<KafkaBridge> bridgeCaptor = ArgumentCaptor.forClass(KafkaBridge.class);
        when(mockBridgeOps.updateStatusAsync(any(), bridgeCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.failedFuture("failure"));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME))
            .onComplete(context.failing(e -> context.verify(() -> {
                // Verify status
                List<KafkaBridge> capturedStatuses = bridgeCaptor.getAllValues();
                assertThat(capturedStatuses.get(0).getStatus().getUrl(), is("http://foo-bridge-service.test.svc:8080"));
                assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getType(), is("NotReady"));

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateBridgeZeroReplica(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        KafkaBridge kb = new KafkaBridgeBuilder(BRIDGE)
                .editSpec()
                    .withReplicas(0)
                .endSpec()
                .build();

        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        ArgumentCaptor<KafkaBridge> bridgeCaptor = ArgumentCaptor.forClass(KafkaBridge.class);
        when(mockBridgeOps.updateStatusAsync(any(), bridgeCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        // Metrics configuration map
        when(mockCmOps.getAsync(any(), eq("my-metrics-config"))).thenReturn(Future.succeededFuture(new ConfigMapBuilder().withData(Map.of("metrics.yaml", "metrics-config")).build()));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // 0 Replicas - readiness should never get called.
                    verify(mockDcOps, never()).readiness(any(), anyString(), anyString(), anyLong(), anyLong());
                    assertNull(bridgeCaptor.getValue().getStatus().getUrl());

                    async.flag();
                })));
    }

    @Test
    public void testUpdatedBridgeConfigurationHash(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;

        KafkaBridgeCluster originalBridgeCluster = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, BRIDGE, SHARED_ENV_PROVIDER);

        KafkaBridge updatedBridge = new KafkaBridgeBuilder(BRIDGE)
                .editSpec()
                    .withNewProducer()
                        .withConfig(Map.of("acks", "1"))
                    .endProducer()
                    .editConsumer()
                        .withConfig(Map.of("auto.offset.reset", "earliest"))
                    .endConsumer()
                .endSpec()
                .build();

        when(mockDcOps.scaleDown(any(), eq(NAMESPACE), any(), anyInt(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), eq(NAMESPACE), any(), anyInt(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> cmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), any(), cmCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockCmOps.getAsync(any(), eq("my-metrics-config"))).thenReturn(Future.succeededFuture(new ConfigMapBuilder().withData(Map.of("metrics.yaml", "metrics-config")).build()));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        MetricsAndLoggingUtils.metricsAndLogging(reconciliation, mockCmOps, originalBridgeCluster.logging(), originalBridgeCluster.metrics())
                .compose(metricsAndLogging -> {
                    ConfigMap originalConfigMap = originalBridgeCluster.generateBridgeConfigMap(metricsAndLogging);
                    String originalHash = Util.hashStub(originalConfigMap.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME));
                    return ops.createOrUpdate(reconciliation, updatedBridge)
                            .onComplete(context.succeeding(v -> context.verify(() -> {
                                // getting the updated ConfigMap and check its hash is different from the original one
                                List<ConfigMap> captureCm = cmCaptor.getAllValues();
                                assertThat(captureCm, hasSize(1));
                                ConfigMap cm = captureCm.get(0);
                                String newHash = Util.hashStub(cm.getData().get(KafkaBridgeCluster.BRIDGE_CONFIGURATION_FILENAME));
                                assertThat(newHash, is(not(originalHash)));

                                async.flag();
                            })));
                });
    }

    @Test
    public void testTlsSecretsFetchedOnlyOncePerReference(VertxTestContext context) {
        String kbName = "foo";
        String kbNamespace = "test";

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

        KafkaBridge bridgeWithTls = new KafkaBridgeBuilder()
                .withNewMetadata()
                    .withName(kbName)
                    .withNamespace(kbNamespace)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers(BOOTSTRAP_SERVERS)
                    .withReplicas(1)
                    .withNewHttp(8080)
                    .withNewTls()
                        .withTrustedCertificates(
                                new CertSecretSourceBuilder()
                                        .withSecretName("shared-tls-secret")
                                        .withCertificate("ca.crt")
                                        .build(),
                                new CertSecretSourceBuilder()
                                        .withSecretName("shared-tls-secret")
                                        .withCertificate("ca2.crt")
                                        .build()
                        )
                    .endTls()
                .endSpec()
                .build();

        when(mockBridgeOps.get(eq(kbNamespace), eq(kbName))).thenReturn(bridgeWithTls);
        when(mockBridgeOps.getAsync(eq(kbNamespace), eq(kbName))).thenReturn(Future.succeededFuture(bridgeWithTls));
        when(mockBridgeOps.updateStatusAsync(any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.scaleDown(any(), eq(kbNamespace), any(), anyInt(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), eq(kbNamespace), any(), anyInt(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        // Mock Secrets - the shared TLS secret referenced multiple times
        Secret tlsSecret = new SecretBuilder()
                .withNewMetadata().withName("shared-tls-secret").endMetadata()
                .withData(Map.of(
                        "ca.crt", Util.encodeToBase64("-----BEGIN CERTIFICATE-----\ntest-cert\n-----END CERTIFICATE-----"),
                        "ca2.crt", Util.encodeToBase64("-----BEGIN CERTIFICATE-----\ntest-cert-2\n-----END CERTIFICATE-----")))
                .build();
        when(mockSecretOps.getAsync(eq(kbNamespace), eq("shared-tls-secret"))).thenReturn(Future.succeededFuture(tlsSecret));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName);

        Checkpoint async = context.checkpoint();
        ops.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockSecretOps, times(2)).getAsync(eq(kbNamespace), eq("shared-tls-secret"));

                    async.flag();
                })));
    }
}