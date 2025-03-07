/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeConsumerSpecBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeProducerSpecBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
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

    protected static Vertx vertx;

    private static final String BOOTSTRAP_SERVERS = "foo-kafka:9092";
    private static final KafkaBridgeConsumerSpec KAFKA_BRIDGE_CONSUMER_SPEC = new KafkaBridgeConsumerSpec();
    private static final KafkaBridgeProducerSpec KAFKA_BRIDGE_PRODUCER_SPEC = new KafkaBridgeProducerSpec();
    private static final KafkaBridgeHttpConfig KAFKA_BRIDGE_HTTP_SPEC = new KafkaBridgeHttpConfig();
    private final String image = "kafka-bridge:latest";

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
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.get(anyString(), anyString())).thenReturn(kb);

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

        ArgumentCaptor<KafkaBridge> bridgeCaptor = ArgumentCaptor.forClass(KafkaBridge.class);
        when(mockBridgeOps.updateStatusAsync(any(), bridgeCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kb, SHARED_ENV_PROVIDER);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(KafkaBridgeResources.serviceName(kbName)));
                assertThat(service, is(bridge.generateService()));

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(bridge.getComponentName()));
                assertThat(dc, is(bridge.generateDeployment(Map.of(
                        Annotations.ANNO_STRIMZI_AUTH_HASH, "0",
                        Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH, "fe8e7089"
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
                assertThat(capturedStatuses.get(0).getStatus().getLabelSelector(), is(bridge.getSelectorLabels().toSelectorString()));
                assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getType(), is("Ready"));

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateWithNoDiffCausesNoChanges(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kb, SHARED_ENV_PROVIDER);
        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(), any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getComponentName())).thenReturn(bridge.generateService());
        when(mockDcOps.get(kbNamespace, bridge.getComponentName())).thenReturn(bridge.generateDeployment(Map.of(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(kbNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(kbNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(kbNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(kbNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), kb)
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
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kb, SHARED_ENV_PROVIDER);
        kb.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(), any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getComponentName())).thenReturn(bridge.generateService());
        when(mockDcOps.get(kbNamespace, bridge.getComponentName())).thenReturn(bridge.generateDeployment(Map.of(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(kbNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(kbNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(kbNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(kbNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), kb)
            .onComplete(context.succeeding(v -> context.verify(() -> {

                KafkaBridgeCluster compareTo = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kb, SHARED_ENV_PROVIDER);

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(KafkaBridgeResources.serviceName(kbName)));
                assertThat(service, is(compareTo.generateService()));

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(compareTo.getComponentName()));
                assertThat(dc, is(compareTo.generateDeployment(Map.of(
                        Annotations.ANNO_STRIMZI_AUTH_HASH, "0",
                        Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH, "fe8e7089"
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
        String bridgeName = "foo";
        String bridgeNamespace = "test";

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        ArgumentCaptor<ClusterRoleBinding> desiredCrb = ArgumentCaptor.forClass(ClusterRoleBinding.class);
        when(mockCrbOps.reconcile(any(), eq(KafkaBridgeResources.initContainerClusterRoleBindingName(bridgeName, bridgeNamespace)), desiredCrb.capture())).thenReturn(Future.succeededFuture());

        CrdOperator<KubernetesClient, KafkaBridge, KafkaBridgeList> mockCntrOps = supplier.kafkaBridgeOperator;
        when(mockCntrOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        KafkaBridgeAssemblyOperator op = new KafkaBridgeAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                                                                         new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                                                                         supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));
        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, bridgeNamespace, bridgeName);

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
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kb, SHARED_ENV_PROVIDER);
        kb.getSpec().setImage("some/different:image"); // Change the image to generate some differences

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(), any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getComponentName())).thenReturn(bridge.generateService());
        when(mockDcOps.get(kbNamespace, bridge.getComponentName())).thenReturn(bridge.generateDeployment(Map.of(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

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
        when(mockDcOps.scaleUp(any(), dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockBridgeOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaBridge())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), kb)
            .onComplete(context.failing(e -> async.flag()));
    }

    @Test
    public void testCreateOrUpdateWithReplicasScaleUpToOne(VertxTestContext context) {
        final int scaleTo = 1;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        KafkaBridge kb = ResourceUtils.createEmptyKafkaBridge(kbNamespace, kbName);
        kb.getSpec().setReplicas(0);
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kb, SHARED_ENV_PROVIDER);
        kb.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(), any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getComponentName())).thenReturn(bridge.generateService());
        Deployment dep = bridge.generateDeployment(new HashMap<>(), true, null, null);
        when(mockDcOps.get(kbNamespace, bridge.getComponentName())).thenReturn(dep);
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(any(), eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(any(), eq(kbNamespace), eq(bridge.getComponentName()), eq(scaleTo), anyLong());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(any(), eq(kbNamespace), eq(bridge.getComponentName()), eq(scaleTo), anyLong());

        when(mockBridgeOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaBridge())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), kb)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(kbNamespace), eq(bridge.getComponentName()), eq(scaleTo), anyLong());

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateWithScaleDown(VertxTestContext context) {
        int scaleTo = 1;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        KafkaBridge kb = ResourceUtils.createEmptyKafkaBridge(kbNamespace, kbName);
        kb.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        // Change replicas to create ScaleDown
        KafkaBridge scaledDownCluster = new KafkaBridgeBuilder(kb)
                .editOrNewSpec()
                    .withReplicas(scaleTo)
                .endSpec()
                .build();
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kb, SHARED_ENV_PROVIDER);

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(), any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getComponentName())).thenReturn(bridge.generateService());
        Deployment dep = bridge.generateDeployment(new HashMap<>(), true, null, null);
        when(mockDcOps.get(kbNamespace, bridge.getComponentName())).thenReturn(dep);
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(any(), eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(any(), eq(kbNamespace), eq(bridge.getComponentName()), eq(scaleTo), anyLong());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(any(), eq(kbNamespace), eq(bridge.getComponentName()), eq(scaleTo), anyLong());

        when(mockBridgeOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaBridge())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), scaledDownCluster)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(kbNamespace), eq(bridge.getComponentName()), eq(scaleTo), anyLong());
                verify(mockDcOps).scaleDown(any(), eq(kbNamespace), eq(bridge.getComponentName()), eq(scaleTo), anyLong());
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
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kbNamespace = "test";

        KafkaBridge foo = ResourceUtils.createKafkaBridge(kbNamespace, "foo", image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);
        KafkaBridge bar = ResourceUtils.createKafkaBridge(kbNamespace, "bar", image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        when(mockBridgeOps.listAsync(eq(kbNamespace), isNull(LabelSelector.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(bar));
        when(mockBridgeOps.updateStatusAsync(any(), any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        // when requested ConfigMap for a specific Kafka Bridge cluster
        when(mockBridgeOps.getAsync(eq(kbNamespace), eq("foo"))).thenReturn(Future.succeededFuture(foo));
        when(mockBridgeOps.getAsync(eq(kbNamespace), eq("bar"))).thenReturn(Future.succeededFuture(bar));

        // providing the list of ALL Deployments for all the Kafka Bridge clusters
        Labels newLabels = Labels.forStrimziKind(KafkaBridge.RESOURCE_KIND);
        when(mockDcOps.list(eq(kbNamespace), eq(newLabels))).thenReturn(
                List.of(KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar, SHARED_ENV_PROVIDER).generateDeployment(Map.of(), true, null, null)));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // providing the list Deployments for already "existing" Kafka Bridge clusters
        Labels barLabels = Labels.forStrimziCluster("bar");
        when(mockDcOps.list(eq(kbNamespace), eq(barLabels))).thenReturn(
                List.of(KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar, SHARED_ENV_PROVIDER).generateDeployment(Map.of(), true, null, null))
        );

        when(mockSecretOps.reconcile(any(), eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

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
        ops.reconcileAll("test", kbNamespace, v -> reconciled.complete());

        reconciled.future().onComplete(context.succeeding(v -> context.verify(() -> {
            assertThat(createdOrUpdated, is(Set.of("foo", "bar")));
            async.flag();
        })));


    }

    @Test
    public void testCreateClusterStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";
        String failureMsg = "failure";

        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.get(anyString(), anyString())).thenReturn(kb);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<KafkaBridge> bridgeCaptor = ArgumentCaptor.forClass(KafkaBridge.class);
        when(mockBridgeOps.updateStatusAsync(any(), bridgeCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName))
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
        var mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 0,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.get(anyString(), anyString())).thenReturn(kb);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<KafkaBridge> bridgeCaptor = ArgumentCaptor.forClass(KafkaBridge.class);
        when(mockBridgeOps.updateStatusAsync(any(), bridgeCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName))
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

        String kbName = "foo";
        String kbNamespace = "test";

        KafkaBridge originalBridge = ResourceUtils.createEmptyKafkaBridge(kbNamespace, kbName);
        KafkaBridgeCluster originalBridgeCluster = KafkaBridgeCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, originalBridge, SHARED_ENV_PROVIDER);
        KafkaBridge updatedBridge = new KafkaBridgeBuilder(originalBridge)
                .editSpec()
                    .withProducer(
                            new KafkaBridgeProducerSpecBuilder()
                                    .withConfig(Map.of("acks", "1"))
                                    .build()
                    )
                    .withConsumer(
                            new KafkaBridgeConsumerSpecBuilder()
                                    .withConfig(Map.of("auto.offset.reset", "earliest"))
                                    .build()
                    )
                .endSpec()
                .build();

        when(mockDcOps.scaleDown(any(), eq(kbNamespace), any(), anyInt(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), eq(kbNamespace), any(), anyInt(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockServiceOps.reconcile(any(), eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> cmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), any(), cmCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Reconciliation reconciliation = new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName);

        Checkpoint async = context.checkpoint();
        MetricsAndLoggingUtils.metricsAndLogging(reconciliation, mockCmOps, originalBridgeCluster.logging(), null)
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

}
