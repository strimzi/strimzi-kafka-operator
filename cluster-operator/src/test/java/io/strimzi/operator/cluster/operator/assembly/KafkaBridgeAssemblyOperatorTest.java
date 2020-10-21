/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaBridgeCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaBridgeAssemblyOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    protected static Vertx vertx;
    private static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";
    private static final String LOGGING_CONFIG = AbstractModel.getOrderedProperties("kafkaBridgeDefaultLoggingProperties")
            .asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.");

    private static final String BOOTSTRAP_SERVERS = "foo-kafka:9092";
    private static final KafkaBridgeConsumerSpec KAFKA_BRIDGE_CONSUMER_SPEC = new KafkaBridgeConsumerSpec();
    private static final KafkaBridgeProducerSpec KAFKA_BRIDGE_PRODUCER_SPEC = new KafkaBridgeProducerSpec();
    private static final KafkaBridgeHttpConfig KAFKA_BRIDGE_HTTP_SPEC = new KafkaBridgeHttpConfig();
    private final String image = "kafka-bridge:latest";

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_9;

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
        CrdOperator mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<KafkaBridge> bridgeCaptor = ArgumentCaptor.forClass(KafkaBridge.class);
        when(mockBridgeOps.updateStatusAsync(bridgeCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(kb,
                VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), kb)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(bridge.getServiceName()));
                assertThat(service, is(bridge.generateService()));

                // No metrics config  => no CMs created
                Set<String> metricsNames = new HashSet<>();
                if (bridge.isMetricsEnabled()) {
                    metricsNames.add(KafkaBridgeResources.metricsAndLogConfigMapName(kbName));
                }

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(bridge.getName()));
                assertThat(dc, is(bridge.generateDeployment(Collections.emptyMap(), true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb.size(), is(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(bridge.getName()));
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
        CrdOperator mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(kb,
                VERSIONS);
        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getName())).thenReturn(bridge.generateService());
        when(mockDcOps.get(kbNamespace, bridge.getName())).thenReturn(bridge.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(eq(kbNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(kbNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(kbNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(kbNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
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
                assertThat(pdb.getMetadata().getName(), is(bridge.getName()));
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
        CrdOperator mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        Map<String, Object> metricsCmP = new HashMap<>();
        metricsCmP.put("foo", "bar");
        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(kb,
                VERSIONS);
        kb.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getName())).thenReturn(bridge.generateService());
        when(mockDcOps.get(kbNamespace, bridge.getName())).thenReturn(bridge.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(eq(kbNamespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(kbNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(kbNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(kbNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock CM get
        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaBridgeResources.metricsAndLogConfigMapName(kbName))
                    .withNamespace(kbNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(kbNamespace, KafkaBridgeResources.metricsAndLogConfigMapName(kbName))).thenReturn(metricsCm);

        ConfigMap loggingCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaBridgeResources.metricsAndLogConfigMapName(kbName))
                    .withNamespace(kbNamespace)
                    .endMetadata()
                    .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG, LOGGING_CONFIG))
                    .build();

        when(mockCmOps.get(kbNamespace, KafkaBridgeResources.metricsAndLogConfigMapName(kbName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(kbNamespace), anyString(), any());

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), kb)
            .onComplete(context.succeeding(v -> context.verify(() -> {

                KafkaBridgeCluster compareTo = KafkaBridgeCluster.fromCrd(kb,
                        VERSIONS);

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
                assertThat(dc, is(compareTo.generateDeployment(Collections.emptyMap(), true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(compareTo.getName()));
                assertThat(pdb, is(compareTo.generatePodDisruptionBudget()));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                // No metrics config  => no CMs created
                verify(mockCmOps, never()).createOrUpdate(any());
                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateThrowsWhenCreateServiceThrows(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(kb,
                VERSIONS);
        kb.getSpec().setImage("some/different:image"); // Change the image to generate some differences

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getName())).thenReturn(bridge.generateService());
        when(mockDcOps.get(kbNamespace, bridge.getName())).thenReturn(bridge.generateDeployment(new HashMap<String, String>(), true, null, null));
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

        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockBridgeOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaBridge())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

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
        CrdOperator mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";

        KafkaBridge kb = ResourceUtils.createEmptyKafkaBridge(kbNamespace, kbName);
        kb.getSpec().setReplicas(0);
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(kb, VERSIONS);
        kb.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getName())).thenReturn(bridge.generateService());
        Deployment dep = bridge.generateDeployment(new HashMap<>(), true, null, null);
        when(mockDcOps.get(kbNamespace, bridge.getName())).thenReturn(dep);
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(kbNamespace, bridge.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(kbNamespace, bridge.getName(), scaleTo);

        when(mockBridgeOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaBridge())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), kb)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(kbNamespace, bridge.getName(), scaleTo);

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateWithScaleDown(VertxTestContext context) {
        int scaleTo = 1;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockBridgeOps = supplier.kafkaBridgeOperator;
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
        KafkaBridgeCluster bridge = KafkaBridgeCluster.fromCrd(kb, VERSIONS);

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockBridgeOps.updateStatusAsync(any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kbNamespace, bridge.getName())).thenReturn(bridge.generateService());
        Deployment dep = bridge.generateDeployment(new HashMap<>(), true, null, null);
        when(mockDcOps.get(kbNamespace, bridge.getName())).thenReturn(dep);
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(kbNamespace, bridge.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(kbNamespace, bridge.getName(), scaleTo);

        when(mockBridgeOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaBridge())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"), supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), scaledDownCluster)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(kbNamespace, bridge.getName(), scaleTo);
                verify(mockDcOps).scaleDown(kbNamespace, bridge.getName(), scaleTo);
                async.flag();
            })));
    }

    @Test
    public void testReconcileCallsCreateOrUpdate(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kbNamespace = "test";

        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");

        KafkaBridge foo = ResourceUtils.createKafkaBridge(kbNamespace, "foo", image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);
        KafkaBridge bar = ResourceUtils.createKafkaBridge(kbNamespace, "bar", image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        when(mockBridgeOps.listAsync(eq(kbNamespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(bar));
        when(mockBridgeOps.updateStatusAsync(any(KafkaBridge.class))).thenReturn(Future.succeededFuture());
        // when requested ConfigMap for a specific Kafka Bridge cluster
        when(mockBridgeOps.get(eq(kbNamespace), eq("foo"))).thenReturn(foo);
        when(mockBridgeOps.get(eq(kbNamespace), eq("bar"))).thenReturn(bar);

        // providing the list of ALL Deployments for all the Kafka Bridge clusters
        Labels newLabels = Labels.forStrimziKind(KafkaBridge.RESOURCE_KIND);
        when(mockDcOps.list(eq(kbNamespace), eq(newLabels))).thenReturn(
                asList(KafkaBridgeCluster.fromCrd(bar,
                        VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null)));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // providing the list Deployments for already "existing" Kafka Bridge clusters
        Labels barLabels = Labels.forStrimziCluster("bar");
        when(mockDcOps.list(eq(kbNamespace), eq(barLabels))).thenReturn(
                asList(KafkaBridgeCluster.fromCrd(bar,
                        VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null))
        );

        when(mockSecretOps.reconcile(eq(kbNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        // Should be called twice, once for foo and once for bar
        Checkpoint asyncCreatedOrUpdated = context.checkpoint(2);

        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaBridge kafkaBridgeAssembly) {
                createdOrUpdated.add(kafkaBridgeAssembly.getMetadata().getName());
                asyncCreatedOrUpdated.flag();
                return Future.succeededFuture();
            }
        };

        Checkpoint async = context.checkpoint();
        Promise reconciled = Promise.promise();
        // Now try to reconcile all the Kafka Bridge clusters
        ops.reconcileAll("test", kbNamespace, v -> reconciled.complete());

        reconciled.future().onComplete(context.succeeding(v -> context.verify(() -> {
            assertThat(createdOrUpdated, is(new HashSet(asList("foo", "bar"))));
            async.flag();
        })));


    }

    @Test
    public void testCreateClusterStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";
        String failureMsg = "failure";
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 1,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockServiceOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<KafkaBridge> bridgeCaptor = ArgumentCaptor.forClass(KafkaBridge.class);
        when(mockBridgeOps.updateStatusAsync(bridgeCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), kb)
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
        CrdOperator mockBridgeOps = supplier.kafkaBridgeOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        String kbName = "foo";
        String kbNamespace = "test";
        String failureMsg = "failure";
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaBridge kb = ResourceUtils.createKafkaBridge(kbNamespace, kbName, image, 0,
                BOOTSTRAP_SERVERS, KAFKA_BRIDGE_PRODUCER_SPEC, KAFKA_BRIDGE_CONSUMER_SPEC, KAFKA_BRIDGE_HTTP_SPEC, true);

        when(mockBridgeOps.get(kbNamespace, kbName)).thenReturn(kb);
        when(mockBridgeOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kb));
        when(mockServiceOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        ArgumentCaptor<KafkaBridge> bridgeCaptor = ArgumentCaptor.forClass(KafkaBridge.class);
        when(mockBridgeOps.updateStatusAsync(bridgeCaptor.capture())).thenReturn(Future.succeededFuture());
        KafkaBridgeAssemblyOperator ops = new KafkaBridgeAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaBridge.RESOURCE_KIND, kbNamespace, kbName), kb)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // 0 Replicas - readiness should never get called.
                    verify(mockDcOps, never()).readiness(anyString(), anyString(), anyLong(), anyLong());
                    assertNull(bridgeCaptor.getValue().getStatus().getUrl());

                    async.flag();
                })));
    }

}
