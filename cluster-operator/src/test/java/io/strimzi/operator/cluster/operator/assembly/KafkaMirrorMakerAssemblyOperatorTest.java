/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaMirrorMakerCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
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

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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
public class KafkaMirrorMakerAssemblyOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    protected static Vertx vertx;
    private static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";
    private static final String LOGGING_CONFIG = AbstractModel.getOrderedProperties("mirrorMakerDefaultLoggingProperties")
            .asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.");

    private final String producerBootstrapServers = "foo-kafka:9092";
    private final String consumerBootstrapServers = "bar-kafka:9092";
    private final String groupId = "my-group-id";
    private final int numStreams = 2;
    private final String whitelist = ".*";
    private final String image = "my-image:latest";

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
    public void testCreateCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaMirrorMaker> statusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker.class);
        when(mockMirrorOps.updateStatusAsync(statusCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm,
                VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // No metrics config  => no CMs created
                Set<String> metricsNames = new HashSet<>();
                if (mirror.isMetricsEnabled()) {
                    metricsNames.add(KafkaMirrorMakerResources.metricsAndLogConfigMapName(clusterCmName));
                }

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(mirror.getName()));
                Map annotations = new HashMap();
                annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, LOGGING_CONFIG);
                assertThat("Deployments are not equal", dc, is(mirror.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(mirror.getName()));
                assertThat("PodDisruptionBudgets are not equal", pdb, is(mirror.generatePodDisruptionBudget()));

                // Verify status
                List<KafkaMirrorMaker> capturedMM = statusCaptor.getAllValues();
                assertThat(capturedMM, hasSize(1));
                KafkaMirrorMaker mm = capturedMM.get(0);
                assertThat(mm.getStatus().getReplicas(), is(mirror.getReplicas()));
                assertThat(mm.getStatus().getPodSelector().getMatchLabels(), is(mirror.getSelectorLabels().toMap()));
                assertThat(mm.getStatus().getConditions().get(0).getType(), is("Ready"));
                assertThat(mm.getStatus().getConditions().get(0).getStatus(), is("True"));

                async.flag();
            })));
    }

    @Test
    public void testUpdateClusterNoDiff(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);

        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm,
                VERSIONS);
        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockMirrorOps.updateStatusAsync(any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(clusterCmNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(clusterCmNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(clusterCmNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.readiness(eq(clusterCmNamespace), eq(mirror.getName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify Deployment Config
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(mirror.getName()));
                assertThat("PodDisruptionBudgets are not equal", pdb, is(mirror.generatePodDisruptionBudget()));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                async.flag();
            })));
    }

    @Test
    public void testUpdateCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCmP = new HashMap<>();
        metricsCmP.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCmP);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm,
                VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockMirrorOps.updateStatusAsync(any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(eq(clusterCmNamespace), eq(mirror.getName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(eq(clusterCmNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(eq(clusterCmNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(eq(clusterCmNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock CM get
        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaMirrorMakerResources.metricsAndLogConfigMapName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(clusterCmNamespace, KafkaMirrorMakerResources.metricsAndLogConfigMapName(clusterCmName))).thenReturn(metricsCm);

        ConfigMap loggingCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaMirrorMakerResources.metricsAndLogConfigMapName(clusterCmName))
                    .withNamespace(clusterCmNamespace)
                    .endMetadata()
                    .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG, LOGGING_CONFIG))
                    .build();

        when(mockCmOps.get(clusterCmNamespace, KafkaMirrorMakerResources.metricsAndLogConfigMapName(clusterCmName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(clusterCmNamespace), anyString(), any());

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                KafkaMirrorMakerCluster compareTo = KafkaMirrorMakerCluster.fromCrd(clusterCm,
                        VERSIONS);

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(compareTo.getName()));
                Map<String, String> annotations = new HashMap();
                annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, loggingCm.getData().get(compareTo.ANCILLARY_CM_KEY_LOG_CONFIG));
                assertThat("Deployments are not equal", dc, is(compareTo.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(compareTo.getName()));
                assertThat("PodDisruptionBudgets are not equal", pdb, is(compareTo.generatePodDisruptionBudget()));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                // No metrics config  => no CMs created
                verify(mockCmOps, never()).createOrUpdate(any());
                async.flag();
            })));
    }

    @Test
    public void testUpdateClusterFailure(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm,
                VERSIONS);
        clusterCm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(eq(clusterCmNamespace), eq(mirror.getName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

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

        when(mockMirrorOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaMirrorMaker())));
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockMirrorOps.updateStatusAsync(any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm)
            .onComplete(context.failing(v -> context.verify(() -> async.flag())));
    }

    @Test
    public void testUpdateClusterScaleUp(VertxTestContext context) {
        final int scaleTo = 4;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm,
                VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(eq(clusterCmNamespace), eq(mirror.getName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, mirror.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, mirror.getName(), scaleTo);

        when(mockMirrorOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaMirrorMaker())));
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockMirrorOps.updateStatusAsync(any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(clusterCmNamespace, mirror.getName(), scaleTo);
                async.flag();
            })));
    }

    @Test
    public void testUpdateClusterScaleDown(VertxTestContext context) {
        int scaleTo = 2;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(clusterCm,
                VERSIONS);
        clusterCm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockMirrorOps.updateStatusAsync(any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockDcOps.get(clusterCmNamespace, mirror.getName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.readiness(eq(clusterCmNamespace), eq(mirror.getName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(clusterCmNamespace, mirror.getName(), scaleTo);

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(clusterCmNamespace, mirror.getName(), scaleTo);

        when(mockMirrorOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaMirrorMaker())));
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(clusterCmNamespace, mirror.getName(), scaleTo);
                async.flag();
            })));
    }

    @Test
    public void testReconcile(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String clusterCmNamespace = "test";

        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");

        KafkaMirrorMaker foo = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, "foo", image, producer, consumer, whitelist, metricsCm);
        KafkaMirrorMaker bar = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, "bar", image, producer, consumer, whitelist, metricsCm);

        when(mockMirrorOps.listAsync(eq(clusterCmNamespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        // when requested ConfigMap for a specific Kafka Mirror Maker cluster
        when(mockMirrorOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockMirrorOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // providing the list of ALL Deployments for all the Kafka Mirror Maker clusters
        Labels newLabels = Labels.forStrimziKind(KafkaMirrorMaker.RESOURCE_KIND);
        when(mockDcOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaMirrorMakerCluster.fromCrd(bar,
                        VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null)));

        // providing the list Deployments for already "existing" Kafka Mirror Maker clusters
        Labels barLabels = Labels.forStrimziCluster("bar");
        when(mockDcOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaMirrorMakerCluster.fromCrd(bar,
                        VERSIONS).generateDeployment(new HashMap<String, String>(), true, null, null))
        );
        when(mockDcOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.reconcile(eq(clusterCmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        Checkpoint createdOrUpdateAsync = context.checkpoint(2);

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker kafkaMirrorMakerAssembly) {
                createdOrUpdated.add(kafkaMirrorMakerAssembly.getMetadata().getName());
                createdOrUpdateAsync.flag();
                return Future.succeededFuture();
            }
        };

        Checkpoint async = context.checkpoint();
        // Now try to reconcile all the Kafka Mirror Maker clusters
        ops.reconcileAll("test", clusterCmNamespace,
            context.succeeding(v -> context.verify(() -> {
                assertThat(createdOrUpdated, is(new HashSet(asList("foo", "bar"))));
                async.flag();
            })));

    }

    @Test
    public void testCreateClusterStatusNotReady(VertxTestContext context) throws InterruptedException {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        String failureMsg = "failure";
        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");
        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, producer, consumer, whitelist, metricsCm);

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockDcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(eq(clusterCmNamespace), eq(clusterCmName), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaMirrorMaker> statusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker.class);
        when(mockMirrorOps.updateStatusAsync(statusCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm)
            .onComplete(context.failing(e -> context.verify(() -> {
                // Verify status
                List<KafkaMirrorMaker> capturedMM = statusCaptor.getAllValues();
                assertThat(capturedMM, hasSize(1));
                KafkaMirrorMaker mm = capturedMM.get(0);
                assertThat(mm.getStatus().getConditions().get(0).getType(), is("NotReady"));
                assertThat(mm.getStatus().getConditions().get(0).getStatus(), is("True"));
                assertThat(mm.getStatus().getConditions().get(0).getMessage(), is(failureMsg));

                async.flag();
            })));
    }

    @Test
    public void testCreateOrUpdateZeroReplica(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        String clusterCmName = "foo";
        String clusterCmNamespace = "test";
        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        Map<String, Object> metricsCm = new HashMap<>();
        metricsCm.put("foo", "bar");

        KafkaMirrorMaker clusterCm = ResourceUtils.createKafkaMirrorMakerCluster(clusterCmNamespace, clusterCmName, image, 0, producer, consumer, whitelist, metricsCm);

        when(mockMirrorOps.get(clusterCmNamespace, clusterCmName)).thenReturn(clusterCm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(clusterCm));
        when(mockDcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<KafkaMirrorMaker> mirrorMakerCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker.class);
        when(mockMirrorOps.updateStatusAsync(mirrorMakerCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // 0 Replicas - readiness should never get called.
                    verify(mockDcOps, never()).readiness(anyString(), anyString(), anyLong(), anyLong());

                    async.flag();
                })));
    }
}
