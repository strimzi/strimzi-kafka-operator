/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.operator.cluster.model.LoggingUtils;
import io.strimzi.platform.KubernetesVersion;
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
    private static final String LOGGING_CONFIG = LoggingUtils.defaultLogConfig(Reconciliation.DUMMY_RECONCILIATION, "KafkaMirrorMakerCluster")
            .asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.");

    private final String producerBootstrapServers = "foo-kafka:9092";
    private final String consumerBootstrapServers = "bar-kafka:9092";
    private final String groupId = "my-group-id";
    private final int numStreams = 2;
    private final String include = ".*";
    private final String image = "my-image:latest";

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_21;

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

        String kmmName = "foo";
        String kmmNamespace = "test";
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
        KafkaMirrorMaker kmm = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, kmmName, image, producer, consumer, include);

        when(mockMirrorOps.get(kmmNamespace, kmmName)).thenReturn(kmm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm));

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaMirrorMaker> statusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker.class);
        when(mockMirrorOps.updateStatusAsync(any(), statusCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm,
                VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, kmmNamespace, kmmName))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // No metrics config  => no CMs created
                Set<String> metricsNames = new HashSet<>();
                if (mirror.isMetricsEnabled()) {
                    metricsNames.add(KafkaMirrorMakerResources.metricsAndLogConfigMapName(kmmName));
                }

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(mirror.getComponentName()));
                Map<String, String> annotations = new HashMap<>();
                annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, LOGGING_CONFIG);
                annotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, "0");
                assertThat("Deployments are not equal", dc, is(mirror.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(mirror.getComponentName()));
                assertThat("PodDisruptionBudgets are not equal", pdb, is(mirror.generatePodDisruptionBudget()));

                // Verify status
                List<KafkaMirrorMaker> capturedMM = statusCaptor.getAllValues();
                assertThat(capturedMM, hasSize(1));
                KafkaMirrorMaker mm = capturedMM.get(0);
                assertThat(mm.getStatus().getReplicas(), is(mirror.getReplicas()));
                assertThat(mm.getStatus().getLabelSelector(), is(mirror.getSelectorLabels().toSelectorString()));
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

        String kmmName = "foo";
        String kmmNamespace = "test";

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
        KafkaMirrorMaker kmm = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, kmmName, image, producer, consumer, include);

        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm,
                VERSIONS);
        when(mockMirrorOps.get(kmmNamespace, kmmName)).thenReturn(kmm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm));
        when(mockMirrorOps.updateStatusAsync(any(), any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockDcOps.get(kmmNamespace, mirror.getComponentName())).thenReturn(mirror.generateDeployment(new HashMap<String, String>(), true, null, null));
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(kmmNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(kmmNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(kmmNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.readiness(any(), eq(kmmNamespace), eq(mirror.getComponentName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());
        
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, kmmNamespace, kmmName), kmm)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Verify Deployment Config
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(mirror.getComponentName()));
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

        String kmmName = "foo";
        String kmmNamespace = "test";

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
        KafkaMirrorMaker kmm = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, kmmName, image, producer, consumer, include);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm,
                VERSIONS);
        kmm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockMirrorOps.get(kmmNamespace, kmmName)).thenReturn(kmm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm));
        when(mockMirrorOps.updateStatusAsync(any(), any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockDcOps.get(kmmNamespace, mirror.getComponentName())).thenReturn(mirror.generateDeployment(new HashMap<>(), true, null, null));
        when(mockDcOps.readiness(any(), eq(kmmNamespace), eq(mirror.getComponentName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(kmmNamespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(kmmNamespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(kmmNamespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        // Mock CM get
        when(mockMirrorOps.get(kmmNamespace, kmmName)).thenReturn(kmm);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaMirrorMakerResources.metricsAndLogConfigMapName(kmmName))
                    .withNamespace(kmmNamespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(kmmNamespace, KafkaMirrorMakerResources.metricsAndLogConfigMapName(kmmName))).thenReturn(metricsCm);

        ConfigMap loggingCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaMirrorMakerResources.metricsAndLogConfigMapName(kmmName))
                    .withNamespace(kmmNamespace)
                    .endMetadata()
                    .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG, LOGGING_CONFIG))
                    .build();

        when(mockCmOps.get(kmmNamespace, KafkaMirrorMakerResources.metricsAndLogConfigMapName(kmmName))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(any(), eq(kmmNamespace), anyString(), any());

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, kmmNamespace, kmmName), kmm)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                KafkaMirrorMakerCluster compareTo = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm,
                        VERSIONS);

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(compareTo.getComponentName()));
                Map<String, String> annotations = new HashMap<>();
                annotations.put(Annotations.STRIMZI_LOGGING_ANNOTATION, loggingCm.getData().get(compareTo.ANCILLARY_CM_KEY_LOG_CONFIG));
                annotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, "0");
                assertThat("Deployments are not equal", dc, is(compareTo.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(compareTo.getComponentName()));
                assertThat("PodDisruptionBudgets are not equal", pdb, is(compareTo.generatePodDisruptionBudget()));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                // No metrics config  => no CMs created
                verify(mockCmOps, never()).createOrUpdate(any(), any());
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

        String kmmName = "foo";
        String kmmNamespace = "test";

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
        KafkaMirrorMaker kmm = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, kmmName, image, producer, consumer, include);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm,
                VERSIONS);
        kmm.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockMirrorOps.get(kmmNamespace, kmmName)).thenReturn(kmm);
        when(mockDcOps.get(kmmNamespace, mirror.getComponentName())).thenReturn(mirror.generateDeployment(new HashMap<>(), true, null, null));
        when(mockDcOps.readiness(any(), eq(kmmNamespace), eq(mirror.getComponentName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

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

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockMirrorOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaMirrorMaker())));
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm));
        when(mockMirrorOps.updateStatusAsync(any(), any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, kmmNamespace, kmmName), kmm)
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

        String kmmName = "foo";
        String kmmNamespace = "test";

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
        KafkaMirrorMaker kmm = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, kmmName, image, producer, consumer, include);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm,
                VERSIONS);
        kmm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockMirrorOps.get(kmmNamespace, kmmName)).thenReturn(kmm);
        when(mockDcOps.get(kmmNamespace, mirror.getComponentName())).thenReturn(mirror.generateDeployment(new HashMap<>(), true, null, null));
        when(mockDcOps.readiness(any(), eq(kmmNamespace), eq(mirror.getComponentName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), eq(kmmNamespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(any(), eq(kmmNamespace), eq(mirror.getComponentName()), eq(scaleTo));

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(any(), eq(kmmNamespace), eq(mirror.getComponentName()), eq(scaleTo));

        when(mockMirrorOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaMirrorMaker())));
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm));
        when(mockMirrorOps.updateStatusAsync(any(), any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, kmmNamespace, kmmName), kmm)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(kmmNamespace), eq(mirror.getComponentName()), eq(scaleTo));
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

        String kmmName = "foo";
        String kmmNamespace = "test";

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
        KafkaMirrorMaker kmm = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, kmmName, image, producer, consumer, include);
        KafkaMirrorMakerCluster mirror = KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm,
                VERSIONS);
        kmm.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockMirrorOps.get(kmmNamespace, kmmName)).thenReturn(kmm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm));
        when(mockMirrorOps.updateStatusAsync(any(), any(KafkaMirrorMaker.class))).thenReturn(Future.succeededFuture());
        when(mockDcOps.get(kmmNamespace, mirror.getComponentName())).thenReturn(mirror.generateDeployment(new HashMap<>(), true, null, null));
        when(mockDcOps.readiness(any(), eq(kmmNamespace), eq(mirror.getComponentName()), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), eq(kmmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(any(), eq(kmmNamespace), eq(mirror.getComponentName()), eq(scaleTo));

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(any(), eq(kmmNamespace), eq(mirror.getComponentName()), eq(scaleTo));

        when(mockMirrorOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaMirrorMaker())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, kmmNamespace, kmmName), kmm)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(kmmNamespace), eq(mirror.getComponentName()), eq(scaleTo));
                async.flag();
            })));
    }

    @Test
    public void testReconcile(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmmNamespace = "test";

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
        KafkaMirrorMaker foo = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, "foo", image, producer, consumer, include);
        KafkaMirrorMaker bar = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, "bar", image, producer, consumer, include);

        when(mockMirrorOps.listAsync(eq(kmmNamespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        // when requested ConfigMap for a specific Kafka Mirror Maker cluster
        when(mockMirrorOps.get(eq(kmmNamespace), eq("foo"))).thenReturn(foo);
        when(mockMirrorOps.get(eq(kmmNamespace), eq("bar"))).thenReturn(bar);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // providing the list of ALL Deployments for all the Kafka Mirror Maker clusters
        Labels newLabels = Labels.forStrimziKind(KafkaMirrorMaker.RESOURCE_KIND);
        when(mockDcOps.list(eq(kmmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar,
                        VERSIONS).generateDeployment(new HashMap<>(), true, null, null)));

        // providing the list Deployments for already "existing" Kafka Mirror Maker clusters
        Labels barLabels = Labels.forStrimziCluster("bar");
        when(mockDcOps.list(eq(kmmNamespace), eq(barLabels))).thenReturn(
                asList(KafkaMirrorMakerCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar,
                        VERSIONS).generateDeployment(new HashMap<>(), true, null, null))
        );
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.reconcile(any(), eq(kmmNamespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        Checkpoint createdOrUpdateAsync = context.checkpoint(2);

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<KafkaMirrorMakerStatus> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker kafkaMirrorMakerAssembly) {
                createdOrUpdated.add(kafkaMirrorMakerAssembly.getMetadata().getName());
                createdOrUpdateAsync.flag();
                return Future.succeededFuture();
            }
        };

        Checkpoint async = context.checkpoint();
        // Now try to reconcile all the Kafka Mirror Maker clusters
        ops.reconcileAll("test", kmmNamespace,
            context.succeeding(v -> context.verify(() -> {
                assertThat(createdOrUpdated, is(new HashSet<>(asList("foo", "bar"))));
                async.flag();
            })));

    }

    @Test
    public void testCreateClusterStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorOps = supplier.mirrorMakerOperator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;

        String failureMsg = "failure";
        String kmmName = "foo";
        String kmmNamespace = "test";
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
        KafkaMirrorMaker kmm = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, kmmName, image, producer, consumer, include);

        when(mockMirrorOps.get(kmmNamespace, kmmName)).thenReturn(kmm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm));
        when(mockDcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(any(), eq(kmmNamespace), eq(kmmName), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaMirrorMaker> statusCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker.class);
        when(mockMirrorOps.updateStatusAsync(any(), statusCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, kmmNamespace, kmmName))
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

        String kmmName = "foo";
        String kmmNamespace = "test";
        KafkaMirrorMakerConsumerSpec consumer = new KafkaMirrorMakerConsumerSpecBuilder()
                .withBootstrapServers(consumerBootstrapServers)
                .withGroupId(groupId)
                .withNumStreams(numStreams)
                .build();
        KafkaMirrorMakerProducerSpec producer = new KafkaMirrorMakerProducerSpecBuilder()
                .withBootstrapServers(producerBootstrapServers)
                .build();
        KafkaMirrorMaker kmm = ResourceUtils.createKafkaMirrorMaker(kmmNamespace, kmmName, image, 0, producer, consumer, include);

        when(mockMirrorOps.get(kmmNamespace, kmmName)).thenReturn(kmm);
        when(mockMirrorOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm));
        when(mockDcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<KafkaMirrorMaker> mirrorMakerCaptor = ArgumentCaptor.forClass(KafkaMirrorMaker.class);
        when(mockMirrorOps.updateStatusAsync(any(), mirrorMakerCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaMirrorMakerAssemblyOperator ops = new KafkaMirrorMakerAssemblyOperator(vertx,
                new PlatformFeaturesAvailability(true, kubernetesVersion),
                new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                supplier,
                ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker.RESOURCE_KIND, kmmNamespace, kmmName))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // 0 Replicas - readiness should never get called.
                    verify(mockDcOps, never()).readiness(any(), anyString(), anyString(), anyLong(), anyLong());

                    assertThat(mirrorMakerCaptor.getValue().getStatus().getConditions().get(0).getType(), is("Ready"));
                    async.flag();
                })));
    }
}
