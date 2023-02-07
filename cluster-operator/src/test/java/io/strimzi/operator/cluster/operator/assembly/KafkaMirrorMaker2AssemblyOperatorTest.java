/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.operator.cluster.model.LoggingUtils;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
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
import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaMirrorMaker2AssemblyOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    protected static Vertx vertx;
    private static final String METRICS_CONFIG = "{\"foo\":\"bar\"}";
    private static final String LOGGING_CONFIG = LoggingUtils.defaultLogConfig(Reconciliation.DUMMY_RECONCILIATION, "KafkaMirrorMaker2Cluster")
            .asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.");

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
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmm2Name = "foo";
        String kmm2Namespace = "test";
        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, kmm2Name);

        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockSecretOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaMirrorMaker2> mirrorMaker2Captor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), mirrorMaker2Captor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaMirrorMaker2Cluster mirrorMaker2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, kmm2Namespace, kmm2Name))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // No metrics config  => no CMs created
                Set<String> metricsNames = new HashSet<>();
                if (mirrorMaker2.isMetricsEnabled()) {
                    metricsNames.add(KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(kmm2Name));
                }

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));
                Service service = capturedServices.get(0);
                assertThat(service.getMetadata().getName(), is(mirrorMaker2.getServiceName()));
                assertThat(service, is(mirrorMaker2.generateService()));

                // Verify Deployment
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));
                Deployment dc = capturedDc.get(0);
                assertThat(dc.getMetadata().getName(), is(mirrorMaker2.getComponentName()));
                Map<String, String> annotations = new HashMap<>();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(LOGGING_CONFIG)));
                assertThat(dc, is(mirrorMaker2.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(mirrorMaker2.getComponentName()));
                assertThat(pdb, is(mirrorMaker2.generatePodDisruptionBudget()));
                // Verify status
                KafkaMirrorMaker2Status capturedStatus = mirrorMaker2Captor.getAllValues().get(0).getStatus();
                assertThat(capturedStatus.getUrl(), is("http://foo-mirrormaker2-api.test.svc:8083"));
                assertThat(capturedStatus.getReplicas(), is(mirrorMaker2.getReplicas()));
                assertThat(capturedStatus.getLabelSelector(), is(mirrorMaker2.getSelectorLabels().toSelectorString()));
                assertThat(capturedStatus.getConditions().get(0).getStatus(), is("True"));
                assertThat(capturedStatus.getConditions().get(0).getType(), is("Ready"));
                async.flag();
            })));
    }

    @Test
    public void testUpdateClusterNoDiff(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmm2Name = "foo";
        String kmm2Namespace = "test";

        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, kmm2Name);
        KafkaMirrorMaker2Cluster mirrorMaker2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);
        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), any(KafkaMirrorMaker2.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kmm2Namespace, mirrorMaker2.getComponentName())).thenReturn(mirrorMaker2.generateService());
        when(mockDcOps.get(kmm2Namespace, mirrorMaker2.getComponentName())).thenReturn(mirrorMaker2.generateDeployment(new HashMap<>(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(kmm2Namespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(kmm2Namespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(kmm2Namespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(kmm2Namespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, kmm2Namespace, kmm2Name), kmm2)
            .onComplete(context.succeeding(v -> context.verify(() -> {

                // Verify service
                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices, hasSize(1));

                // Verify Deployment Config
                List<Deployment> capturedDc = dcCaptor.getAllValues();
                assertThat(capturedDc, hasSize(1));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(mirrorMaker2.getComponentName()));
                assertThat("PodDisruptionBudgets are not equal", pdb, is(mirrorMaker2.generatePodDisruptionBudget()));

                async.flag();
            })));
    }

    @Test
    public void testUpdateCluster(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmm2Name = "foo";
        String kmm2Namespace = "test";

        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, kmm2Name);
        KafkaMirrorMaker2Cluster mirrorMaker2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);
        kmm2.getSpec().setImage("some/different:image"); // Change the image to generate some diff
        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), any(KafkaMirrorMaker2.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kmm2Namespace, mirrorMaker2.getComponentName())).thenReturn(mirrorMaker2.generateService());
        when(mockDcOps.get(kmm2Namespace, mirrorMaker2.getComponentName()))
                .thenReturn(mirrorMaker2.generateDeployment(new HashMap<>(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), eq(kmm2Namespace), serviceNameCaptor.capture(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), eq(kmm2Namespace), dcNameCaptor.capture(), dcCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleUpNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleUpReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleUp(any(), eq(kmm2Namespace), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), eq(kmm2Namespace), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(kmm2.getMetadata().getName())), any()))
                .thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock CM get
        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(kmm2Name))
                    .withNamespace(kmm2Namespace)
                .endMetadata()
                .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, METRICS_CONFIG))
                .build();
        when(mockCmOps.get(kmm2Namespace, KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(kmm2Name))).thenReturn(metricsCm);

        ConfigMap loggingCm = new ConfigMapBuilder().withNewMetadata()
                    .withName(KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(kmm2Name))
                    .withNamespace(kmm2Namespace)
                    .endMetadata()
                    .withData(Collections.singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG, LOGGING_CONFIG))
                    .build();

        when(mockCmOps.get(kmm2Namespace, KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(kmm2Name))).thenReturn(metricsCm);

        // Mock CM patch
        Set<String> metricsCms = TestUtils.set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(any(), eq(kmm2Namespace), anyString(), any());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, kmm2Namespace, kmm2Name), kmm2)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                KafkaMirrorMaker2Cluster compareTo = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);

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
                assertThat(dc.getMetadata().getName(), is(compareTo.getComponentName()));
                Map<String, String> annotations = new HashMap<>();
                annotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(loggingCm.getData().get(KafkaMirrorMaker2Cluster.ANCILLARY_CM_KEY_LOG_CONFIG))));
                assertThat(dc, is(compareTo.generateDeployment(annotations, true, null, null)));

                // Verify PodDisruptionBudget
                List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                assertThat(capturedPdb, hasSize(1));
                PodDisruptionBudget pdb = capturedPdb.get(0);
                assertThat(pdb.getMetadata().getName(), is(mirrorMaker2.getComponentName()));
                assertThat(pdb, is(mirrorMaker2.generatePodDisruptionBudget()));

                // Verify scaleDown / scaleUp were not called
                assertThat(dcScaleDownNameCaptor.getAllValues(), hasSize(1));
                assertThat(dcScaleUpNameCaptor.getAllValues(), hasSize(1));

                // No metrics config  => no CMs created
                verify(mockCmOps, never()).createOrUpdate(any(), any());
                async.flag();
            })));
    }

    @Test
    public void testUpdateClusterWithFailingDeploymentFailure(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmm2Name = "foo";
        String kmm2Namespace = "test";

        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, kmm2Name);
        KafkaMirrorMaker2Cluster mirrorMaker2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);
        kmm2.getSpec().setImage("some/different:image"); // Change the image to generate some diff

        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), any(KafkaMirrorMaker2.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kmm2Namespace, mirrorMaker2.getComponentName())).thenReturn(mirrorMaker2.generateService());
        when(mockDcOps.get(kmm2Namespace, mirrorMaker2.getComponentName())).thenReturn(mirrorMaker2.generateDeployment(new HashMap<>(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

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
        when(mockDcOps.scaleUp(any(), dcScaleUpNamespaceCaptor.capture(), dcScaleUpNameCaptor.capture(), dcScaleUpReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> dcScaleDownNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dcScaleDownNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> dcScaleDownReplicasCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockDcOps.scaleDown(any(), dcScaleDownNamespaceCaptor.capture(), dcScaleDownNameCaptor.capture(), dcScaleDownReplicasCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockMirrorMaker2Ops.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaMirrorMaker2())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, kmm2Namespace, kmm2Name), kmm2)
            .onComplete(context.failing(v -> async.flag()));
    }

    @Test
    public void testUpdateClusterScaleUp(VertxTestContext context) {
        final int scaleTo = 4;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmm2Name = "foo";
        String kmm2Namespace = "test";

        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, kmm2Name);
        KafkaMirrorMaker2Cluster mirrorMaker2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);
        kmm2.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleUp

        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), any(KafkaMirrorMaker2.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kmm2Namespace, mirrorMaker2.getComponentName())).thenReturn(mirrorMaker2.generateService());
        when(mockDcOps.get(kmm2Namespace, mirrorMaker2.getComponentName())).thenReturn(mirrorMaker2.generateDeployment(new HashMap<>(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), eq(kmm2Namespace), any(), any())).thenReturn(Future.succeededFuture());

        when(mockDcOps.reconcile(any(), eq(kmm2Namespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(any(), eq(kmm2Namespace), eq(mirrorMaker2.getComponentName()), eq(scaleTo));

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(any(), eq(kmm2Namespace), eq(mirrorMaker2.getComponentName()), eq(scaleTo));

        when(mockMirrorMaker2Ops.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaMirrorMaker2())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, kmm2Namespace, kmm2Name), kmm2)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(kmm2Namespace), eq(mirrorMaker2.getComponentName()), eq(scaleTo));
                async.flag();
            })));
    }

    @Test
    public void testUpdateClusterScaleDown(VertxTestContext context) {
        int scaleTo = 2;

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmm2Name = "foo";
        String kmm2Namespace = "test";

        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, kmm2Name);
        KafkaMirrorMaker2Cluster mirrorMaker2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);
        kmm2.getSpec().setReplicas(scaleTo); // Change replicas to create ScaleDown

        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), any(KafkaMirrorMaker2.class))).thenReturn(Future.succeededFuture());
        when(mockServiceOps.get(kmm2Namespace, mirrorMaker2.getComponentName())).thenReturn(mirrorMaker2.generateService());
        when(mockDcOps.get(kmm2Namespace, mirrorMaker2.getComponentName())).thenReturn(mirrorMaker2.generateDeployment(new HashMap<>(), true, null, null));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockServiceOps.reconcile(any(), eq(kmm2Namespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), eq(kmm2Namespace), any(), any())).thenReturn(Future.succeededFuture());

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleUp(any(), eq(kmm2Namespace), eq(mirrorMaker2.getComponentName()), eq(scaleTo));

        doAnswer(i -> Future.succeededFuture(scaleTo))
                .when(mockDcOps).scaleDown(any(), eq(kmm2Namespace), eq(mirrorMaker2.getComponentName()), eq(scaleTo));

        when(mockMirrorMaker2Ops.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new KafkaMirrorMaker2())));
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, kmm2Namespace, kmm2Name), kmm2)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                verify(mockDcOps).scaleUp(any(), eq(kmm2Namespace), eq(mirrorMaker2.getComponentName()), eq(scaleTo));
                async.flag();
            })));
    }

    @Test
    public void testReconcile(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;

        String kmm2Namespace = "test";

        KafkaMirrorMaker2 foo = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, "foo");
        KafkaMirrorMaker2 bar = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, "bar");
        when(mockMirrorMaker2Ops.listAsync(eq(kmm2Namespace), any(Optional.class))).thenReturn(Future.succeededFuture(asList(foo, bar)));
        // when requested ConfigMap for a specific Kafka MirrorMaker 2.0 cluster
        when(mockMirrorMaker2Ops.get(eq(kmm2Namespace), eq("foo"))).thenReturn(foo);
        when(mockMirrorMaker2Ops.get(eq(kmm2Namespace), eq("bar"))).thenReturn(bar);

        // providing the list of ALL Deployments for all the Kafka MirrorMaker 2.0 clusters
        Labels newLabels = Labels.forStrimziKind(KafkaMirrorMaker2.RESOURCE_KIND);
        when(mockDcOps.list(eq(kmm2Namespace), eq(newLabels))).thenReturn(
                asList(KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar, VERSIONS).generateDeployment(new HashMap<>(), true, null, null)));

        // providing the list Deployments for already "existing" Kafka MirrorMaker 2.0 clusters
        Labels barLabels = Labels.forStrimziCluster("bar");
        when(mockDcOps.list(eq(kmm2Namespace), eq(barLabels))).thenReturn(
                asList(KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, bar, VERSIONS).generateDeployment(new HashMap<>(), true, null, null))
        );
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.reconcile(any(), eq(kmm2Namespace), any(), any())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(any(), eq(kmm2Namespace), any(), any())).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();

        Checkpoint createOrUpdateAsync = context.checkpoint(2);
        //Must create all checkpoints used before flagging any to avoid premature test success
        Checkpoint async = context.checkpoint();

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS)) {

            @Override
            public Future<KafkaMirrorMaker2Status> createOrUpdate(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2Assembly) {
                createdOrUpdated.add(kafkaMirrorMaker2Assembly.getMetadata().getName());
                createOrUpdateAsync.flag();
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka MirrorMaker 2.0 clusters
        ops.reconcileAll("test", kmm2Namespace,
            context.succeeding(v -> context.verify(() -> {
                assertThat(createdOrUpdated, is(new HashSet<>(asList("foo", "bar"))));
                async.flag();
            })));

    }

    @Test
    public void testCreateClusterStatusNotReady(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmm2Name = "foo";
        String kmm2Namespace = "test";
        String failureMsg = "failure";
        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, kmm2Name);

        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.failedFuture(failureMsg));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockPdbOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), eq(kmm2Namespace), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaMirrorMaker2> mirrorMaker2Captor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), mirrorMaker2Captor.capture())).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS));

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, kmm2Namespace, kmm2Name))
            .onComplete(context.failing(v -> context.verify(() -> {
                // Verify status
                List<KafkaMirrorMaker2> capturedMirrorMaker2s = mirrorMaker2Captor.getAllValues();
                assertThat(capturedMirrorMaker2s.get(0).getStatus().getUrl(), is("http://foo-mirrormaker2-api.test.svc:8083"));
                assertThat(capturedMirrorMaker2s.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                assertThat(capturedMirrorMaker2s.get(0).getStatus().getConditions().get(0).getType(), is("NotReady"));
                assertThat(capturedMirrorMaker2s.get(0).getStatus().getConditions().get(0).getMessage(), is(failureMsg));
                async.flag();
            })));
    }


    @Test
    public void testCreateClusterWithZeroReplicas(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmm2Name = "foo";
        String kmm2Namespace = "test";
        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, kmm2Name, 0);

        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockSecretOps.reconcile(any(), eq(kmm2Namespace), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<KafkaMirrorMaker2> mirrorMaker2Captor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), mirrorMaker2Captor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaMirrorMaker2Cluster mirrorMaker2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, kmm2Namespace, kmm2Name))
                .onComplete(context.succeeding(v -> context.verify(() -> {

                    // 0 Replicas - readiness should never get called.
                    verify(mockDcOps, never()).readiness(any(), anyString(), anyString(), anyLong(), anyLong());

                    // Verify service
                    List<Service> capturedServices = serviceCaptor.getAllValues();
                    assertThat(capturedServices, hasSize(1));
                    Service service = capturedServices.get(0);
                    assertThat(service.getMetadata().getName(), is(mirrorMaker2.getServiceName()));
                    assertThat(service, is(mirrorMaker2.generateService()));

                    // Verify Deployment
                    List<Deployment> capturedDc = dcCaptor.getAllValues();
                    assertThat(capturedDc, hasSize(1));
                    Deployment dc = capturedDc.get(0);
                    assertThat(dc.getMetadata().getName(), is(mirrorMaker2.getComponentName()));
                    Map<String, String> annotations = new HashMap<>();
                    annotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(LOGGING_CONFIG)));
                    assertThat(dc, is(mirrorMaker2.generateDeployment(annotations, true, null, null)));

                    // Verify PodDisruptionBudget
                    List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                    assertThat(capturedPdb, hasSize(1));
                    PodDisruptionBudget pdb = capturedPdb.get(0);
                    assertThat(pdb.getMetadata().getName(), is(mirrorMaker2.getComponentName()));
                    assertThat(pdb, is(mirrorMaker2.generatePodDisruptionBudget()));

                    // Verify status
                    List<KafkaMirrorMaker2> capturedMirrorMaker2s = mirrorMaker2Captor.getAllValues();
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getUrl(), is(nullValue()));
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getReplicas(), is(mirrorMaker2.getReplicas()));
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getLabelSelector(), is(mirrorMaker2.getSelectorLabels().toSelectorString()));
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getConditions().get(0).getType(), is("Ready"));
                    async.flag();
                })));
    }

    @Test
    public void testCreateClusterWithJmxEnabled(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        PodDisruptionBudgetV1Beta1Operator mockPdbOpsV1Beta1 = supplier.podDisruptionBudgetV1Beta1Operator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        String kmm2Name = "foo";
        String kmm2Namespace = "test";
        KafkaMirrorMaker2 kmm2 = ResourceUtils.createEmptyKafkaMirrorMaker2(kmm2Namespace, kmm2Name);

        kmm2.getSpec().setJmxOptions(new KafkaJmxOptionsBuilder()
                .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                .build());

        when(mockMirrorMaker2Ops.get(kmm2Namespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockSecretOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(mockSecretOps.getAsync(anyString(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaMirrorMaker2> mirrorMaker2Captor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), mirrorMaker2Captor.capture())).thenReturn(Future.succeededFuture());

        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        KafkaMirrorMaker2AssemblyOperator ops = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
                supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        KafkaMirrorMaker2Cluster mirrorMaker2 = KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);

        Checkpoint async = context.checkpoint();
        ops.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, kmm2Namespace, kmm2Name))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    // No metrics config  => no CMs created
                    Set<String> metricsNames = new HashSet<>();
                    if (mirrorMaker2.isMetricsEnabled()) {
                        metricsNames.add(KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(kmm2Name));
                    }

                    // Verify service
                    List<Service> capturedServices = serviceCaptor.getAllValues();
                    assertThat(capturedServices, hasSize(1));
                    Service service = capturedServices.get(0);
                    assertThat(service.getMetadata().getName(), is(mirrorMaker2.getServiceName()));
                    assertThat(service, is(mirrorMaker2.generateService()));

                    // Verify Deployment
                    List<Deployment> capturedDc = dcCaptor.getAllValues();
                    assertThat(capturedDc, hasSize(1));
                    Deployment dc = capturedDc.get(0);
                    assertThat(dc.getMetadata().getName(), is(mirrorMaker2.getComponentName()));
                    Map<String, String> annotations = new HashMap<>();
                    annotations.put(Annotations.ANNO_STRIMZI_LOGGING_APPENDERS_HASH, Util.hashStub(Util.getLoggingDynamicallyUnmodifiableEntries(LOGGING_CONFIG)));
                    assertThat(dc, is(mirrorMaker2.generateDeployment(annotations, true, null, null)));

                    // Verify PodDisruptionBudget
                    List<PodDisruptionBudget> capturedPdb = pdbCaptor.getAllValues();
                    assertThat(capturedPdb, hasSize(1));
                    PodDisruptionBudget pdb = capturedPdb.get(0);
                    assertThat(pdb.getMetadata().getName(), is(mirrorMaker2.getComponentName()));
                    assertThat(pdb, is(mirrorMaker2.generatePodDisruptionBudget()));

                    // Verify status
                    List<KafkaMirrorMaker2> capturedMirrorMaker2s = mirrorMaker2Captor.getAllValues();
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getUrl(), is("http://foo-mirrormaker2-api.test.svc:8083"));
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getReplicas(), is(mirrorMaker2.getReplicas()));
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getLabelSelector(), is(mirrorMaker2.getSelectorLabels().toSelectorString()));
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getConditions().get(0).getStatus(), is("True"));
                    assertThat(capturedMirrorMaker2s.get(0).getStatus().getConditions().get(0).getType(), is("Ready"));

                    assertThat(mirrorMaker2.isJmxEnabled(), is(true));
                    assertThat(mirrorMaker2.isJmxAuthenticated(), is(true));
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

        KafkaMirrorMaker2MirrorSpec deprecatedMirrorConnector = new KafkaMirrorMaker2MirrorSpecBuilder()
            .withSourceCluster(sourceClusterAlias)
            .withTargetCluster(targetClusterAlias)
            .withTopicsExcludePattern(excludedTopicList)
            .withGroupsExcludePattern(excludedGroupList)
            .build();
        kmm2.getSpec().setMirrors(List.of(deprecatedMirrorConnector));

        KafkaMirrorMaker2AssemblyOperator mm2AssemblyOperator = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
            supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);
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

        KafkaMirrorMaker2MirrorSpec deprecatedMirrorConnector = new KafkaMirrorMaker2MirrorSpecBuilder()
            .withSourceCluster(sourceClusterAlias)
            .withTargetCluster(targetClusterAlias)
            .withTopicsBlacklistPattern(excludedTopicList)
            .withGroupsBlacklistPattern(excludedGroupList)
            .build();
        kmm2.getSpec().setMirrors(List.of(deprecatedMirrorConnector));

        KafkaMirrorMaker2AssemblyOperator mm2AssemblyOperator = new KafkaMirrorMaker2AssemblyOperator(vertx, new PlatformFeaturesAvailability(true, kubernetesVersion),
            supplier, ResourceUtils.dummyClusterOperatorConfig(VERSIONS), x -> mockConnectClient);

        Checkpoint async = context.checkpoint();
        KafkaMirrorMaker2Cluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kmm2, VERSIONS);
        mm2AssemblyOperator.reconcile(new Reconciliation("test-blacklist", KafkaMirrorMaker2.RESOURCE_KIND, targetNamespace, kmm2Name))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                KafkaMirrorMaker2MirrorSpec capturedMirrorConnector = mirrorMaker2Captor.getAllValues().get(0).getSpec().getMirrors().get(0);
                assertThat(capturedMirrorConnector.getTopicsBlacklistPattern(), is(excludedTopicList));
                assertThat(capturedMirrorConnector.getGroupsBlacklistPattern(), is(excludedGroupList));
                async.flag();
            })));
    }

    private ArgumentCaptor<KafkaMirrorMaker2> createMirrorMaker2CaptorMock(String targetNamespace, String kmm2Name, KafkaMirrorMaker2 kmm2, ResourceOperatorSupplier supplier) {
        CrdOperator mockMirrorMaker2Ops = supplier.mirrorMaker2Operator;
        DeploymentOperator mockDcOps = supplier.deploymentOperations;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        PodDisruptionBudgetV1Beta1Operator mockPdbOpsV1Beta1 = supplier.podDisruptionBudgetV1Beta1Operator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolOps = supplier.networkPolicyOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        when(mockMirrorMaker2Ops.get(targetNamespace, kmm2Name)).thenReturn(kmm2);
        when(mockMirrorMaker2Ops.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(kmm2));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Deployment> dcCaptor = ArgumentCaptor.forClass(Deployment.class);
        when(mockDcOps.reconcile(any(), anyString(), anyString(), dcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockDcOps.scaleUp(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.scaleDown(any(), anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockDcOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDcOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockCmOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));
        when(mockNetPolOps.reconcile(any(), eq(kmm2.getMetadata().getNamespace()), eq(KafkaMirrorMaker2Resources.deploymentName(
            kmm2.getMetadata().getName())), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockSecretOps.reconcile(any(), anyString(), any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget> pdbV1Beta1Captor = ArgumentCaptor.forClass(io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget.class);
        when(mockPdbOpsV1Beta1.reconcile(any(), anyString(), any(), pdbV1Beta1Captor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<KafkaMirrorMaker2> mirrorMaker2Captor = ArgumentCaptor.forClass(KafkaMirrorMaker2.class);
        when(mockMirrorMaker2Ops.updateStatusAsync(any(), mirrorMaker2Captor.capture())).thenReturn(Future.succeededFuture());

        return mirrorMaker2Captor;
    }

    private KafkaConnectApi createConnectClientMock() {
        KafkaConnectApi mockConnectClient = mock(KafkaConnectApi.class);
        when(mockConnectClient.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mockConnectClient.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());
        return mockConnectClient;
    }
}
