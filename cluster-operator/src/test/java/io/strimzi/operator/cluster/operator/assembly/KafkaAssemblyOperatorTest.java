/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteIngressBuilder;
import io.fabric8.openshift.api.model.RouteStatus;
import io.fabric8.openshift.api.model.RouteStatusBuilder;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptions;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterSpec;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaMetadataConfigurationState;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.IngressOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NodeOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RouteOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.model.Ca.x509Certificate;
import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class KafkaAssemblyOperatorTest {
    private final static String NAMESPACE = "my-namespace";
    private final static String CLUSTER_NAME = "my-cluster";
    private final static MockCertManager CERT_MANAGER = new MockCertManager();
    private final static PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(10, "a", "a");
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
                .withAnnotations(Map.of(
                        Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled",
                        Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"
                ))
                .withGeneration(1L)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withConfig(new HashMap<>())
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("tls")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .build())
                .endKafka()
            .endSpec()
            .build();
    private final static KafkaNodePool CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .withGeneration(1L)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();
    private final static KafkaNodePool BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .withGeneration(1L)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    public static final Map<String, Object> METRICS_CONFIG = new HashMap<>();
    public static final InlineLogging LOG_KAFKA_CONFIG = new InlineLogging();

    private static WorkerExecutor sharedWorkerExecutor;

    static {
        METRICS_CONFIG.put("foo", "bar");
        LOG_KAFKA_CONFIG.setLoggers(singletonMap("kafka.root.logger.level", "INFO"));
    }

    private final String metricsCmJson = "{\"foo\":\"bar\"}";
    private final String metricsCMName = "metrics-cm";
    private final String differentMetricsCMName = "metrics-cm-2";
    private final ConfigMap metricsCM = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm(metricsCmJson, metricsCMName, "metrics-config.yml");

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;

    private static boolean openShift;
    private static boolean metrics;
    private static List<GenericKafkaListener> kafkaListeners;
    private static Map<String, Object> kafkaConfig;
    private static Storage kafkaStorage;
    private static EntityOperatorSpec eoConfig;

    public static class Params {
        private final boolean openShift;
        private final boolean metrics;
        private final List<GenericKafkaListener> kafkaListeners;
        private final Map<String, Object> kafkaConfig;
        private final Storage kafkaStorage;
        private final EntityOperatorSpec eoConfig;

        public Params(boolean openShift, boolean metrics, List<GenericKafkaListener> kafkaListeners, Map<String, Object> kafkaConfig, Storage kafkaStorage, EntityOperatorSpec eoConfig) {
            this.openShift = openShift;
            this.metrics = metrics;
            this.kafkaConfig = kafkaConfig;
            this.kafkaListeners = kafkaListeners;
            this.kafkaStorage = kafkaStorage;
            this.eoConfig = eoConfig;
        }

        public String toString() {
            return "openShift=" + openShift +
                    ",metrics=" + metrics +
                    ",kafkaListeners=" + kafkaListeners +
                    ",kafkaConfig=" + kafkaConfig +
                    ",kafkaStorage=" + kafkaStorage +
                    ",eoConfig=" + eoConfig;
        }
    }

    public static Iterable<Params> data() {
        boolean[] metricsOpenShiftAndEntityOperatorOptions = {true, false};

        SingleVolumeStorage[] storageConfig = {
            new EphemeralStorage(),
            new PersistentClaimStorageBuilder()
                    .withSize("123")
                    .withStorageClass("foo")
                    .withDeleteClaim(true)
                .build()
        };

        List<Map<String, Object>> configs = asList(
            null,
            Map.of(),
            Map.of("foo", "bar")
        );

        List<Params> result = new ArrayList<>();
        for (boolean metricsOpenShiftAndEntityOperator: metricsOpenShiftAndEntityOperatorOptions) {
            for (Map<String, Object> config : configs) {
                for (SingleVolumeStorage storage : storageConfig) {
                    EntityOperatorSpec eoConfig;
                    if (metricsOpenShiftAndEntityOperator) {
                        eoConfig = new EntityOperatorSpecBuilder()
                                .withUserOperator(new EntityUserOperatorSpecBuilder().build())
                                .withTopicOperator(new EntityTopicOperatorSpecBuilder().build())
                                .build();
                    } else {
                        eoConfig = null;
                    }

                    List<GenericKafkaListener> listeners = new ArrayList<>(3);

                    listeners.add(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .build());

                    listeners.add(new GenericKafkaListenerBuilder()
                            .withName("tls")
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build());

                    if (metricsOpenShiftAndEntityOperator) {
                        // On OpenShift, use Routes
                        listeners.add(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.ROUTE)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build());
                    } else {
                        // On Kube, use node ports
                        listeners.add(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build());
                    }

                    result.add(new Params(metricsOpenShiftAndEntityOperator, metricsOpenShiftAndEntityOperator, listeners, config, storage, eoConfig));
                }
            }
        }
        return result;
    }

    public static void setFields(Params params) {
        openShift = params.openShift;
        metrics = params.metrics;
        kafkaListeners = params.kafkaListeners;
        kafkaConfig = params.kafkaConfig;
        kafkaStorage = params.kafkaStorage;
        eoConfig = params.eoConfig;
    }

    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testCreateCluster(Params params, VertxTestContext context) {
        setFields(params);
        createCluster(context, getParametrizedKafkaCr(), getParametrizedKafkaNodePoolCrs(), emptyList());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testCreateClusterWithJmxEnabled(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafka = getParametrizedKafkaCr();
        KafkaJmxOptions jmxOptions = new KafkaJmxOptionsBuilder()
                .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                .build();
        kafka.getSpec().getKafka().setJmxOptions(jmxOptions);
        Secret kafkaJmxSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.kafkaJmxSecretName(CLUSTER_NAME))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(singletonMap("foo", "bar"))
                .build();
        createCluster(context, kafka, getParametrizedKafkaNodePoolCrs(), List.of(kafkaJmxSecret));
    }

    private Map<String, PersistentVolumeClaim> createKafkaPvcs(Map<String, Storage> storageMap, Set<NodeRef> nodes) {
        Map<String, PersistentVolumeClaim> pvcs = new HashMap<>();

        for (NodeRef node : nodes) {
            Storage storage = storageMap.get(node.poolName());

            if (storage instanceof PersistentClaimStorage) {
                Integer storageId = ((PersistentClaimStorage) storage).getId();
                String pvcName = VolumeUtils.createVolumePrefix(storageId, false) + "-" + node.podName();

                PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                        .withNewMetadata()
                            .withNamespace(NAMESPACE)
                            .withName(pvcName)
                        .endMetadata()
                        .build();

                pvcs.put(pvcName, pvc);
            }
        }

        return pvcs;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity", "checkstyle:JavaNCSS", "checkstyle:MethodLength"})
    private void createCluster(VertxTestContext context, Kafka kafka, List<KafkaNodePool> nodePools, List<Secret> secrets) {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, nodePools, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        EntityOperator entityOperator = EntityOperator.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        // Kafka CRs
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        // KafkaNodePool CRs
        CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> mockKafkaNodePoolOps = supplier.kafkaNodePoolOperator;
        when(mockKafkaNodePoolOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(nodePools));
        when(mockKafkaNodePoolOps.updateStatusAsync(any(), any(KafkaNodePool.class))).thenReturn(Future.succeededFuture());

        // StrimziPodSets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        ConcurrentHashMap<String, StrimziPodSet> podSets = new ConcurrentHashMap<>();
        ArgumentCaptor<StrimziPodSet> spsCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), startsWith(CLUSTER_NAME + "-"), spsCaptor.capture())).thenAnswer(i -> {
            StrimziPodSet podSet = i.getArgument(3, StrimziPodSet.class);
            podSets.put(podSet.getMetadata().getName(), podSet);
            return Future.succeededFuture(ReconcileResult.created(podSet));
        });
        when(mockPodSetOps.getAsync(eq(NAMESPACE), startsWith(CLUSTER_NAME + "-"))).thenAnswer(i -> Future.succeededFuture(podSets.get(i.getArgument(1, String.class))));
        when(mockPodSetOps.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        when(mockPodSetOps.listAsync(eq(NAMESPACE), eq(kafkaCluster.getSelectorLabels()))).thenAnswer(i -> Future.succeededFuture(new ArrayList<>(podSets.values())));

        StatefulSetOperator mockStsOps = supplier.stsOperations;
        when(mockStsOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));

        // Config maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ArgumentCaptor<ConfigMap> metricsCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        ArgumentCaptor<String> metricsNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.reconcile(any(), anyString(), metricsNameCaptor.capture(), metricsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<ConfigMap> logCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        ArgumentCaptor<String> logNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.reconcile(any(), anyString(), logNameCaptor.capture(), logCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockCmOps.getAsync(NAMESPACE, metricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.getAsync(NAMESPACE, differentMetricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.listAsync(NAMESPACE, kafkaCluster.getSelectorLabels())).thenReturn(Future.succeededFuture(List.of()));
        when(mockCmOps.deleteAsync(any(), any(), any(), anyBoolean())).thenReturn(Future.succeededFuture());

        // Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;

        Set<Service> createdServices = new HashSet<>();
        createdServices.add(kafkaCluster.generateService());
        createdServices.add(kafkaCluster.generateHeadlessService());
        createdServices.addAll(kafkaCluster.generateExternalBootstrapServices());
        createdServices.addAll(kafkaCluster.generatePerPodServices());

        Map<String, Service> expectedServicesMap = createdServices.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));
        when(mockServiceOps.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();         // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockServiceOps.getAsync(eq(NAMESPACE), anyString())).thenAnswer(i -> {
            Service svc = expectedServicesMap.get(i.<String>getArgument(1));

            if (svc != null && "NodePort".equals(svc.getSpec().getType()))    {
                svc.getSpec().getPorts().get(0).setNodePort(32000);
            }

            return Future.succeededFuture(svc);
        });

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Service())));
        when(mockServiceOps.endpointReadiness(any(), anyString(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockServiceOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // PVCs
        PvcOperator mockPvcOps = supplier.pvcOperations;
        Map<String, PersistentVolumeClaim> kafkaPvcs = createKafkaPvcs(kafkaCluster.getStorageByPoolName(), kafkaCluster.nodes());
        when(mockPvcOps.get(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(kafkaCluster.getComponentName())) {
                        return kafkaPvcs.get(pvcName);
                    } else {
                        return null;
                    }
                });
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(kafkaCluster.getComponentName())) {
                        return Future.succeededFuture(kafkaPvcs.get(pvcName));
                    } else {
                        return Future.succeededFuture(null);
                    }
                });

        when(mockPvcOps.listAsync(eq(NAMESPACE), ArgumentMatchers.any(Labels.class))).thenAnswer(invocation -> Future.succeededFuture(emptyList()));
        Set<String> expectedPvcs = kafkaPvcs.keySet();
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPodOps.listAsync(anyString(), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Deployments
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        when(mockDepOps.reconcile(any(), anyString(), anyString(), any())).thenAnswer(invocation -> {
            String name = invocation.getArgument(2);
            Deployment desired = invocation.getArgument(3);
            if (desired != null) {
                if (name.contains("operator")) {
                    if (entityOperator != null) {
                        context.verify(() -> assertThat(desired.getMetadata().getName(), is(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))));
                    }
                } else if (name.contains("exporter"))   {
                    context.verify(() -> assertThat(metrics, is(true)));
                }
            }
            return Future.succeededFuture(desired != null ? ReconcileResult.created(desired) : ReconcileResult.deleted());
        });
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(mockDepOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockDepOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        Set<String> expectedSecrets = set(
                KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
                KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
                KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME),
                KafkaResources.clusterCaKeySecretName(CLUSTER_NAME),
                KafkaResources.kafkaSecretName(CLUSTER_NAME),
                KafkaResources.clusterOperatorCertsSecretName(CLUSTER_NAME));

        if (metrics)    {
            expectedSecrets.add(KafkaExporterResources.secretName(CLUSTER_NAME));
        }

        expectedSecrets.addAll(secrets.stream().map(s -> s.getMetadata().getName()).collect(Collectors.toSet()));
        if (eoConfig != null) {
            // it's expected only when the Entity Operator is deployed by the Cluster Operator
            expectedSecrets.add(KafkaResources.entityTopicOperatorSecretName(CLUSTER_NAME));
            expectedSecrets.add(KafkaResources.entityUserOperatorSecretName(CLUSTER_NAME));
        }

        Map<String, Secret> secretsMap = secrets.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));
        when(mockSecretOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(new ArrayList<>(secretsMap.values())));
        when(mockSecretOps.getAsync(anyString(), any())).thenAnswer(i -> Future.succeededFuture(secretsMap.get(i.<String>getArgument(1))));
        when(mockSecretOps.getAsync(NAMESPACE, KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME))).thenAnswer(i -> Future.succeededFuture(secretsMap.get(i.<String>getArgument(1))));
        when(mockSecretOps.getAsync(NAMESPACE, KafkaResources.clusterOperatorCertsSecretName(CLUSTER_NAME))).thenAnswer(i -> Future.succeededFuture(secretsMap.get(i.<String>getArgument(1))));

        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenAnswer(invocation -> {
            Secret desired = invocation.getArgument(3);
            if (desired != null) {
                secretsMap.put(desired.getMetadata().getName(), desired);
            }
            return Future.succeededFuture(ReconcileResult.created(new Secret()));
        });

        // Network Policies
        NetworkPolicyOperator mockPolicyOps = supplier.networkPolicyOperator;
        ArgumentCaptor<NetworkPolicy> policyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockPolicyOps.reconcile(any(), anyString(), anyString(), policyCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));

        // PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), anyString(), pdbCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        // Routes
        RouteOperator mockRouteOps = supplier.routeOperations;
        ArgumentCaptor<Route> routeCaptor = ArgumentCaptor.forClass(Route.class);
        ArgumentCaptor<String> routeNameCaptor = ArgumentCaptor.forClass(String.class);
        if (openShift) {
            Set<Route> expectedRoutes = new HashSet<>(kafkaCluster.generateExternalBootstrapRoutes());
            expectedRoutes.addAll(kafkaCluster.generateExternalRoutes());

            Map<String, Route> expectedRoutesMap = expectedRoutes.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));

            // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
            when(mockRouteOps.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
            when(mockRouteOps.getAsync(eq(NAMESPACE), anyString())).thenAnswer(i -> {
                Route rt = expectedRoutesMap.get(i.<String>getArgument(1));

                if (rt != null)    {
                    RouteStatus st = new RouteStatusBuilder()
                            .withIngress(new RouteIngressBuilder()
                                    .withHost("host")
                                    .build())
                            .build();

                    rt.setStatus(st);
                }

                return Future.succeededFuture(rt);
            });
            when(mockRouteOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
            when(mockRouteOps.reconcile(any(), eq(NAMESPACE), routeNameCaptor.capture(), routeCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Route())));
        }

        // Ingresses
        IngressOperator mockIngressOps = supplier.ingressOperations;
        when(mockIngressOps.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        when(mockIngressOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Nodes
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config
        );

        // Now try to create a KafkaCluster based on this CM
        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), kafka)
            .onComplete(context.succeeding(status -> context.verify(() -> {
                // Check Services
                List<String> expectedServices = new ArrayList<>();
                expectedServices.add(KafkaResources.bootstrapServiceName(CLUSTER_NAME));
                expectedServices.add(KafkaResources.brokersServiceName(CLUSTER_NAME));
                if (kafkaListeners != null) {
                    List<GenericKafkaListener> externalListeners = ListenersUtils.listenersWithOwnServices(kafkaListeners);

                    for (GenericKafkaListener listener : externalListeners) {
                        expectedServices.add(ListenersUtils.backwardsCompatibleBootstrapServiceName(CLUSTER_NAME, listener));

                        for (NodeRef node : kafkaCluster.nodes()) {
                            if (node.broker()) {
                                expectedServices.add(ListenersUtils.backwardsCompatiblePerBrokerServiceName("my-cluster-brokers", node.nodeId(), listener));
                            }
                        }
                    }
                }

                List<Service> capturedServices = serviceCaptor.getAllValues();
                assertThat(capturedServices.stream().filter(Objects::nonNull).map(svc -> svc.getMetadata().getName()).toList().size(), is(expectedServices.size()));
                assertThat(capturedServices.stream().filter(Objects::nonNull).map(svc -> svc.getMetadata().getName()).toList(), is(expectedServices));

                // Check Pod Sets
                List<StrimziPodSet> capturedSps = spsCaptor.getAllValues();
                assertThat(capturedSps.stream().map(sps -> sps.getMetadata().getName()).collect(Collectors.toSet()), is(Set.of("my-cluster-brokers", "my-cluster-controllers")));

                // Check secrets
                assertThat(new TreeSet<>(secretsMap.keySet()), is(new TreeSet<>(expectedSecrets)));

                // Check CA expiration metrics
                MeterRegistry meterRegistry = ops.metrics().metricsProvider().meterRegistry();
                List<Meter> expectedMetrics = meterRegistry
                        .getMeters()
                        .stream()
                        .filter(m -> m.getId().getName().equals(KafkaAssemblyOperatorMetricsHolder.METRICS_CERTIFICATE_EXPIRATION_MS))
                        .toList();
                assertThat(expectedMetrics, hasSize(2));

                for (Meter expectedMetric : expectedMetrics) {
                    long metricValue = ((Double) expectedMetric.measure().iterator().next().getValue()).longValue();
                    String caTypeTag = expectedMetric.getId().getTag("type");
                    assertNotNull(caTypeTag);

                    // The actual type of the ca does not matter, as MockCertManager is using CLUSTER_CERT for both cluster and client
                    String expectedCa = MockCertManager.clusterCaCert();
                    try {
                        X509Certificate x509Certificate = x509Certificate(Base64.getDecoder().decode(expectedCa));
                        assertThat(metricValue, is(x509Certificate.getNotAfter().getTime()));
                    } catch (CertificateException e) {
                        fail("Failure decoding cluster CA cert");
                    }
                }

                // Check PDBs
                assertThat(pdbCaptor.getAllValues(), hasSize(1));
                assertThat(pdbCaptor.getValue().getMetadata().getName(), is(KafkaResources.kafkaComponentName(CLUSTER_NAME)));

                // Check PVCs
                assertThat(pvcCaptor.getAllValues(), hasSize(expectedPvcs.size()));
                assertThat(pvcCaptor.getAllValues().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet()), is(expectedPvcs));
                for (PersistentVolumeClaim pvc : pvcCaptor.getAllValues()) {
                    assertThat(pvc.getMetadata().getAnnotations(), hasKey(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM));
                }

                // Check routes
                if (openShift) {
                    Set<String> expectedRoutes = set(KafkaResources.bootstrapServiceName(CLUSTER_NAME));
                    for (NodeRef node : kafkaCluster.nodes()) {
                        if (node.broker()) {
                            expectedRoutes.add(node.podName());
                        }
                    }

                    assertThat(captured(routeNameCaptor), is(expectedRoutes));
                } else {
                    assertThat(routeNameCaptor.getAllValues(), hasSize(0));
                }

                async.flag();
            })));
    }

    private Kafka getParametrizedKafkaCr() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(kafkaListeners)
                        .withMetricsConfig(metrics ? null : io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics("metrics-config.yml", "metrics-cm"))
                    .endKafka()
                    .withEntityOperator(eoConfig)
                    .withKafkaExporter(metrics ? new KafkaExporterSpec() : null)
                .endSpec()
                .build();

        if (kafkaConfig != null)    {
            kafka.getSpec().getKafka().setConfig(kafkaConfig);
        }

        return kafka;
    }

    private List<KafkaNodePool> getParametrizedKafkaNodePoolCrs() {
        KafkaNodePool controllers = new KafkaNodePoolBuilder(CONTROLLERS)
                .editSpec()
                    .withStorage(kafkaStorage)
                .endSpec()
                .build();
        KafkaNodePool brokers = new KafkaNodePoolBuilder(BROKERS)
                .editSpec()
                    .withStorage(kafkaStorage)
                .endSpec()
                .build();

        return List.of(controllers, brokers);
    }

    private static <T> Set<T> captured(ArgumentCaptor<T> captor) {
        return new HashSet<>(captor.getAllValues());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterNoop(Params params, VertxTestContext context) {
        setFields(params);
        updateCluster(context, getParametrizedKafkaCr(), getParametrizedKafkaCr(), getParametrizedKafkaNodePoolCrs(), getParametrizedKafkaNodePoolCrs());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaClusterChangeImage(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getParametrizedKafkaCr();
        kafkaAssembly.getSpec().getKafka().setImage("a-changed-image");
        updateCluster(context, getParametrizedKafkaCr(), kafkaAssembly, getParametrizedKafkaNodePoolCrs(), getParametrizedKafkaNodePoolCrs());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaClusterScaleUp(Params params, VertxTestContext context) {
        setFields(params);

        List<KafkaNodePool> updatedNodePools = getParametrizedKafkaNodePoolCrs();
        updatedNodePools.stream().filter(knp -> "brokers".equals(knp.getMetadata().getName())).findFirst().orElseThrow().getSpec().setReplicas(4);

        updateCluster(context, getParametrizedKafkaCr(), getParametrizedKafkaCr(), getParametrizedKafkaNodePoolCrs(), updatedNodePools);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaClusterScaleDown(Params params, VertxTestContext context) {
        setFields(params);

        List<KafkaNodePool> originalNodePools = getParametrizedKafkaNodePoolCrs();
        originalNodePools.stream().filter(knp -> "brokers".equals(knp.getMetadata().getName())).findFirst().orElseThrow().getSpec().setReplicas(4);

        updateCluster(context, getParametrizedKafkaCr(), getParametrizedKafkaCr(), originalNodePools, getParametrizedKafkaNodePoolCrs());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterAuthenticationTrue(Params params, VertxTestContext context) {
        setFields(params);

        Kafka kafkaAssembly = getParametrizedKafkaCr();
        KafkaJmxOptions kafkaJmxOptions = new KafkaJmxOptionsBuilder().withAuthentication(
                 new KafkaJmxAuthenticationPasswordBuilder().build())
                .build();
        kafkaAssembly.getSpec().getKafka().setJmxOptions(kafkaJmxOptions);

        updateCluster(context, getParametrizedKafkaCr(), kafkaAssembly, getParametrizedKafkaNodePoolCrs(), getParametrizedKafkaNodePoolCrs());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterLogConfig(Params params, VertxTestContext context) {
        setFields(params);

        Kafka kafkaAssembly = getParametrizedKafkaCr();
        InlineLogging logger = new InlineLogging();
        logger.setLoggers(singletonMap("kafka.root.logger.level", "DEBUG"));
        kafkaAssembly.getSpec().getKafka().setLogging(logger);

        updateCluster(context, getParametrizedKafkaCr(), kafkaAssembly, getParametrizedKafkaNodePoolCrs(), getParametrizedKafkaNodePoolCrs());
    }

    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:JavaNCSS", "checkstyle:MethodLength"})
    private void updateCluster(VertxTestContext context, Kafka originalKafka, Kafka updatedKafka, List<KafkaNodePool> originalNodePools, List<KafkaNodePool> updatedNodePools) {
        List<KafkaPool> originalPools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, originalKafka, originalNodePools, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster originalKafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, originalKafka, originalPools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        EntityOperator originalEntityOperator = EntityOperator.fromCrd(new Reconciliation("test", originalKafka.getKind(), originalKafka.getMetadata().getNamespace(), originalKafka.getMetadata().getName()), originalKafka, SHARED_ENV_PROVIDER, ResourceUtils.dummyClusterOperatorConfig());
        KafkaExporter originalKafkaExporter = KafkaExporter.fromCrd(new Reconciliation("test", originalKafka.getKind(), originalKafka.getMetadata().getNamespace(), originalKafka.getMetadata().getName()), originalKafka, VERSIONS, SHARED_ENV_PROVIDER);
        CruiseControl originalCruiseControl = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, originalKafka, VERSIONS, originalKafkaCluster.nodes(), Map.of(), Map.of(), SHARED_ENV_PROVIDER);

        List<KafkaPool> updatedPools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, updatedKafka, updatedNodePools, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);
        KafkaCluster updatedKafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, updatedKafka, updatedPools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);

        // Kafka CRs
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME))).thenReturn(Future.succeededFuture(updatedKafka));
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        // KafkaNodePool CRs
        CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> mockKafkaNodePoolOps = supplier.kafkaNodePoolOperator;
        when(mockKafkaNodePoolOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(updatedNodePools));
        when(mockKafkaNodePoolOps.updateStatusAsync(any(), any(KafkaNodePool.class))).thenReturn(Future.succeededFuture());

        // Pod Sets
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        ConcurrentHashMap<String, StrimziPodSet> podSets = new ConcurrentHashMap<>();
        originalKafkaCluster.generatePodSets(openShift, null, null, (p) -> Map.of()).forEach(sps -> podSets.put(sps.getMetadata().getName(), sps));
        when(mockPodSetOps.reconcile(any(), eq(NAMESPACE), startsWith(CLUSTER_NAME + "-"), any())).thenAnswer(invocation -> {
            StrimziPodSet sps = invocation.getArgument(3, StrimziPodSet.class);
            podSets.put(sps.getMetadata().getName(), sps);
            return Future.succeededFuture(ReconcileResult.patched(sps));
        });
        when(mockPodSetOps.getAsync(eq(NAMESPACE), startsWith(CLUSTER_NAME + "-"))).thenAnswer(i -> Future.succeededFuture(podSets.get(i.getArgument(1, String.class))));
        when(mockPodSetOps.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        when(mockPodSetOps.listAsync(eq(NAMESPACE), eq(updatedKafkaCluster.getSelectorLabels()))).thenAnswer(i -> Future.succeededFuture(new ArrayList<>(podSets.values())));

        // StatefulSets
        StatefulSetOperator mockStsOps = supplier.stsOperations;
        when(mockStsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.kafkaComponentName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture());

        // Pods
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPodOps.listAsync(anyString(), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockPodOps.waitFor(any(), eq(NAMESPACE), anyString(), eq("to be deleted"), anyLong(), anyLong(), any())).thenReturn(Future.succeededFuture()); // Needed for scale-down

        // Deployments
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        ArgumentCaptor<String> depCaptor = ArgumentCaptor.forClass(String.class);
        when(mockDepOps.reconcile(any(), anyString(), depCaptor.capture(), any())).thenReturn(Future.succeededFuture());

        if (originalEntityOperator != null) {
            when(mockDepOps.getAsync(NAMESPACE, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))).thenReturn(Future.succeededFuture(originalEntityOperator.generateDeployment(Map.of(), true, null, null)));
            when(mockDepOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
            when(mockDepOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        }

        if (originalCruiseControl != null) {
            when(mockDepOps.getAsync(NAMESPACE, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))).thenReturn(Future.succeededFuture(originalCruiseControl.generateDeployment(Map.of(), true, null, null)));
            when(mockDepOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
            when(mockDepOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        }

        if (metrics) {
            when(mockDepOps.getAsync(NAMESPACE, KafkaExporterResources.componentName(CLUSTER_NAME))).thenReturn(originalKafkaExporter == null ? null : Future.succeededFuture(originalKafkaExporter.generateDeployment(Map.of(), true, null, null)));
            when(mockDepOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
            when(mockDepOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        }

        // Config Maps
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        when(mockCmOps.getAsync(NAMESPACE, metricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.getAsync(NAMESPACE, differentMetricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.listAsync(NAMESPACE, updatedKafkaCluster.getSelectorLabels())).thenReturn(Future.succeededFuture(List.of()));
        when(mockCmOps.deleteAsync(any(), any(), any(), anyBoolean())).thenReturn(Future.succeededFuture());

        Set<String> metricsCms = set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(any(), eq(NAMESPACE), any(), any());

        Set<String> logCms = set();
        doAnswer(invocation -> {
            logCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(any(), eq(NAMESPACE), any(), any());

        // Services
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        Set<Service> expectedServices = new HashSet<>();
        expectedServices.add(updatedKafkaCluster.generateService());
        expectedServices.add(updatedKafkaCluster.generateHeadlessService());
        expectedServices.addAll(updatedKafkaCluster.generateExternalBootstrapServices());
        expectedServices.addAll(updatedKafkaCluster.generatePerPodServices());

        Map<String, Service> expectedServicesMap = expectedServices.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));

        when(mockServiceOps.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        when(mockServiceOps.endpointReadiness(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockServiceOps.getAsync(eq(NAMESPACE), anyString())).thenAnswer(i -> {
            Service svc = expectedServicesMap.get(i.<String>getArgument(1));

            if (svc != null && "NodePort".equals(svc.getSpec().getType()))    {
                svc.getSpec().getPorts().get(0).setNodePort(32000);
            }

            return Future.succeededFuture(svc);
        });
        when(mockServiceOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(
                Future.succeededFuture(asList(
                        originalKafkaCluster.generateService(),
                        originalKafkaCluster.generateHeadlessService()
                ))
        );
        when(mockServiceOps.hasNodePort(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> patchedServicesCaptor = ArgumentCaptor.forClass(String.class);
        when(mockServiceOps.reconcile(any(), eq(NAMESPACE), patchedServicesCaptor.capture(), any())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new Service())));

        // PVCs
        PvcOperator mockPvcOps = supplier.pvcOperations;
        Map<String, PersistentVolumeClaim> kafkaPvcs = createKafkaPvcs(originalKafkaCluster.getStorageByPoolName(), originalKafkaCluster.nodes());
        kafkaPvcs.putAll(createKafkaPvcs(updatedKafkaCluster.getStorageByPoolName(), updatedKafkaCluster.nodes()));

        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(originalKafkaCluster.getComponentName())) {
                        return Future.succeededFuture(kafkaPvcs.get(pvcName));
                    } else {
                        return Future.succeededFuture(null);
                    }
                });
        when(mockPvcOps.listAsync(eq(NAMESPACE), ArgumentMatchers.any(Labels.class)))
                .thenAnswer(invocation -> {
                    Labels labels = invocation.getArgument(1);
                    if (labels.toMap().get(Labels.STRIMZI_NAME_LABEL).contains("kafka")) {
                        return Future.succeededFuture(new ArrayList<>(kafkaPvcs.values()));
                    } else {
                        return Future.succeededFuture(emptyList());
                    }
                });
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Secrets
        SecretOperator mockSecretOps = supplier.secretOperations;
        when(mockSecretOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockSecretOps.getAsync(NAMESPACE, KafkaResources.kafkaJmxSecretName(CLUSTER_NAME))).thenReturn(Future.succeededFuture(originalKafkaCluster.jmx().jmxSecret(null)));
        when(mockSecretOps.getAsync(NAMESPACE, KafkaResources.entityTopicOperatorSecretName(CLUSTER_NAME))).thenReturn(Future.succeededFuture());
        when(mockSecretOps.getAsync(NAMESPACE, KafkaExporterResources.secretName(CLUSTER_NAME))).thenReturn(Future.succeededFuture());
        when(mockSecretOps.getAsync(NAMESPACE, KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME))).thenReturn(
                Future.succeededFuture(new SecretBuilder()
                        .withNewMetadata().withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)).endMetadata()
                        .addToData("ca-cert.crt", "cert")
                        .build())
        );
        when(mockSecretOps.getAsync(NAMESPACE, KafkaResources.clusterOperatorCertsSecretName(CLUSTER_NAME))).thenReturn(
                Future.succeededFuture(new SecretBuilder()
                        .withNewMetadata().withName(KafkaResources.clusterOperatorCertsSecretName(CLUSTER_NAME)).endMetadata()
                        .addToData("cluster-operator.key", "key")
                        .addToData("cluster-operator.crt", "cert")
                        .addToData("cluster-operator.p12", "p12")
                        .addToData("cluster-operator.password", "password")
                        .build())
        );
        when(mockSecretOps.getAsync(NAMESPACE, CruiseControlResources.secretName(CLUSTER_NAME))).thenReturn(Future.succeededFuture());
        when(mockSecretOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());

        // Network policies
        NetworkPolicyOperator mockPolicyOps = supplier.networkPolicyOperator;
        when(mockPolicyOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());

        // PDBs
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        when(mockPdbOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture());

        // Nodes
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Ingress resources
        IngressOperator mockIngressOps = supplier.ingressOperations;
        when(mockIngressOps.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        when(mockIngressOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Routes
        RouteOperator mockRouteOps = supplier.routeOperations;
        if (openShift) {
            Set<Route> expectedRoutes = new HashSet<>(originalKafkaCluster.generateExternalBootstrapRoutes());
            // We use the updatedKafkaCluster here to mock the Route status even for the scaled up replicas
            expectedRoutes.addAll(updatedKafkaCluster.generateExternalRoutes());

            Map<String, Route> expectedRoutesMap = expectedRoutes.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));

            // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
            when(mockRouteOps.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
            when(mockRouteOps.getAsync(eq(NAMESPACE), anyString())).thenAnswer(i -> {
                Route rt = expectedRoutesMap.get(i.<String>getArgument(1));

                if (rt != null)    {
                    RouteStatus st = new RouteStatusBuilder()
                            .withIngress(new RouteIngressBuilder()
                                    .withHost("host")
                                    .build())
                            .build();

                    rt.setStatus(st);
                }

                return Future.succeededFuture(rt);
            });
            when(mockRouteOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
            when(mockRouteOps.hasAddress(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
            when(mockRouteOps.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new Route())));
        }

        // Mock broker scale down operation
        BrokersInUseCheck operations = supplier.brokersInUseCheck;
        when(operations.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of()));

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                CERT_MANAGER,
                PASSWORD_GENERATOR,
                supplier,
                config
        );

        // Now try to update a KafkaCluster based on this CM
        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME), updatedKafka)
            .onComplete(context.succeeding(status -> context.verify(() -> {
                assertThat(status.getKafkaVersion(), is(VERSIONS.defaultVersion().version()));

                async.flag();
            })));
    }
}
