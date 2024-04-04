/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
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
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
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
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
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
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
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
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.Timeout;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.model.Ca.x509Certificate;
import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
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
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class KafkaAssemblyOperatorTest {
    public static final Map<String, Object> METRICS_CONFIG = new HashMap<>();
    public static final InlineLogging LOG_KAFKA_CONFIG = new InlineLogging();
    public static final InlineLogging LOG_ZOOKEEPER_CONFIG = new InlineLogging();
    public static final InlineLogging LOG_CONNECT_CONFIG = new InlineLogging();
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static WorkerExecutor sharedWorkerExecutor;

    static {
        METRICS_CONFIG.put("foo", "bar");
        LOG_KAFKA_CONFIG.setLoggers(singletonMap("kafka.root.logger.level", "INFO"));
        LOG_ZOOKEEPER_CONFIG.setLoggers(singletonMap("zookeeper.root.logger", "INFO"));
        LOG_CONNECT_CONFIG.setLoggers(singletonMap("connect.root.logger.level", "INFO"));
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
    private static Map<String, Object> zooConfig;
    private static Storage kafkaStorage;
    private static SingleVolumeStorage zkStorage;
    private static EntityOperatorSpec eoConfig;
    private final MockCertManager certManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");

    public static class Params {
        private final boolean openShift;
        private final boolean metrics;
        private final List<GenericKafkaListener> kafkaListeners;
        private final Map<String, Object> kafkaConfig;
        private final Map<String, Object> zooConfig;
        private final Storage kafkaStorage;
        private final SingleVolumeStorage zkStorage;
        private final EntityOperatorSpec eoConfig;

        public Params(boolean openShift, boolean metrics, List<GenericKafkaListener> kafkaListeners, Map<String, Object> kafkaConfig, Map<String, Object> zooConfig, Storage kafkaStorage, SingleVolumeStorage zkStorage, EntityOperatorSpec eoConfig) {
            this.openShift = openShift;
            this.metrics = metrics;
            this.kafkaConfig = kafkaConfig;
            this.kafkaListeners = kafkaListeners;
            this.zooConfig = zooConfig;
            this.kafkaStorage = kafkaStorage;
            this.zkStorage = zkStorage;
            this.eoConfig = eoConfig;
        }

        public String toString() {
            return "openShift=" + openShift +
                    ",metrics=" + metrics +
                    ",kafkaListeners=" + kafkaListeners +
                    ",kafkaConfig=" + kafkaConfig +
                    ",zooConfig=" + zooConfig +
                    ",kafkaStorage=" + kafkaStorage +
                    ",zkStorage=" + zkStorage +
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
            emptyMap(),
            singletonMap("foo", "bar")
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
                        // On Kube, use nodeports
                        listeners.add(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build());
                    }

                    result.add(new Params(metricsOpenShiftAndEntityOperator, metricsOpenShiftAndEntityOperator, listeners, config, config, storage, storage, eoConfig));
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
        zooConfig = params.zooConfig;
        kafkaStorage = params.kafkaStorage;
        zkStorage = params.zkStorage;
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
        createCluster(context, getKafkaAssembly("foo"),
                emptyList());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testCreateClusterWithJmxEnabled(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafka = getKafkaAssembly("foo");
        KafkaJmxOptions jmxOptions = new KafkaJmxOptionsBuilder()
                .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                .build();
        kafka.getSpec().getKafka().setJmxOptions(jmxOptions);
        kafka.getSpec().getZookeeper().setJmxOptions(jmxOptions);
        Secret kafkaJmxSecret = new SecretBuilder()
                .withNewMetadata()
                .withName(KafkaResources.kafkaJmxSecretName("foo"))
                .withNamespace("test")
                .endMetadata()
                .withData(singletonMap("foo", "bar"))
                .build();
        Secret zookeeperJmxSecret = new SecretBuilder()
                .withNewMetadata()
                .withName(KafkaResources.zookeeperJmxSecretName("foo"))
                .withNamespace("test")
                .endMetadata()
                .withData(singletonMap("foo", "bar"))
                .build();
        createCluster(context, kafka, List.of(kafkaJmxSecret, zookeeperJmxSecret));
    }

    private Map<String, PersistentVolumeClaim> createKafkaPvcs(String namespace, Map<String, Storage> storageMap, Set<NodeRef> nodes,
                                                          BiFunction<Integer, Integer, String> pvcNameFunction) {

        Map<String, PersistentVolumeClaim> pvcs = new HashMap<>();

        for (NodeRef node : nodes) {
            Storage storage = storageMap.get(node.poolName());

            if (storage instanceof PersistentClaimStorage) {
                Integer storageId = ((PersistentClaimStorage) storage).getId();
                String pvcName = pvcNameFunction.apply(node.nodeId(), storageId);
                pvcs.put(pvcName, createPvc(namespace, pvcName));
            }
        }

        return pvcs;
    }

    private Map<String, PersistentVolumeClaim> createZooPvcs(String namespace, Storage storage, Set<NodeRef> nodes,
                                                   BiFunction<Integer, Integer, String> pvcNameFunction) {

        Map<String, PersistentVolumeClaim> pvcs = new HashMap<>();
        if (storage instanceof PersistentClaimStorage) {
            for (NodeRef node : nodes) {
                Integer storageId = ((PersistentClaimStorage) storage).getId();
                String pvcName = pvcNameFunction.apply(node.nodeId(), storageId);
                pvcs.put(pvcName, createPvc(namespace, pvcName));
            }

        }
        return pvcs;
    }

    private PersistentVolumeClaim createPvc(String namespace, String pvcName) {
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(pvcName)
                .endMetadata()
                .build();
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity", "checkstyle:JavaNCSS", "checkstyle:MethodLength"})
    private void createCluster(VertxTestContext context, Kafka kafka, List<Secret> secrets) {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, SHARED_ENV_PROVIDER);
        EntityOperator entityOperator = EntityOperator.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, SHARED_ENV_PROVIDER);

        // create CM, Service, headless service, statefulset and so on
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        var mockKafkaOps = supplier.kafkaOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        StatefulSetOperator mockStsOps = supplier.stsOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        PodOperator mockPodOps = supplier.podOperations;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator mockPolicyOps = supplier.networkPolicyOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        RouteOperator mockRouteOps = supplier.routeOperations;
        IngressOperator mockIngressOps = supplier.ingressOperations;
        NodeOperator mockNodeOps = supplier.nodeOperator;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;

        // Create a Kafka CR
        String kafkaName = kafka.getMetadata().getName();
        String kafkaNamespace = kafka.getMetadata().getNamespace();
        when(mockKafkaOps.get(kafkaNamespace, kafkaName)).thenReturn(null);
        when(mockKafkaOps.getAsync(eq(kafkaNamespace), eq(kafkaName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        // Mock PodSets
        AtomicReference<StrimziPodSet> podSetRef = new AtomicReference<>();
        ArgumentCaptor<StrimziPodSet> spsCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.reconcile(any(), eq(kafkaNamespace), eq(KafkaResources.zookeeperComponentName(kafkaName)), spsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new StrimziPodSet())));
        when(mockPodSetOps.reconcile(any(), eq(kafkaNamespace), eq(KafkaResources.kafkaComponentName(kafkaName)), spsCaptor.capture())).thenAnswer(i -> {
            StrimziPodSet sps = new StrimziPodSetBuilder()
                    .withNewMetadata()
                        .withName(kafkaName + "-kafka")
                        .withNamespace(kafkaNamespace)
                        .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, kafkaName)
                    .endMetadata()
                    .withNewSpec()
                        .withPods(PodSetUtils.podsToMaps(List.of(new Pod(), new Pod(), new Pod())))
                    .endSpec()
                    .build();
            podSetRef.set(sps);
            return Future.succeededFuture(ReconcileResult.created(sps));
        });
        when(mockPodSetOps.getAsync(eq(kafkaNamespace), eq(KafkaResources.zookeeperComponentName(kafkaName)))).thenReturn(Future.succeededFuture());
        when(mockPodSetOps.getAsync(eq(kafkaNamespace), eq(KafkaResources.kafkaComponentName(kafkaName)))).thenAnswer(i -> Future.succeededFuture(podSetRef.get()));
        when(mockPodSetOps.batchReconcile(any(), eq(kafkaNamespace), any(), any())).thenCallRealMethod();
        when(mockPodSetOps.listAsync(eq(kafkaNamespace), eq(kafkaCluster.getSelectorLabels()))).thenAnswer(i -> {
            if (podSetRef.get() != null) {
                return Future.succeededFuture(List.of(podSetRef.get()));
            } else {
                return Future.succeededFuture(List.of());
            }
        });

        // Mock StatefulSets
        when(mockStsOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        ArgumentCaptor<NetworkPolicy> policyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPolicyOps.reconcile(any(), anyString(), anyString(), policyCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockPdbOps.reconcile(any(), anyString(), anyString(), pdbCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        // Service mocks
        Set<Service> createdServices = new HashSet<>();
        createdServices.add(kafkaCluster.generateService());
        createdServices.add(kafkaCluster.generateHeadlessService());
        createdServices.addAll(kafkaCluster.generateExternalBootstrapServices());
        createdServices.addAll(kafkaCluster.generatePerPodServices());

        Map<String, Service> expectedServicesMap = createdServices.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));

        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockServiceOps.batchReconcile(any(), eq(kafkaNamespace), any(), any())).thenCallRealMethod();
        when(mockServiceOps.get(eq(kafkaNamespace), anyString())).thenAnswer(i -> Future.succeededFuture(expectedServicesMap.get(i.<String>getArgument(1))));
        when(mockServiceOps.getAsync(eq(kafkaNamespace), anyString())).thenAnswer(i -> {
            Service svc = expectedServicesMap.get(i.<String>getArgument(1));

            if (svc != null && "NodePort".equals(svc.getSpec().getType()))    {
                svc.getSpec().getPorts().get(0).setNodePort(32000);
            }

            return Future.succeededFuture(svc);
        });
        when(mockServiceOps.reconcile(any(), anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Service())));
        when(mockServiceOps.endpointReadiness(any(), anyString(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockServiceOps.listAsync(eq(kafkaNamespace), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Ingress mocks

        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockIngressOps.batchReconcile(any(), eq(kafkaNamespace), any(), any())).thenCallRealMethod();
        when(mockIngressOps.listAsync(eq(kafkaNamespace), any(Labels.class))).thenReturn(
                Future.succeededFuture(emptyList())
        );

        // Route Mocks
        if (openShift) {
            Set<Route> expectedRoutes = new HashSet<>(kafkaCluster.generateExternalBootstrapRoutes());
            expectedRoutes.addAll(kafkaCluster.generateExternalRoutes());

            Map<String, Route> expectedRoutesMap = expectedRoutes.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));

            // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
            when(mockRouteOps.batchReconcile(any(), eq(kafkaNamespace), any(), any())).thenCallRealMethod();
            when(mockRouteOps.get(eq(kafkaNamespace), anyString())).thenAnswer(i -> Future.succeededFuture(expectedRoutesMap.get(i.<String>getArgument(1))));
            when(mockRouteOps.getAsync(eq(kafkaNamespace), anyString())).thenAnswer(i -> {
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
            when(mockRouteOps.listAsync(eq(kafkaNamespace), any(Labels.class))).thenReturn(
                    Future.succeededFuture(emptyList())
            );
        }

        // Mock pod readiness
        when(mockPodOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPodOps.listAsync(anyString(), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock node ops
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        Map<String, PersistentVolumeClaim> zkPvcs = createZooPvcs(kafkaNamespace, zookeeperCluster.getStorage(), zookeeperCluster.nodes(),
            (replica, storageId) -> VolumeUtils.DATA_VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(kafkaName, replica));

        Map<String, PersistentVolumeClaim> kafkaPvcs = createKafkaPvcs(kafkaNamespace, kafkaCluster.getStorageByPoolName(), kafkaCluster.nodes(),
            (replica, storageId) -> {
                String name = VolumeUtils.createVolumePrefix(storageId, false);
                return name + "-" + KafkaResources.kafkaPodName(kafkaName, replica);
            });

        when(mockPvcOps.get(eq(kafkaNamespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(zookeeperCluster.getComponentName())) {
                        return zkPvcs.get(pvcName);
                    } else if (pvcName.contains(kafkaCluster.getComponentName())) {
                        return kafkaPvcs.get(pvcName);
                    }
                    return null;
                });

        when(mockPvcOps.getAsync(eq(kafkaNamespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(zookeeperCluster.getComponentName())) {
                        return Future.succeededFuture(zkPvcs.get(pvcName));
                    } else if (pvcName.contains(kafkaCluster.getComponentName())) {
                        return Future.succeededFuture(kafkaPvcs.get(pvcName));
                    }
                    return Future.succeededFuture(null);
                });

        when(mockPvcOps.listAsync(eq(kafkaNamespace), ArgumentMatchers.any(Labels.class)))
                .thenAnswer(invocation -> Future.succeededFuture(emptyList()));

        Set<String> expectedPvcs = new HashSet<>(zkPvcs.keySet());
        expectedPvcs.addAll(kafkaPvcs.keySet());
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        Set<String> expectedSecrets = set(
                KafkaResources.clientsCaKeySecretName(kafkaName),
                KafkaResources.clientsCaCertificateSecretName(kafkaName),
                KafkaResources.clusterCaCertificateSecretName(kafkaName),
                KafkaResources.clusterCaKeySecretName(kafkaName),
                KafkaResources.kafkaSecretName(kafkaName),
                KafkaResources.zookeeperSecretName(kafkaName),
                KafkaResources.secretName(kafkaName));

        if (metrics)    {
            expectedSecrets.add(KafkaExporterResources.secretName(kafkaName));
        }

        expectedSecrets.addAll(secrets.stream().map(s -> s.getMetadata().getName()).collect(Collectors.toSet()));
        if (eoConfig != null) {
            // it's expected only when the Entity Operator is deployed by the Cluster Operator
            expectedSecrets.add(KafkaResources.entityTopicOperatorSecretName(kafkaName));
            expectedSecrets.add(KafkaResources.entityUserOperatorSecretName(kafkaName));
        }

        when(mockDepOps.reconcile(any(), anyString(), anyString(), any())).thenAnswer(invocation -> {
            String name = invocation.getArgument(2);
            Deployment desired = invocation.getArgument(3);
            if (desired != null) {
                if (name.contains("operator")) {
                    if (entityOperator != null) {
                        context.verify(() -> assertThat(desired.getMetadata().getName(), is(KafkaResources.entityOperatorDeploymentName(kafkaName))));
                    }
                } else if (name.contains("exporter"))   {
                    context.verify(() -> assertThat(metrics, is(true)));
                }
            }
            return Future.succeededFuture(desired != null ? ReconcileResult.created(desired) : ReconcileResult.deleted());
        });
        when(mockDepOps.getAsync(anyString(), anyString())).thenReturn(
                Future.succeededFuture()
        );
        when(mockDepOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                Future.succeededFuture()
        );
        when(mockDepOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                Future.succeededFuture()
        );

        Map<String, Secret> secretsMap = secrets.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));
        when(mockSecretOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(new ArrayList<>(secretsMap.values())));
        when(mockSecretOps.getAsync(anyString(), any())).thenAnswer(i ->
                Future.succeededFuture(secretsMap.get(i.<String>getArgument(1)))
        );
        when(mockSecretOps.getAsync(kafkaNamespace, KafkaResources.clusterCaCertificateSecretName(kafkaName))).thenAnswer(i ->
                Future.succeededFuture(secretsMap.get(i.<String>getArgument(1)))
        );
        when(mockSecretOps.getAsync(kafkaNamespace, KafkaResources.secretName(kafkaName))).thenAnswer(i ->
                Future.succeededFuture(secretsMap.get(i.<String>getArgument(1)))
        );

        when(mockSecretOps.reconcile(any(), anyString(), anyString(), any())).thenAnswer(invocation -> {
            Secret desired = invocation.getArgument(3);
            if (desired != null) {
                secretsMap.put(desired.getMetadata().getName(), desired);
            }
            return Future.succeededFuture(ReconcileResult.created(new Secret()));
        });

        ArgumentCaptor<ConfigMap> metricsCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        ArgumentCaptor<String> metricsNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.reconcile(any(), anyString(), metricsNameCaptor.capture(), metricsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<ConfigMap> logCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        ArgumentCaptor<String> logNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.reconcile(any(), anyString(), logNameCaptor.capture(), logCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        when(mockCmOps.getAsync(kafkaNamespace, metricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.getAsync(kafkaNamespace, differentMetricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.listAsync(kafkaNamespace, kafkaCluster.getSelectorLabels())).thenReturn(Future.succeededFuture(List.of()));
        when(mockCmOps.deleteAsync(any(), any(), any(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Route> routeCaptor = ArgumentCaptor.forClass(Route.class);
        ArgumentCaptor<String> routeNameCaptor = ArgumentCaptor.forClass(String.class);
        if (openShift) {
            when(mockRouteOps.reconcile(any(), eq(kafkaNamespace), routeNameCaptor.capture(), routeCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Route())));
        }

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config
        );

        // Now try to create a KafkaCluster based on this CM
        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, kafkaNamespace, kafkaName), kafka)
            .onComplete(context.succeeding(status -> context.verify(() -> {

                // We expect a headless and headful service
                Set<String> expectedServices = set(
                        KafkaResources.zookeeperHeadlessServiceName(kafkaName),
                        KafkaResources.zookeeperServiceName(kafkaName),
                        KafkaResources.bootstrapServiceName(kafkaName),
                        KafkaResources.brokersServiceName(kafkaName));

                if (kafkaListeners != null) {
                    List<GenericKafkaListener> externalListeners = ListenersUtils.listenersWithOwnServices(kafkaListeners);

                    for (GenericKafkaListener listener : externalListeners) {
                        expectedServices.add(ListenersUtils.backwardsCompatibleBootstrapServiceName(kafkaName, listener));

                        for (NodeRef node : kafkaCluster.nodes()) {
                            expectedServices.add(ListenersUtils.backwardsCompatiblePerBrokerServiceName(kafkaCluster.getComponentName(), node.nodeId(), listener));
                        }
                    }
                }

                List<Service> capturedServices = serviceCaptor.getAllValues();

                assertThat(capturedServices.stream().filter(Objects::nonNull).map(svc -> svc.getMetadata().getName()).collect(Collectors.toSet()).size(),
                        is(expectedServices.size()));
                assertThat(capturedServices.stream().filter(Objects::nonNull).map(svc -> svc.getMetadata().getName()).collect(Collectors.toSet()),
                        is(expectedServices));

                // Assertions on the StrimziPodSets
                List<StrimziPodSet> capturedSps = spsCaptor.getAllValues();
                // We expect a StrimziPodSet for kafka and zookeeper...
                assertThat(capturedSps.stream().map(sps -> sps.getMetadata().getName()).collect(Collectors.toSet()),
                        is(set(KafkaResources.kafkaComponentName(kafkaName), KafkaResources.zookeeperComponentName(kafkaName))));

                // expected Secrets with certificates
                assertThat(new TreeSet<>(secretsMap.keySet()), is(new TreeSet<>(expectedSecrets)));

                // expected secret metrics emitted
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
                assertThat(pdbCaptor.getAllValues(), hasSize(2));
                assertThat(pdbCaptor.getAllValues().stream().map(sts -> sts.getMetadata().getName()).collect(Collectors.toSet()),
                        is(set(KafkaResources.kafkaComponentName(kafkaName), KafkaResources.zookeeperComponentName(kafkaName))));

                // Check PVCs
                assertThat(pvcCaptor.getAllValues(), hasSize(expectedPvcs.size()));
                assertThat(pvcCaptor.getAllValues().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet()),
                        is(expectedPvcs));
                for (PersistentVolumeClaim pvc : pvcCaptor.getAllValues()) {
                    assertThat(pvc.getMetadata().getAnnotations(), hasKey(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM));
                }

                // Verify deleted routes
                if (openShift) {
                    Set<String> expectedRoutes = set(KafkaResources.bootstrapServiceName(kafkaName));

                    for (NodeRef node : kafkaCluster.nodes()) {
                        expectedRoutes.add(node.podName());
                    }

                    assertThat(captured(routeNameCaptor), is(expectedRoutes));
                } else {
                    assertThat(routeNameCaptor.getAllValues(), hasSize(0));
                }

                assertThat(status.getOperatorLastSuccessfulVersion(), is(KafkaAssemblyOperator.OPERATOR_VERSION));

                async.flag();
            })));
    }

    private Kafka getKafkaAssembly(String clusterName) {
        String clusterNamespace = "test";
        int replicas = 3;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;
        KafkaExporterSpec exporter = metrics ? new KafkaExporterSpec() : null;
        String metricsCMName = "metrics-cm";
        JmxPrometheusExporterMetrics jmxMetricsConfig = metrics ? null : io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics("metrics-config.yml", metricsCMName);

        Kafka resource = ResourceUtils.createKafka(clusterNamespace, clusterName, replicas, image, healthDelay, healthTimeout, jmxMetricsConfig, kafkaConfig, zooConfig, kafkaStorage, zkStorage, LOG_KAFKA_CONFIG, LOG_ZOOKEEPER_CONFIG, exporter, null);

        return new KafkaBuilder(resource)
                .editSpec()
                    .editKafka()
                        .withListeners(kafkaListeners)
                    .endKafka()
                    .withEntityOperator(eoConfig)
                .endSpec()
                .build();
    }

    private static <T> Set<T> captured(ArgumentCaptor<T> captor) {
        return new HashSet<>(captor.getAllValues());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterNoop(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaClusterChangeImage(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setImage("a-changed-image");
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZookeeperClusterChangeImage(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setImage("a-changed-image");
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaClusterScaleUp(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setReplicas(4);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaClusterScaleDown(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setReplicas(2);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZookeeperClusterScaleUp(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setReplicas(4);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZookeeperClusterScaleDown(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setReplicas(2);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterAuthenticationTrue(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        KafkaJmxOptions kafkaJmxOptions = new KafkaJmxOptionsBuilder().withAuthentication(
                 new KafkaJmxAuthenticationPasswordBuilder().build())
                .build();
        kafkaAssembly.getSpec().getKafka().setJmxOptions(kafkaJmxOptions);
        kafkaAssembly.getSpec().getZookeeper().setJmxOptions(kafkaJmxOptions);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterLogConfig(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        InlineLogging logger = new InlineLogging();
        logger.setLoggers(singletonMap("kafka.root.logger.level", "DEBUG"));
        kafkaAssembly.getSpec().getKafka().setLogging(logger);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZkClusterMetricsConfig(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics("metrics-config.yml", differentMetricsCMName);
        kafkaAssembly.getSpec().getKafka().setMetricsConfig(jmxMetricsConfig);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZkClusterLogConfig(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        InlineLogging logger = new InlineLogging();
        logger.setLoggers(singletonMap("zookeeper.root.logger", "DEBUG"));
        kafkaAssembly.getSpec().getZookeeper().setLogging(logger);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:JavaNCSS", "checkstyle:MethodLength"})
    private void updateCluster(VertxTestContext context, Kafka originalAssembly, Kafka updatedAssembly) {
        List<KafkaPool> originalPools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, originalAssembly, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster originalKafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, originalAssembly, originalPools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        List<KafkaPool> updatedPools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, updatedAssembly, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster updatedKafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, updatedAssembly, updatedPools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        ZookeeperCluster originalZookeeperCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, originalAssembly, VERSIONS, SHARED_ENV_PROVIDER);
        ZookeeperCluster updatedZookeeperCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, updatedAssembly, VERSIONS, SHARED_ENV_PROVIDER);
        EntityOperator originalEntityOperator = EntityOperator.fromCrd(new Reconciliation("test", originalAssembly.getKind(), originalAssembly.getMetadata().getNamespace(), originalAssembly.getMetadata().getName()), originalAssembly, VERSIONS, SHARED_ENV_PROVIDER);
        KafkaExporter originalKafkaExporter = KafkaExporter.fromCrd(new Reconciliation("test", originalAssembly.getKind(), originalAssembly.getMetadata().getNamespace(), originalAssembly.getMetadata().getName()), originalAssembly, VERSIONS, SHARED_ENV_PROVIDER);
        CruiseControl originalCruiseControl = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, originalAssembly, VERSIONS, originalKafkaCluster.nodes(), Map.of(), Map.of(), SHARED_ENV_PROVIDER);

        // create CM, Service, headless service, statefulset and so on
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        var mockKafkaOps = supplier.kafkaOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        StatefulSetOperator mockStsOps = supplier.stsOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        PodOperator mockPodOps = supplier.podOperations;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator mockPolicyOps = supplier.networkPolicyOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        NodeOperator mockNodeOps = supplier.nodeOperator;
        IngressOperator mockIngressOps = supplier.ingressOperations;
        RouteOperator mockRouteOps = supplier.routeOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;

        String clusterName = updatedAssembly.getMetadata().getName();
        String clusterNamespace = updatedAssembly.getMetadata().getNamespace();

        Map<String, PersistentVolumeClaim> zkPvcs =
                createZooPvcs(clusterNamespace, originalZookeeperCluster.getStorage(), originalZookeeperCluster.nodes(),
                    (replica, storageId) -> VolumeUtils.DATA_VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(clusterName, replica));
        zkPvcs.putAll(createZooPvcs(clusterNamespace, updatedZookeeperCluster.getStorage(), updatedZookeeperCluster.nodes(),
            (replica, storageId) -> VolumeUtils.DATA_VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(clusterName, replica)));

        Map<String, PersistentVolumeClaim> kafkaPvcs =
                createKafkaPvcs(clusterNamespace, originalKafkaCluster.getStorageByPoolName(), originalKafkaCluster.nodes(),
                    (replica, storageId) -> {
                        String name = VolumeUtils.createVolumePrefix(storageId, false);
                        return name + "-" + KafkaResources.kafkaPodName(clusterName, replica);
                    });
        kafkaPvcs.putAll(createKafkaPvcs(clusterNamespace, updatedKafkaCluster.getStorageByPoolName(), updatedKafkaCluster.nodes(),
            (replica, storageId) -> {
                String name = VolumeUtils.createVolumePrefix(storageId, false);
                return name + "-" + KafkaResources.kafkaPodName(clusterName, replica);
            }));

        when(mockPvcOps.get(eq(clusterNamespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(originalZookeeperCluster.getComponentName())) {
                        return zkPvcs.get(pvcName);
                    } else if (pvcName.contains(originalKafkaCluster.getComponentName())) {
                        return kafkaPvcs.get(pvcName);
                    }
                    return null;
                });

        when(mockPvcOps.getAsync(eq(clusterNamespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(originalZookeeperCluster.getComponentName())) {
                        return Future.succeededFuture(zkPvcs.get(pvcName));
                    } else if (pvcName.contains(originalKafkaCluster.getComponentName())) {
                        return Future.succeededFuture(kafkaPvcs.get(pvcName));
                    }
                    return Future.succeededFuture(null);
                });

        when(mockPvcOps.listAsync(eq(clusterNamespace), ArgumentMatchers.any(Labels.class)))
                .thenAnswer(invocation -> {
                    Labels labels = invocation.getArgument(1);
                    if (labels.toMap().get(Labels.STRIMZI_NAME_LABEL).contains("kafka")) {
                        return Future.succeededFuture(new ArrayList<>(kafkaPvcs.values()));
                    } else if (labels.toMap().get(Labels.STRIMZI_NAME_LABEL).contains("zookeeper")) {
                        return Future.succeededFuture(new ArrayList<>(zkPvcs.values()));
                    }
                    return Future.succeededFuture(emptyList());
                });

        when(mockPvcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock Kafka CR get
        when(mockKafkaOps.get(clusterNamespace, clusterName)).thenReturn(updatedAssembly);
        when(mockKafkaOps.getAsync(eq(clusterNamespace), eq(clusterName))).thenReturn(Future.succeededFuture(updatedAssembly));
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        ConfigMap zkMetricsCm = new ConfigMapBuilder().withNewMetadata()
                .withName(KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName))
                .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(MetricsModel.CONFIG_MAP_KEY, TestUtils.toYamlString(METRICS_CONFIG)))
                .build();
        when(mockCmOps.get(clusterNamespace, KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName))).thenReturn(zkMetricsCm);

        ConfigMap logCm = new ConfigMapBuilder().withNewMetadata()
                .withName(KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName))
                .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(LoggingModel.LOG4J1_CONFIG_MAP_KEY,
                        updatedKafkaCluster.logging().loggingConfiguration(Reconciliation.DUMMY_RECONCILIATION, null)))
                .build();
        when(mockCmOps.get(clusterNamespace, KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName))).thenReturn(logCm);

        ConfigMap zklogsCm = new ConfigMapBuilder().withNewMetadata()
                .withName(KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName))
                .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(LoggingModel.LOG4J1_CONFIG_MAP_KEY,
                        updatedZookeeperCluster.logging().loggingConfiguration(Reconciliation.DUMMY_RECONCILIATION, null)))
                .build();
        when(mockCmOps.get(clusterNamespace, KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName))).thenReturn(zklogsCm);
        when(mockCmOps.getAsync(clusterNamespace, metricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.getAsync(clusterNamespace, differentMetricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.listAsync(clusterNamespace, updatedKafkaCluster.getSelectorLabels())).thenReturn(Future.succeededFuture(List.of()));
        when(mockCmOps.deleteAsync(any(), any(), any(), anyBoolean())).thenReturn(Future.succeededFuture());

        // Mock pod ops
        when(mockPodOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPodOps.listAsync(anyString(), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockPodOps.waitFor(any(), eq(clusterNamespace), anyString(), eq("to be deleted"), anyLong(), anyLong(), any())).thenReturn(Future.succeededFuture()); // Needed fot scale-down

        // Mock node ops
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock Service gets
        Set<Service> expectedServices = new HashSet<>();
        expectedServices.add(updatedKafkaCluster.generateService());
        expectedServices.add(updatedKafkaCluster.generateHeadlessService());
        expectedServices.addAll(updatedKafkaCluster.generateExternalBootstrapServices());
        expectedServices.addAll(updatedKafkaCluster.generatePerPodServices());

        Map<String, Service> expectedServicesMap = expectedServices.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));

        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockServiceOps.batchReconcile(any(), eq(clusterNamespace), any(), any())).thenCallRealMethod();
        when(mockServiceOps.endpointReadiness(any(), eq(clusterNamespace), any(), anyLong(), anyLong())).thenReturn(
                Future.succeededFuture()
        );
        when(mockServiceOps.get(eq(clusterNamespace), anyString())).thenAnswer(i -> Future.succeededFuture(expectedServicesMap.get(i.<String>getArgument(1))));
        when(mockServiceOps.getAsync(eq(clusterNamespace), anyString())).thenAnswer(i -> {
            Service svc = expectedServicesMap.get(i.<String>getArgument(1));

            if (svc != null && "NodePort".equals(svc.getSpec().getType()))    {
                svc.getSpec().getPorts().get(0).setNodePort(32000);
            }

            return Future.succeededFuture(svc);
        });
        when(mockServiceOps.listAsync(eq(clusterNamespace), any(Labels.class))).thenReturn(
                Future.succeededFuture(asList(
                        originalKafkaCluster.generateService(),
                        originalKafkaCluster.generateHeadlessService()
                ))
        );
        when(mockServiceOps.hasNodePort(any(), eq(clusterNamespace), any(), anyLong(), anyLong())).thenReturn(
                Future.succeededFuture()
        );

        // Ingress mocks

        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockIngressOps.batchReconcile(any(), eq(clusterNamespace), any(), any())).thenCallRealMethod();
        when(mockIngressOps.listAsync(eq(clusterNamespace), any(Labels.class))).thenReturn(
                Future.succeededFuture(emptyList())
        );

        // Route Mocks
        if (openShift) {
            Set<Route> expectedRoutes = new HashSet<>(originalKafkaCluster.generateExternalBootstrapRoutes());
            // We use the updatedKafkaCluster here to mock the Route status even for the scaled up replicas
            expectedRoutes.addAll(updatedKafkaCluster.generateExternalRoutes());

            Map<String, Route> expectedRoutesMap = expectedRoutes.stream().collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));

            // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
            when(mockRouteOps.batchReconcile(any(), eq(clusterNamespace), any(), any())).thenCallRealMethod();
            when(mockRouteOps.get(eq(clusterNamespace), anyString())).thenAnswer(i -> Future.succeededFuture(expectedRoutesMap.get(i.<String>getArgument(1))));
            when(mockRouteOps.getAsync(eq(clusterNamespace), anyString())).thenAnswer(i -> {
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
            when(mockRouteOps.listAsync(eq(clusterNamespace), any(Labels.class))).thenReturn(
                    Future.succeededFuture(emptyList())
            );
            when(mockRouteOps.hasAddress(any(), eq(clusterNamespace), any(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
            when(mockRouteOps.reconcile(any(), eq(clusterNamespace), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new Route())));
        }

        // Mock Secret gets
        when(mockSecretOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.kafkaJmxSecretName(clusterName))).thenReturn(
                Future.succeededFuture(originalKafkaCluster.jmx().jmxSecret(null))
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.zookeeperJmxSecretName(clusterName))).thenReturn(
                Future.succeededFuture(originalZookeeperCluster.jmx().jmxSecret(null))
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.zookeeperSecretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );

        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.entityTopicOperatorSecretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaExporterResources.secretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.clusterCaCertificateSecretName(clusterName))).thenReturn(
                Future.succeededFuture(new SecretBuilder()
                        .withNewMetadata().withName(KafkaResources.clusterCaCertificateSecretName(clusterName)).endMetadata()
                        .addToData("ca-cert.crt", "cert")
                        .build())
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.secretName(clusterName))).thenReturn(
                Future.succeededFuture(new SecretBuilder()
                        .withNewMetadata().withName(KafkaResources.secretName(clusterName)).endMetadata()
                        .addToData("cluster-operator.key", "key")
                        .addToData("cluster-operator.crt", "cert")
                        .addToData("cluster-operator.p12", "p12")
                        .addToData("cluster-operator.password", "password")
                        .build())
        );
        when(mockSecretOps.getAsync(clusterNamespace, CruiseControlResources.secretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );

        // Mock NetworkPolicy get
        when(mockPolicyOps.get(clusterNamespace, KafkaResources.kafkaNetworkPolicyName(clusterName))).thenReturn(originalKafkaCluster.generateNetworkPolicy(null, null));
        when(mockPolicyOps.get(clusterNamespace, KafkaResources.zookeeperNetworkPolicyName(clusterName))).thenReturn(originalZookeeperCluster.generateNetworkPolicy(null, null));

        // Mock PodDisruptionBudget get
        when(mockPdbOps.get(clusterNamespace, KafkaResources.kafkaComponentName(clusterName))).thenReturn(originalKafkaCluster.generatePodDisruptionBudget());
        when(mockPdbOps.get(clusterNamespace, KafkaResources.zookeeperComponentName(clusterName))).thenReturn(originalZookeeperCluster.generatePodDisruptionBudget());

        // Mock StrimziPodSets
        AtomicReference<StrimziPodSet> zooPodSetRef = new AtomicReference<>();
        zooPodSetRef.set(originalZookeeperCluster.generatePodSet(originalZookeeperCluster.getReplicas(), openShift, null, null, podNum -> Map.of()));
        when(mockPodSetOps.reconcile(any(), eq(clusterNamespace), eq(KafkaResources.zookeeperComponentName(clusterName)), any())).thenAnswer(invocation -> {
            StrimziPodSet sps = invocation.getArgument(3, StrimziPodSet.class);
            zooPodSetRef.set(sps);
            return Future.succeededFuture(ReconcileResult.patched(sps));
        });
        when(mockPodSetOps.getAsync(eq(clusterNamespace), eq(KafkaResources.zookeeperComponentName(clusterName)))).thenReturn(Future.succeededFuture(zooPodSetRef.get()));

        AtomicReference<StrimziPodSet> kafkaPodSetRef = new AtomicReference<>();
        kafkaPodSetRef.set(originalKafkaCluster.generatePodSets(openShift, null, null, (p) -> Map.of()).get(0));
        when(mockPodSetOps.reconcile(any(), eq(clusterNamespace), eq(KafkaResources.kafkaComponentName(clusterName)), any())).thenAnswer(invocation -> {
            StrimziPodSet sps = invocation.getArgument(3, StrimziPodSet.class);
            kafkaPodSetRef.set(sps);
            return Future.succeededFuture(ReconcileResult.patched(sps));
        });
        when(mockPodSetOps.getAsync(eq(clusterNamespace), eq(KafkaResources.kafkaComponentName(clusterName)))).thenReturn(Future.succeededFuture(kafkaPodSetRef.get()));
        when(mockPodSetOps.batchReconcile(any(), eq(clusterNamespace), any(), any())).thenCallRealMethod();
        when(mockPodSetOps.listAsync(eq(clusterNamespace), eq(updatedKafkaCluster.getSelectorLabels()))).thenAnswer(i -> {
            if (kafkaPodSetRef.get() != null) {
                return Future.succeededFuture(List.of(kafkaPodSetRef.get()));
            } else {
                return Future.succeededFuture(List.of());
            }
        });

        // Mock StatefulSet get
        when(mockStsOps.getAsync(eq(clusterNamespace), eq(KafkaResources.zookeeperComponentName(clusterName)))).thenReturn(Future.succeededFuture());
        when(mockStsOps.getAsync(eq(clusterNamespace), eq(KafkaResources.kafkaComponentName(clusterName)))).thenReturn(Future.succeededFuture());

        // Mock Deployment get
        if (originalEntityOperator != null) {
            when(mockDepOps.get(clusterNamespace, KafkaResources.entityOperatorDeploymentName(clusterName))).thenReturn(
                    originalEntityOperator.generateDeployment(true, null, null)
            );
            when(mockDepOps.getAsync(clusterNamespace, KafkaResources.entityOperatorDeploymentName(clusterName))).thenReturn(
                    Future.succeededFuture(originalEntityOperator.generateDeployment(true, null, null))
            );
            when(mockDepOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
            when(mockDepOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
        }

        if (originalCruiseControl != null) {
            when(mockDepOps.get(clusterNamespace, CruiseControlResources.componentName(clusterName))).thenReturn(
                    originalCruiseControl.generateDeployment(Map.of(), true, null, null)
            );
            when(mockDepOps.getAsync(clusterNamespace, KafkaResources.entityOperatorDeploymentName(clusterName))).thenReturn(
                    Future.succeededFuture(originalCruiseControl.generateDeployment(Map.of(), true, null, null))
            );
            when(mockDepOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
            when(mockDepOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
        }

        if (metrics) {
            when(mockDepOps.get(clusterNamespace, KafkaExporterResources.componentName(clusterName))).thenReturn(
                    originalKafkaExporter.generateDeployment(true, null, null)
            );
            when(mockDepOps.getAsync(clusterNamespace, KafkaExporterResources.componentName(clusterName))).thenReturn(
                    Future.succeededFuture(originalKafkaExporter.generateDeployment(true, null, null))
            );
            when(mockDepOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
            when(mockDepOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
        }

        // Mock CM patch
        Set<String> metricsCms = set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(any(), eq(clusterNamespace), any(), any());

        Set<String> logCms = set();
        doAnswer(invocation -> {
            logCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(any(), eq(clusterNamespace), any(), any());

        // Mock Service patch (both service and headless service
        ArgumentCaptor<String> patchedServicesCaptor = ArgumentCaptor.forClass(String.class);
        when(mockServiceOps.reconcile(any(), eq(clusterNamespace), patchedServicesCaptor.capture(), any())).thenReturn(Future.succeededFuture(ReconcileResult.patched(new Service())));
        // Mock Secrets patch
        when(mockSecretOps.reconcile(any(), eq(clusterNamespace), any(), any())).thenReturn(Future.succeededFuture());

        // Mock NetworkPolicy patch
        when(mockPolicyOps.reconcile(any(), eq(clusterNamespace), any(), any())).thenReturn(Future.succeededFuture());

        // Mock PodDisruptionBudget patch
        when(mockPdbOps.reconcile(any(), eq(clusterNamespace), any(), any())).thenReturn(Future.succeededFuture());

        // Mock Deployment patch
        ArgumentCaptor<String> depCaptor = ArgumentCaptor.forClass(String.class);
        when(mockDepOps.reconcile(any(), anyString(), depCaptor.capture(), any())).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config
        );

        // Mock broker scale down operation
        BrokersInUseCheck operations = supplier.brokersInUseCheck;
        when(operations.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of()));

        // Now try to update a KafkaCluster based on this CM
        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, clusterNamespace, clusterName),
                updatedAssembly)
            .onComplete(context.succeeding(status -> context.verify(() -> {
                // Check that ZK scale-up happens when it should
                assertThat(zooPodSetRef.get().getSpec().getPods().size(), is(updatedAssembly.getSpec().getZookeeper().getReplicas()));

                assertThat(status.getOperatorLastSuccessfulVersion(), is(KafkaAssemblyOperator.OPERATOR_VERSION));
                assertThat(status.getKafkaVersion(), is(VERSIONS.defaultVersion().version()));

                async.flag();
            })));
    }

    @ParameterizedTest
    @MethodSource("data")
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    public void testReconcile(Params params, VertxTestContext context) {
        //Must create all checkpoints before flagging any, as not doing so can lead to premature test success
        Checkpoint fooAsync = context.checkpoint();
        Checkpoint barAsync = context.checkpoint();
        Checkpoint completeTest = context.checkpoint();

        setFields(params);

        // create CRs
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        var mockKafkaOps = supplier.kafkaOperator;
        String kafkaNamespace = "test";

        Kafka foo = getKafkaAssembly("foo");
        Kafka bar = getKafkaAssembly("bar");
        when(mockKafkaOps.listAsync(eq(kafkaNamespace), isNull(LabelSelector.class))).thenReturn(
            Future.succeededFuture(asList(foo, bar))
        );
        // when requested Custom Resource for a specific Kafka cluster
        when(mockKafkaOps.get(eq(kafkaNamespace), eq("foo"))).thenReturn(foo);
        when(mockKafkaOps.get(eq(kafkaNamespace), eq("bar"))).thenReturn(bar);
        when(mockKafkaOps.getAsync(eq(kafkaNamespace), eq("foo"))).thenReturn(Future.succeededFuture(foo));
        when(mockKafkaOps.getAsync(eq(kafkaNamespace), eq("bar"))).thenReturn(Future.succeededFuture(bar));
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config) {
            @Override
            public Future<KafkaStatus> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
                String name = kafkaAssembly.getMetadata().getName();
                if ("foo".equals(name)) {
                    fooAsync.flag();
                } else if ("bar".equals(name)) {
                    barAsync.flag();
                } else {
                    context.failNow(new AssertionError("Unexpected name " + name));
                }
                return Future.succeededFuture();
            }
        };


        // Now try to reconcile all the Kafka clusters
        ops.reconcileAll("test", kafkaNamespace, context.succeeding(v -> completeTest.flag()));
    }

    @ParameterizedTest
    @MethodSource("data")
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    public void testReconcileAllNamespaces(Params params, VertxTestContext context) {
        setFields(params);

        // create CRs
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        var mockKafkaOps = supplier.kafkaOperator;

        Kafka foo = getKafkaAssembly("foo");
        foo.getMetadata().setNamespace("namespace1");
        Kafka bar = getKafkaAssembly("bar");
        bar.getMetadata().setNamespace("namespace2");
        when(mockKafkaOps.listAsync(eq("*"), isNull(LabelSelector.class))).thenReturn(
                Future.succeededFuture(asList(foo, bar))
        );
        // when requested Custom Resource for a specific Kafka cluster
        when(mockKafkaOps.get(eq("namespace1"), eq("foo"))).thenReturn(foo);
        when(mockKafkaOps.get(eq("namespace2"), eq("bar"))).thenReturn(bar);
        when(mockKafkaOps.getAsync(eq("namespace1"), eq("foo"))).thenReturn(Future.succeededFuture(foo));
        when(mockKafkaOps.getAsync(eq("namespace2"), eq("bar"))).thenReturn(Future.succeededFuture(bar));
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        Checkpoint fooAsync = context.checkpoint();
        Checkpoint barAsync = context.checkpoint();
        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config) {
            @Override
            public Future<KafkaStatus> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
                String name = kafkaAssembly.getMetadata().getName();
                if ("foo".equals(name)) {
                    fooAsync.flag();
                } else if ("bar".equals(name)) {
                    barAsync.flag();
                } else {
                    context.failNow(new AssertionError("Unexpected name " + name));
                }
                return Future.succeededFuture();
            }
        };

        Checkpoint async = context.checkpoint();
        // Now try to reconcile all the Kafka clusters
        ops.reconcileAll("test", "*", context.succeeding(v -> async.flag()));
    }

}
