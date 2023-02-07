/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
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
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxTransResources;
import io.strimzi.api.kafka.model.JmxTransSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptions;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplateBuilder;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplateBuilder;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_21;

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

    @ParameterizedTest
    @MethodSource("data")
    @SuppressWarnings("deprecation") // Suppress deprecated JMX Trans
    public void testCreateClusterWithJmxTrans(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafka = getKafkaAssembly("foo");
        kafka.getSpec()
                .getKafka().setJmxOptions(new KafkaJmxOptionsBuilder()
                .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                .build());

        kafka.getSpec().setJmxTrans(new JmxTransSpecBuilder()
                .withKafkaQueries(new JmxTransQueryTemplateBuilder()
                        .withTargetMBean("mbean")
                        .withAttributes("attribute")
                        .withOutputs("output")
                        .build())
                .withOutputDefinitions(new JmxTransOutputDefinitionTemplateBuilder()
                        .withOutputType("host")
                        .withName("output")
                        .build())
                .build());

        createCluster(context, kafka, Collections.singletonList(new SecretBuilder()
                .withNewMetadata()
                .withName(KafkaResources.kafkaJmxSecretName("foo"))
                .withNamespace("test")
                .endMetadata()
                .withData(Collections.singletonMap("foo", "bar"))
                .build()
        ));
    }

    private Map<String, PersistentVolumeClaim> createPvcs(String namespace, Storage storage, int replicas,
                                                   BiFunction<Integer, Integer, String> pvcNameFunction) {

        Map<String, PersistentVolumeClaim> pvcs = new HashMap<>();
        if (storage instanceof PersistentClaimStorage) {

            for (int i = 0; i < replicas; i++) {
                Integer storageId = ((PersistentClaimStorage) storage).getId();
                String pvcName = pvcNameFunction.apply(i, storageId);
                PersistentVolumeClaim pvc =
                        new PersistentVolumeClaimBuilder()
                                .withNewMetadata()
                                .withNamespace(namespace)
                                .withName(pvcName)
                                .endMetadata()
                                .build();
                pvcs.put(pvcName, pvc);
            }

        }
        return pvcs;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity", "checkstyle:JavaNCSS", "checkstyle:MethodLength"})
    private void createCluster(VertxTestContext context, Kafka kafka, List<Secret> secrets) {
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
        EntityOperator entityOperator = EntityOperator.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS, true);

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
        when(mockPodSetOps.reconcile(any(), eq(kafkaNamespace), eq(KafkaResources.zookeeperStatefulSetName(kafkaName)), spsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new StrimziPodSet())));
        when(mockPodSetOps.reconcile(any(), eq(kafkaNamespace), eq(KafkaResources.kafkaStatefulSetName(kafkaName)), spsCaptor.capture())).thenAnswer(i -> {
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
        when(mockPodSetOps.getAsync(eq(kafkaNamespace), eq(KafkaResources.zookeeperStatefulSetName(kafkaName)))).thenReturn(Future.succeededFuture());
        when(mockPodSetOps.getAsync(eq(kafkaNamespace), eq(KafkaResources.kafkaStatefulSetName(kafkaName)))).thenAnswer(i -> Future.succeededFuture(podSetRef.get()));

        // Mock StatefulSets
        when(mockStsOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));
        when(mockStsOps.deleteAsync(any(), eq(kafkaNamespace), eq(KafkaResources.zookeeperStatefulSetName(kafkaName)), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockStsOps.deleteAsync(any(), eq(kafkaNamespace), eq(KafkaResources.kafkaStatefulSetName(kafkaName)), anyBoolean())).thenReturn(Future.succeededFuture());

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

        int replicas = kafkaCluster.getReplicas();
        for (int i = 0; i < replicas; i++) {
            createdServices.addAll(kafkaCluster.generateExternalServices(i));
        }

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
            for (int i = 0; i < replicas; i++) {
                expectedRoutes.addAll(kafkaCluster.generateExternalRoutes(i));
            }

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

        Map<String, PersistentVolumeClaim> zkPvcs = createPvcs(kafkaNamespace, zookeeperCluster.getStorage(), zookeeperCluster.getReplicas(),
            (replica, storageId) -> AbstractModel.VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(kafkaName, replica));

        Map<String, PersistentVolumeClaim> kafkaPvcs = createPvcs(kafkaNamespace, kafkaCluster.getStorage(), kafkaCluster.getReplicas(),
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
                .thenAnswer(invocation -> Future.succeededFuture(Collections.EMPTY_LIST));

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
                ClusterOperator.secretName(kafkaName));

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

        when(mockSecretOps.list(anyString(), any())).thenAnswer(i ->
                new ArrayList<>(secretsMap.values())
        );
        when(mockSecretOps.getAsync(anyString(), any())).thenAnswer(i ->
                Future.succeededFuture(secretsMap.get(i.<String>getArgument(1)))
        );
        when(mockSecretOps.getAsync(kafkaNamespace, KafkaResources.clusterCaCertificateSecretName(kafkaName))).thenAnswer(i ->
                Future.succeededFuture(secretsMap.get(i.<String>getArgument(1)))
        );
        when(mockSecretOps.getAsync(kafkaNamespace, ClusterOperator.secretName(kafkaName))).thenAnswer(i ->
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

        ConfigMap metricsCm = kafkaCluster.generateSharedConfigurationConfigMap(new MetricsAndLogging(metricsCM, null), Map.of(), Map.of());
        when(mockCmOps.getAsync(kafkaNamespace, KafkaResources.kafkaMetricsAndLogConfigMapName(kafkaName))).thenReturn(Future.succeededFuture(metricsCm));
        when(mockCmOps.getAsync(kafkaNamespace, metricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.getAsync(kafkaNamespace, differentMetricsCMName)).thenReturn(Future.succeededFuture(metricsCM));
        when(mockCmOps.getAsync(anyString(), eq(JmxTransResources.configMapName(kafkaName)))).thenReturn(
            Future.succeededFuture(new ConfigMapBuilder()
                    .withNewMetadata().withResourceVersion("123").endMetadata()
                    .build())
        );
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
            .onComplete(context.succeeding(v -> context.verify(() -> {

                // We expect a headless and headful service
                Set<String> expectedServices = set(
                        KafkaResources.zookeeperHeadlessServiceName(kafkaName),
                        KafkaResources.zookeeperServiceName(kafkaName),
                        KafkaResources.bootstrapServiceName(kafkaName),
                        KafkaResources.brokersServiceName(kafkaName));

                if (kafkaListeners != null) {
                    List<GenericKafkaListener> externalListeners = ListenersUtils.externalListeners(kafkaListeners);

                    for (GenericKafkaListener listener : externalListeners) {
                        expectedServices.add(ListenersUtils.backwardsCompatibleBootstrapServiceName(kafkaName, listener));

                        for (int i = 0; i < kafkaCluster.getReplicas(); i++) {
                            expectedServices.add(ListenersUtils.backwardsCompatibleBrokerServiceName(kafkaName, i, listener));
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
                        is(set(KafkaResources.kafkaStatefulSetName(kafkaName), KafkaResources.zookeeperStatefulSetName(kafkaName))));

                // expected Secrets with certificates
                assertThat(new TreeSet<>(secretsMap.keySet()), is(new TreeSet<>(expectedSecrets)));

                // Check PDBs
                assertThat(pdbCaptor.getAllValues(), hasSize(2));
                assertThat(pdbCaptor.getAllValues().stream().map(sts -> sts.getMetadata().getName()).collect(Collectors.toSet()),
                        is(set(KafkaResources.kafkaStatefulSetName(kafkaName), KafkaResources.zookeeperStatefulSetName(kafkaName))));

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

                    for (int i = 0; i < kafkaCluster.getReplicas(); i++)    {
                        expectedRoutes.add(KafkaResources.kafkaStatefulSetName(kafkaName) + "-" + i);
                    }

                    assertThat(captured(routeNameCaptor), is(expectedRoutes));
                } else {
                    assertThat(routeNameCaptor.getAllValues(), hasSize(0));
                }

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
        KafkaCluster originalKafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, originalAssembly, VERSIONS);
        KafkaCluster updatedKafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, updatedAssembly, VERSIONS);
        ZookeeperCluster originalZookeeperCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, originalAssembly, VERSIONS);
        ZookeeperCluster updatedZookeeperCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, updatedAssembly, VERSIONS);
        EntityOperator originalEntityOperator = EntityOperator.fromCrd(new Reconciliation("test", originalAssembly.getKind(), originalAssembly.getMetadata().getNamespace(), originalAssembly.getMetadata().getName()), originalAssembly, VERSIONS, true);
        KafkaExporter originalKafkaExporter = KafkaExporter.fromCrd(new Reconciliation("test", originalAssembly.getKind(), originalAssembly.getMetadata().getNamespace(), originalAssembly.getMetadata().getName()), originalAssembly, VERSIONS);
        CruiseControl originalCruiseControl = CruiseControl.fromCrd(Reconciliation.DUMMY_RECONCILIATION, originalAssembly, VERSIONS, updatedKafkaCluster.getStorage());

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
                createPvcs(clusterNamespace, originalZookeeperCluster.getStorage(), originalZookeeperCluster.getReplicas(),
                    (replica, storageId) -> AbstractModel.VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(clusterName, replica));
        zkPvcs.putAll(createPvcs(clusterNamespace, updatedZookeeperCluster.getStorage(), updatedZookeeperCluster.getReplicas(),
            (replica, storageId) -> AbstractModel.VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(clusterName, replica)));

        Map<String, PersistentVolumeClaim> kafkaPvcs =
                createPvcs(clusterNamespace, originalKafkaCluster.getStorage(), originalKafkaCluster.getReplicas(),
                    (replica, storageId) -> {
                        String name = VolumeUtils.createVolumePrefix(storageId, false);
                        return name + "-" + KafkaResources.kafkaPodName(clusterName, replica);
                    });
        kafkaPvcs.putAll(createPvcs(clusterNamespace, updatedKafkaCluster.getStorage(), updatedKafkaCluster.getReplicas(),
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
                    return Future.succeededFuture(Collections.EMPTY_LIST);
                });

        when(mockPvcOps.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock Kafka CR get
        when(mockKafkaOps.get(clusterNamespace, clusterName)).thenReturn(updatedAssembly);
        when(mockKafkaOps.getAsync(eq(clusterNamespace), eq(clusterName))).thenReturn(Future.succeededFuture(updatedAssembly));
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        ConfigMap metricsCm = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName("metrics-cm")
                .endMetadata()
                .withData(singletonMap("metrics-config.yml", ""))
                .build();
        ConfigMap metricsAndLoggingCm = originalKafkaCluster.generateSharedConfigurationConfigMap(new MetricsAndLogging(metricsCm, null), Map.of(), Map.of());
        when(mockCmOps.get(clusterNamespace, KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName))).thenReturn(metricsAndLoggingCm);
        when(mockCmOps.getAsync(clusterNamespace, KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName))).thenReturn(Future.succeededFuture(metricsAndLoggingCm));

        ConfigMap zkMetricsCm = new ConfigMapBuilder().withNewMetadata()
                .withName(KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName))
                .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, TestUtils.toYamlString(METRICS_CONFIG)))
                .build();
        when(mockCmOps.get(clusterNamespace, KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName))).thenReturn(zkMetricsCm);

        ConfigMap logCm = new ConfigMapBuilder().withNewMetadata()
                .withName(KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName))
                .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG,
                        updatedKafkaCluster.loggingConfiguration(null)))
                .build();
        when(mockCmOps.get(clusterNamespace, KafkaResources.kafkaMetricsAndLogConfigMapName(clusterName))).thenReturn(logCm);

        ConfigMap zklogsCm = new ConfigMapBuilder().withNewMetadata()
                .withName(KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName))
                .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG,
                        updatedZookeeperCluster.loggingConfiguration(null)))
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

        int replicas = updatedKafkaCluster.getReplicas();
        for (int i = 0; i < replicas; i++) {
            expectedServices.addAll(updatedKafkaCluster.generateExternalServices(i));
        }

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
            for (int i = 0; i < replicas; i++) {
                expectedRoutes.addAll(originalKafkaCluster.generateExternalRoutes(i));
            }

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
        }

        // Mock Secret gets
        when(mockSecretOps.list(anyString(), any())).thenReturn(
                emptyList()
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.kafkaJmxSecretName(clusterName))).thenReturn(
                Future.succeededFuture(originalKafkaCluster.generateJmxSecret(null))
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.zookeeperJmxSecretName(clusterName))).thenReturn(
                Future.succeededFuture(originalZookeeperCluster.generateJmxSecret(null))
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.zookeeperSecretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.kafkaSecretName(clusterName))).thenReturn(
                Future.succeededFuture(ResourceUtils.createMockBrokersCertsSecret(clusterNamespace,
                        clusterName,
                        updatedKafkaCluster.getReplicas(),
                        KafkaResources.kafkaSecretName(clusterName),
                        MockCertManager.serverCert(),
                        MockCertManager.serverKey(),
                        MockCertManager.serverKeyStore(),
                        MockCertManager.certStorePassword()
                ))
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.entityTopicOperatorSecretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaExporterResources.secretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaResources.clusterCaCertificateSecretName(clusterName))).thenReturn(
                Future.succeededFuture(new Secret())
        );
        when(mockSecretOps.getAsync(clusterNamespace, ClusterOperator.secretName(clusterName))).thenReturn(
                Future.succeededFuture(new Secret())
        );
        when(mockSecretOps.getAsync(clusterNamespace, CruiseControlResources.secretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );

        // Mock NetworkPolicy get
        when(mockPolicyOps.get(clusterNamespace, KafkaResources.kafkaNetworkPolicyName(clusterName))).thenReturn(originalKafkaCluster.generateNetworkPolicy(null, null));
        when(mockPolicyOps.get(clusterNamespace, KafkaResources.zookeeperNetworkPolicyName(clusterName))).thenReturn(originalZookeeperCluster.generateNetworkPolicy(null, null));

        // Mock PodDisruptionBudget get
        when(mockPdbOps.get(clusterNamespace, KafkaResources.kafkaStatefulSetName(clusterName))).thenReturn(originalKafkaCluster.generatePodDisruptionBudget(false));
        when(mockPdbOps.get(clusterNamespace, KafkaResources.zookeeperStatefulSetName(clusterName))).thenReturn(originalZookeeperCluster.generatePodDisruptionBudget(false));

        // Mock StrimziPodSets
        AtomicReference<StrimziPodSet> zooPodSetRef = new AtomicReference<>();
        zooPodSetRef.set(originalZookeeperCluster.generatePodSet(originalZookeeperCluster.getReplicas(), openShift, null, null, Map.of()));
        when(mockPodSetOps.reconcile(any(), eq(clusterNamespace), eq(KafkaResources.zookeeperStatefulSetName(clusterName)), any())).thenAnswer(invocation -> {
            StrimziPodSet sps = invocation.getArgument(3, StrimziPodSet.class);
            zooPodSetRef.set(sps);
            return Future.succeededFuture(ReconcileResult.patched(sps));
        });
        when(mockPodSetOps.getAsync(eq(clusterNamespace), eq(KafkaResources.zookeeperStatefulSetName(clusterName)))).thenReturn(Future.succeededFuture(zooPodSetRef.get()));

        AtomicReference<StrimziPodSet> kafkaPodSetRef = new AtomicReference<>();
        kafkaPodSetRef.set(originalKafkaCluster.generatePodSet(originalKafkaCluster.getReplicas(), openShift, null, null, (p) -> Map.of()));
        when(mockPodSetOps.reconcile(any(), eq(clusterNamespace), eq(KafkaResources.kafkaStatefulSetName(clusterName)), any())).thenAnswer(invocation -> {
            StrimziPodSet sps = invocation.getArgument(3, StrimziPodSet.class);
            kafkaPodSetRef.set(sps);
            return Future.succeededFuture(ReconcileResult.patched(sps));
        });
        when(mockPodSetOps.getAsync(eq(clusterNamespace), eq(KafkaResources.kafkaStatefulSetName(clusterName)))).thenReturn(Future.succeededFuture(kafkaPodSetRef.get()));

        // Mock StatefulSet get
        when(mockStsOps.deleteAsync(any(), eq(clusterNamespace), eq(KafkaResources.zookeeperStatefulSetName(clusterName)), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockStsOps.deleteAsync(any(), eq(clusterNamespace), eq(KafkaResources.kafkaStatefulSetName(clusterName)), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockStsOps.getAsync(eq(clusterNamespace), eq(KafkaResources.zookeeperStatefulSetName(clusterName)))).thenReturn(Future.succeededFuture());
        when(mockStsOps.getAsync(eq(clusterNamespace), eq(KafkaResources.kafkaStatefulSetName(clusterName)))).thenReturn(Future.succeededFuture());

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
            when(mockDepOps.get(clusterNamespace, CruiseControlResources.deploymentName(clusterName))).thenReturn(
                    originalCruiseControl.generateDeployment(true, null, null)
            );
            when(mockDepOps.getAsync(clusterNamespace, KafkaResources.entityOperatorDeploymentName(clusterName))).thenReturn(
                    Future.succeededFuture(originalCruiseControl.generateDeployment(true, null, null))
            );
            when(mockDepOps.waitForObserved(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
            when(mockDepOps.readiness(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
        }

        if (metrics) {
            when(mockDepOps.get(clusterNamespace, KafkaExporterResources.deploymentName(clusterName))).thenReturn(
                    originalKafkaExporter.generateDeployment(true, null, null)
            );
            when(mockDepOps.getAsync(clusterNamespace, KafkaExporterResources.deploymentName(clusterName))).thenReturn(
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
        when(mockServiceOps.reconcile(any(), eq(clusterNamespace), patchedServicesCaptor.capture(), any())).thenReturn(Future.succeededFuture());
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

        // Now try to update a KafkaCluster based on this CM
        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, clusterNamespace, clusterName),
                updatedAssembly)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                // Check that ZK scale-up happens when it should
                assertThat(zooPodSetRef.get().getSpec().getPods().size(), is(updatedAssembly.getSpec().getZookeeper().getReplicas()));

                async.flag();
            })));
    }

    @SuppressWarnings("unchecked")
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
        when(mockKafkaOps.listAsync(eq(kafkaNamespace), any(Optional.class))).thenReturn(
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

    @SuppressWarnings("unchecked")
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
        when(mockKafkaOps.listAsync(eq("*"), any(Optional.class))).thenReturn(
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
