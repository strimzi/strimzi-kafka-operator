/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxTransSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptions;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.TopicOperatorSpec;
import io.strimzi.api.kafka.model.TopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.listener.KafkaListenersBuilder;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplateBuilder;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplateBuilder;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.JmxTrans;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaExporter;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetDiff;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorTest {

    public static final Map<String, Object> METRICS_CONFIG = singletonMap("foo", "bar");
    public static final InlineLogging LOG_KAFKA_CONFIG = new InlineLogging();
    public static final InlineLogging LOG_ZOOKEEPER_CONFIG = new InlineLogging();
    public static final InlineLogging LOG_CONNECT_CONFIG = new InlineLogging();
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    static {
        LOG_KAFKA_CONFIG.setLoggers(singletonMap("kafka.root.logger.level", "INFO"));
        LOG_ZOOKEEPER_CONFIG.setLoggers(singletonMap("zookeeper.root.logger", "INFO"));
        LOG_CONNECT_CONFIG.setLoggers(singletonMap("connect.root.logger.level", "INFO"));
    }

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_9;

    private static boolean openShift;
    private static boolean metrics;
    private static KafkaListeners kafkaListeners;
    private static Map<String, Object> kafkaConfig;
    private static Map<String, Object> zooConfig;
    private static Storage kafkaStorage;
    private static SingleVolumeStorage zkStorage;
    private static TopicOperatorSpec toConfig;
    private static EntityOperatorSpec eoConfig;
    private static MockCertManager certManager = new MockCertManager();
    private static PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");

    public static class Params {
        private final boolean openShift;
        private final boolean metrics;
        private final KafkaListeners kafkaListeners;
        private final Map<String, Object> kafkaConfig;
        private final Map<String, Object> zooConfig;
        private final Storage kafkaStorage;
        private final SingleVolumeStorage zkStorage;
        private final TopicOperatorSpec toConfig;
        private final EntityOperatorSpec eoConfig;

        public Params(boolean openShift, boolean metrics, KafkaListeners kafkaListeners, Map<String, Object> kafkaConfig, Map<String, Object> zooConfig, Storage kafkaStorage, SingleVolumeStorage zkStorage, TopicOperatorSpec toConfig, EntityOperatorSpec eoConfig) {
            this.openShift = openShift;
            this.metrics = metrics;
            this.kafkaConfig = kafkaConfig;
            this.kafkaListeners = kafkaListeners;
            this.zooConfig = zooConfig;
            this.kafkaStorage = kafkaStorage;
            this.zkStorage = zkStorage;
            this.toConfig = toConfig;
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
                    ",toConfig=" + toConfig +
                    ",eoConfig=" + eoConfig;
        }
    }

    public static Iterable<Params> data() {
        boolean[] shiftiness = {true, false};
        boolean[] metrics = {true, false};
        Storage[] kafkaStorageConfigs = {
            new EphemeralStorage(),
            new PersistentClaimStorageBuilder()
                    .withSize("123")
                    .withStorageClass("foo")
                    .withDeleteClaim(true)
                .build()
        };
        SingleVolumeStorage[] zkStorageConfigs = {
            new EphemeralStorage(),
            new PersistentClaimStorageBuilder()
                    .withSize("123")
                    .withStorageClass("foo")
                    .withDeleteClaim(true)
                    .build()
        };
        Map[] kafkaConfigs = {
            null,
            emptyMap(),
            singletonMap("foo", "bar")
        };
        Map[] zooConfigs = {
            null,
            emptyMap(),
            singletonMap("foo", "bar")
        };
        TopicOperatorSpec[] toConfigs = {
            null,
            new TopicOperatorSpec(),
            new TopicOperatorSpecBuilder().withReconciliationIntervalSeconds(600)
                    .withZookeeperSessionTimeoutSeconds(10).build()
        };
        EntityOperatorSpec[] eoConfigs = {
            null,
            new EntityOperatorSpecBuilder()
                    .withUserOperator(new EntityUserOperatorSpecBuilder().build())
                    .withTopicOperator(new EntityTopicOperatorSpecBuilder().build())
                    .build()
        };
        List<Params> result = new ArrayList();
        for (boolean shift: shiftiness) {
            for (boolean metric: metrics) {
                for (Map kafkaConfig : kafkaConfigs) {
                    for (Map zooConfig : zooConfigs) {
                        for (Storage kafkaStorage : kafkaStorageConfigs) {
                            for (SingleVolumeStorage zkStorage : zkStorageConfigs) {
                                for (TopicOperatorSpec toConfig : toConfigs) {
                                    for (EntityOperatorSpec eoConfig : eoConfigs) {
                                        KafkaListeners listeners;
                                        if (shift) {
                                            listeners = new KafkaListenersBuilder()
                                                    .withNewPlain()
                                                    .withNewKafkaListenerAuthenticationScramSha512Auth()
                                                    .endKafkaListenerAuthenticationScramSha512Auth()
                                                    .endPlain()
                                                    .withNewTls()
                                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerAuthenticationTlsAuth()
                                                    .endTls()
                                                    .withNewKafkaListenerExternalRoute()
                                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerExternalRoute()
                                                    .build();
                                        } else {
                                            listeners = new KafkaListenersBuilder()
                                                    .withNewPlain()
                                                    .withNewKafkaListenerAuthenticationScramSha512Auth()
                                                    .endKafkaListenerAuthenticationScramSha512Auth()
                                                    .endPlain()
                                                    .withNewTls()
                                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerAuthenticationTlsAuth()
                                                    .endTls()
                                                    .withNewKafkaListenerExternalNodePort()
                                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerExternalNodePort()
                                                    .build();
                                        }

                                        // TO and EO cannot be deployed together so no need for testing this case
                                        if (!(toConfig != null && eoConfig != null)) {
                                            result.add(new Params(shift, metric, listeners, kafkaConfig, zooConfig, kafkaStorage, zkStorage, toConfig, eoConfig));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * Find the first secret in the given secrets with the given name.
     * @param secrets The secrets to search.
     * @param sname The secret name.
     * @return The secret with that name.
     */
    public static Secret findSecretWithName(List<Secret> secrets, String sname) {
        return secrets.stream().filter(s -> s.getMetadata().getName().equals(sname)).findFirst().orElse(null);
    }

    public static void setFields(Params params) {
        openShift = params.openShift;
        metrics = params.metrics;
        kafkaListeners = params.kafkaListeners;
        kafkaConfig = params.kafkaConfig;
        zooConfig = params.zooConfig;
        kafkaStorage = params.kafkaStorage;
        zkStorage = params.zkStorage;
        toConfig = params.toConfig;
        eoConfig = params.eoConfig;
    }

    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
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
        kafka.getSpec().getKafka().setJmxOptions(new KafkaJmxOptionsBuilder()
            .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
            .build());
        createCluster(context, kafka,
                Collections.singletonList(new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.jmxSecretName("foo"))
                        .withNamespace("test")
                        .endMetadata()
                        .withData(Collections.singletonMap("foo", "bar"))
                        .build()
                )); //getInitialCertificates(getKafkaAssembly("foo").getMetadata().getName()));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testCreateClusterWithJmxTrans(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafka = getKafkaAssembly("foo");
        kafka.getSpec()
                .getKafka().setJmxOptions(new KafkaJmxOptionsBuilder()
                .withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build())
                .build());

        kafka.getSpec().setJmxTrans(new JmxTransSpecBuilder()
                .withKafkaQueries(new JmxTransQueryTemplateBuilder()
                        .withNewTargetMBean("mbean")
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
                .withName(KafkaCluster.jmxSecretName("foo"))
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

    private void createCluster(VertxTestContext context, Kafka clusterCm, List<Secret> secrets) {
        ClusterCa clusterCa = new ClusterCa(new MockCertManager(), new PasswordGenerator(10, "a", "a"), clusterCm.getMetadata().getName(),
                findSecretWithName(secrets, AbstractModel.clusterCaCertSecretName(clusterCm.getMetadata().getName())),
                findSecretWithName(secrets, AbstractModel.clusterCaKeySecretName(clusterCm.getMetadata().getName())));
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(clusterCm, VERSIONS);
        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromCrd(clusterCm, VERSIONS);
        TopicOperator topicOperator = TopicOperator.fromCrd(clusterCm, VERSIONS);
        EntityOperator entityOperator = EntityOperator.fromCrd(clusterCm, VERSIONS);

        // create CM, Service, headless service, statefulset and so on
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ZookeeperSetOperator mockZsOps = supplier.zkSetOperations;
        KafkaSetOperator mockKsOps = supplier.kafkaSetOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        PodOperator mockPodOps = supplier.podOperations;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator mockPolicyOps = supplier.networkPolicyOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        RouteOperator mockRotueOps = supplier.routeOperations;
        NodeOperator mockNodeOps = supplier.nodeOperator;

        // Create a CM
        String clusterCmName = clusterCm.getMetadata().getName();
        String clusterCmNamespace = clusterCm.getMetadata().getNamespace();
        when(mockKafkaOps.get(clusterCmNamespace, clusterCmName)).thenReturn(null);
        when(mockKafkaOps.getAsync(eq(clusterCmNamespace), eq(clusterCmName))).thenReturn(Future.succeededFuture(clusterCm));
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        ArgumentCaptor<NetworkPolicy> policyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Service())));
        when(mockServiceOps.endpointReadiness(anyString(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StatefulSet> ssCaptor = ArgumentCaptor.forClass(StatefulSet.class);
        when(mockZsOps.reconcile(anyString(), anyString(), ssCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new StatefulSet())));
        when(mockZsOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(null));
        when(mockZsOps.maybeRollingUpdate(any(), any(Predicate.class))).thenReturn(Future.succeededFuture());
        when(mockZsOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockKsOps.reconcile(anyString(), anyString(), ssCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new StatefulSet())));
        when(mockKsOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(null));
        when(mockKsOps.maybeRollingUpdate(any(), any(Predicate.class))).thenReturn(Future.succeededFuture());
        when(mockKsOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockPolicyOps.reconcile(anyString(), anyString(), policyCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new NetworkPolicy())));
        when(mockZsOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(mockKsOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.reconcile(anyString(), anyString(), pdbCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new PodDisruptionBudget())));

        // Mock pod readiness
        when(mockPodOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPodOps.listAsync(anyString(), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock node ops
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        Map<String, PersistentVolumeClaim> zkPvcs = createPvcs(clusterCmNamespace, zookeeperCluster.getStorage(), zookeeperCluster.getReplicas(),
            (replica, storageId) -> AbstractModel.VOLUME_NAME + "-" + ZookeeperCluster.zookeeperPodName(clusterCmName, replica));

        Map<String, PersistentVolumeClaim> kafkaPvcs = createPvcs(clusterCmNamespace, kafkaCluster.getStorage(), kafkaCluster.getReplicas(),
            (replica, storageId) -> {
                String name = VolumeUtils.getVolumePrefix(storageId);
                return name + "-" + KafkaCluster.kafkaPodName(clusterCmName, replica);
            });

        when(mockPvcOps.get(eq(clusterCmNamespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(zookeeperCluster.getName())) {
                        return zkPvcs.get(pvcName);
                    } else if (pvcName.contains(kafkaCluster.getName())) {
                        return kafkaPvcs.get(pvcName);
                    }
                    return null;
                });

        when(mockPvcOps.getAsync(eq(clusterCmNamespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(zookeeperCluster.getName())) {
                        return Future.succeededFuture(zkPvcs.get(pvcName));
                    } else if (pvcName.contains(kafkaCluster.getName())) {
                        return Future.succeededFuture(kafkaPvcs.get(pvcName));
                    }
                    return Future.succeededFuture(null);
                });

        when(mockPvcOps.listAsync(eq(clusterCmNamespace), ArgumentMatchers.any(Labels.class)))
                .thenAnswer(invocation -> {
                    return Future.succeededFuture(Collections.EMPTY_LIST);
                });

        Set<String> expectedPvcs = new HashSet<>(zkPvcs.keySet());
        expectedPvcs.addAll(kafkaPvcs.keySet());
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        Set<String> expectedSecrets = set(
                KafkaCluster.clientsCaKeySecretName(clusterCmName),
                KafkaCluster.clientsCaCertSecretName(clusterCmName),
                KafkaCluster.clusterCaCertSecretName(clusterCmName),
                KafkaCluster.clusterCaKeySecretName(clusterCmName),
                KafkaCluster.brokersSecretName(clusterCmName),
                ZookeeperCluster.nodesSecretName(clusterCmName),
                ClusterOperator.secretName(clusterCmName));

        if (metrics)    {
            expectedSecrets.add(KafkaExporter.secretName(clusterCmName));
        }

        expectedSecrets.addAll(secrets.stream().map(s -> s.getMetadata().getName()).collect(Collectors.toSet()));
        if (toConfig != null) {
            // it's expected only when the Topic Operator is deployed by the Cluster Operator
            expectedSecrets.add(TopicOperator.secretName(clusterCmName));
        }
        if (eoConfig != null) {
            // it's expected only when the Entity Operator is deployed by the Cluster Operator
            expectedSecrets.add(EntityOperator.secretName(clusterCmName));
        }

        when(mockDepOps.reconcile(anyString(), anyString(), any())).thenAnswer(invocation -> {
            String name = invocation.getArgument(1);
            Deployment desired = invocation.getArgument(2);
            if (desired != null) {
                if (name.contains("operator")) {
                    if (topicOperator != null) {
                        context.verify(() -> assertThat(desired.getMetadata().getName(), is(TopicOperator.topicOperatorName(clusterCmName))));
                    } else if (entityOperator != null) {
                        context.verify(() -> assertThat(desired.getMetadata().getName(), is(EntityOperator.entityOperatorName(clusterCmName))));
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
        when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                Future.succeededFuture()
        );
        when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                Future.succeededFuture()
        );

        when(mockSecretOps.list(anyString(), any())).thenReturn(
                secrets
        );
        when(mockSecretOps.getAsync(anyString(), any())).thenReturn(
                Future.succeededFuture(null)
        );

        Set<String> createdOrUpdatedSecrets = new HashSet<>();
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenAnswer(invocation -> {
            Secret desired = invocation.getArgument(2);
            if (desired != null) {
                createdOrUpdatedSecrets.add(desired.getMetadata().getName());
            }
            return Future.succeededFuture(ReconcileResult.created(new Secret()));
        });

        ArgumentCaptor<ConfigMap> metricsCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        ArgumentCaptor<String> metricsNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.reconcile(anyString(), metricsNameCaptor.capture(), metricsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ArgumentCaptor<ConfigMap> logCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        ArgumentCaptor<String> logNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.reconcile(anyString(), logNameCaptor.capture(), logCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new ConfigMap())));

        ConfigMap metricsCm = kafkaCluster.generateAncillaryConfigMap(null, emptySet(), emptySet());
        when(mockCmOps.getAsync(clusterCmNamespace, KafkaCluster.metricAndLogConfigsName(clusterCmName))).thenReturn(Future.succeededFuture(metricsCm));

        when(mockCmOps.getAsync(anyString(), eq(JmxTrans.jmxTransConfigName(clusterCmName)))).thenReturn(
            Future.succeededFuture(new ConfigMapBuilder()
                    .withNewMetadata().withResourceVersion("123").endMetadata()
                    .build())
        );

        ArgumentCaptor<Route> routeCaptor = ArgumentCaptor.forClass(Route.class);
        ArgumentCaptor<String> routeNameCaptor = ArgumentCaptor.forClass(String.class);
        if (openShift) {
            when(mockRotueOps.reconcile(eq(clusterCmNamespace), routeNameCaptor.capture(), routeCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Route())));
        }

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config) {
            @Override
            public ReconciliationState createReconciliationState(Reconciliation r, Kafka ka) {
                return new ReconciliationState(r, ka) {
                    @Override
                    public Future<Void> waitForQuiescence(StatefulSet sts) {
                        return Future.succeededFuture();
                    }
                };
            }
        };

        // Now try to create a KafkaCluster based on this CM
        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            if (createResult.failed()) {
                createResult.cause().printStackTrace();
            }
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            // No metrics config  => no CMs created
            Set<String> logsAndMetricsNames = new HashSet<>();
            logsAndMetricsNames.add(KafkaCluster.metricAndLogConfigsName(clusterCmName));

            // We expect a headless and headful service
            Set<String> expectedServices = set(
                    ZookeeperCluster.headlessServiceName(clusterCmName),
                    ZookeeperCluster.serviceName(clusterCmName),
                    KafkaCluster.serviceName(clusterCmName),
                    KafkaCluster.headlessServiceName(clusterCmName));

            if (metrics)    {
                expectedServices.add(KafkaExporterResources.serviceName(clusterCmName));
            }

            if (kafkaListeners != null && kafkaListeners.getExternal() != null) {
                expectedServices.add(KafkaCluster.externalBootstrapServiceName(clusterCmName));

                for (int i = 0; i < kafkaCluster.getReplicas(); i++) {
                    expectedServices.add(KafkaCluster.externalServiceName(clusterCmName, i));
                }
            }

            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedServices.stream().filter(svc -> svc != null).map(svc -> svc.getMetadata().getName()).collect(Collectors.toSet()).size(),
                    is(expectedServices.size())));
            context.verify(() -> assertThat(capturedServices.stream().filter(svc -> svc != null).map(svc -> svc.getMetadata().getName()).collect(Collectors.toSet()),
                    is(expectedServices)));

            // Assertions on the statefulset
            List<StatefulSet> capturedSs = ssCaptor.getAllValues();
            // We expect a statefulSet for kafka and zookeeper...
            context.verify(() -> assertThat(capturedSs.stream().map(sts -> sts.getMetadata().getName()).collect(Collectors.toSet()),
                    is(set(KafkaCluster.kafkaClusterName(clusterCmName), ZookeeperCluster.zookeeperClusterName(clusterCmName)))));

            // expected Secrets with certificates
            context.verify(() -> assertThat(new TreeSet(createdOrUpdatedSecrets), is(new TreeSet(expectedSecrets))));

            // Check PDBs
            context.verify(() -> assertThat(pdbCaptor.getAllValues().size(), is(2)));
            context.verify(() -> assertThat(pdbCaptor.getAllValues().stream().map(sts -> sts.getMetadata().getName()).collect(Collectors.toSet()),
                    is(set(KafkaCluster.kafkaClusterName(clusterCmName), ZookeeperCluster.zookeeperClusterName(clusterCmName)))));

            // Check PVCs
            context.verify(() -> assertThat(pvcCaptor.getAllValues().size(), is(expectedPvcs.size())));
            context.verify(() -> assertThat(pvcCaptor.getAllValues().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toSet()),
                    is(expectedPvcs)));
            for (PersistentVolumeClaim pvc : pvcCaptor.getAllValues()) {
                context.verify(() -> assertThat(Annotations.hasAnnotation(pvc, AbstractModel.ANNO_STRIMZI_IO_DELETE_CLAIM), is(true)));
            }

            // Verify deleted routes
            if (openShift) {
                Set<String> expectedRoutes = set(KafkaCluster.serviceName(clusterCmName));

                for (int i = 0; i < kafkaCluster.getReplicas(); i++)    {
                    expectedRoutes.add(KafkaCluster.externalServiceName(clusterCmName, i));
                }

                context.verify(() -> assertThat(captured(routeNameCaptor), is(expectedRoutes)));
            } else {
                context.verify(() -> assertThat(routeNameCaptor.getAllValues().size(), is(0)));
            }

            async.flag();
        });
    }

    private Kafka getKafkaAssembly(String clusterName) {
        String clusterNamespace = "test";
        int replicas = 3;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;
        Map<String, Object> metricsCmJson = metrics ? METRICS_CONFIG : null;
        KafkaExporterSpec exporter = metrics ? new KafkaExporterSpec() : null;

        Kafka resource = ResourceUtils.createKafkaCluster(clusterNamespace, clusterName, replicas, image, healthDelay, healthTimeout, metricsCmJson, kafkaConfig, zooConfig, kafkaStorage, zkStorage, null, LOG_KAFKA_CONFIG, LOG_ZOOKEEPER_CONFIG, exporter);

        Kafka kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editKafka()
                        .withListeners(kafkaListeners)
                    .endKafka()
                    .withTopicOperator(toConfig)
                    .withEntityOperator(eoConfig)
                .endSpec()
                .build();

        return kafka;
    }

    private List<Secret> getInitialCertificates(String clusterName) {
        String clusterCmNamespace = "test";
        return ResourceUtils.createKafkaClusterInitialSecrets(clusterCmNamespace, clusterName);
    }

    private List<Secret> getClusterCertificates(String clusterCmName, int kafkaReplicas, int zkReplicas) {
        String clusterCmNamespace = "test";
        return ResourceUtils.createKafkaClusterSecretsWithReplicas(clusterCmNamespace, clusterCmName, kafkaReplicas, zkReplicas);
    }

    private static <T> Set<T> captured(ArgumentCaptor<T> captor) {
        return new HashSet<>(captor.getAllValues());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterNoop(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaClusterChangeImage(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setImage("a-changed-image");
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZookeeperClusterChangeImage(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setImage("a-changed-image");
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZookeeperClusterChangeStunnelImage(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly = new KafkaBuilder(kafkaAssembly)
                .editSpec().editZookeeper()
                    .editOrNewTlsSidecar().withImage("a-changed-tls-sidecar-image")
                    .endTlsSidecar().endZookeeper().endSpec().build();
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaClusterScaleUp(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setReplicas(4);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateKafkaClusterScaleDown(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setReplicas(2);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZookeeperClusterScaleUp(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setReplicas(4);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZookeeperClusterScaleDown(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setReplicas(2);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterMetricsConfig(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setMetrics(singletonMap("something", "changed"));
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterAuthenticationTrue(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        KafkaJmxOptions kafkaJmxOptions = new KafkaJmxOptionsBuilder().withAuthentication(
                 new KafkaJmxAuthenticationPasswordBuilder().build())
                .build();
        kafkaAssembly.getSpec().getKafka().setJmxOptions(kafkaJmxOptions);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateClusterLogConfig(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        InlineLogging logger = new InlineLogging();
        logger.setLoggers(singletonMap("kafka.root.logger.level", "DEBUG"));
        kafkaAssembly.getSpec().getKafka().setLogging(logger);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZkClusterMetricsConfig(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setMetrics(singletonMap("something", "changed"));
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateZkClusterLogConfig(Params params, VertxTestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        InlineLogging logger = new InlineLogging();
        logger.setLoggers(singletonMap("zookeeper.root.logger", "DEBUG"));
        kafkaAssembly.getSpec().getZookeeper().setLogging(logger);
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUpdateTopicOperatorConfig(Params params, VertxTestContext context) {
        setFields(params);
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        if (toConfig != null) {
            kafkaAssembly.getSpec().getTopicOperator().setImage("some/other:image");
            kafkaAssembly = new KafkaBuilder(kafkaAssembly)
                    .editSpec().editTopicOperator()
                    .editOrNewTlsSidecar().withImage("a-changed-tls-sidecar-image")
                    .endTlsSidecar().endTopicOperator().endSpec().build();
            updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly);
        }
        context.completeNow();
    }

    private void updateCluster(VertxTestContext context, Kafka originalAssembly, Kafka updatedAssembly) {
        KafkaCluster originalKafkaCluster = KafkaCluster.fromCrd(originalAssembly, VERSIONS);
        KafkaCluster updatedKafkaCluster = KafkaCluster.fromCrd(updatedAssembly, VERSIONS);
        ZookeeperCluster originalZookeeperCluster = ZookeeperCluster.fromCrd(originalAssembly, VERSIONS);
        ZookeeperCluster updatedZookeeperCluster = ZookeeperCluster.fromCrd(updatedAssembly, VERSIONS);
        TopicOperator originalTopicOperator = TopicOperator.fromCrd(originalAssembly, VERSIONS);
        EntityOperator originalEntityOperator = EntityOperator.fromCrd(originalAssembly, VERSIONS);
        KafkaExporter originalKafkaExporter = KafkaExporter.fromCrd(originalAssembly, VERSIONS);

        // create CM, Service, headless service, statefulset and so on
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ZookeeperSetOperator mockZsOps = supplier.zkSetOperations;
        KafkaSetOperator mockKsOps = supplier.kafkaSetOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        PodOperator mockPodOps = supplier.podOperations;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator mockPolicyOps = supplier.networkPolicyOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        ServiceAccountOperator mockSao = supplier.serviceAccountOperations;
        RoleBindingOperator mockRbo = supplier.roleBindingOperations;
        ClusterRoleBindingOperator mockCrbo = supplier.clusterRoleBindingOperator;
        RouteOperator mockRouteOps = supplier.routeOperations;
        NodeOperator mockNodeOps = supplier.nodeOperator;

        String clusterName = updatedAssembly.getMetadata().getName();
        String clusterNamespace = updatedAssembly.getMetadata().getNamespace();

        Map<String, PersistentVolumeClaim> zkPvcs =
                createPvcs(clusterNamespace, originalZookeeperCluster.getStorage(), originalZookeeperCluster.getReplicas(),
                    (replica, storageId) -> AbstractModel.VOLUME_NAME + "-" + ZookeeperCluster.zookeeperPodName(clusterName, replica));
        zkPvcs.putAll(createPvcs(clusterNamespace, updatedZookeeperCluster.getStorage(), updatedZookeeperCluster.getReplicas(),
            (replica, storageId) -> AbstractModel.VOLUME_NAME + "-" + ZookeeperCluster.zookeeperPodName(clusterName, replica)));

        Map<String, PersistentVolumeClaim> kafkaPvcs =
                createPvcs(clusterNamespace, originalKafkaCluster.getStorage(), originalKafkaCluster.getReplicas(),
                    (replica, storageId) -> {
                        String name = VolumeUtils.getVolumePrefix(storageId);
                        return name + "-" + KafkaCluster.kafkaPodName(clusterName, replica);
                    });
        kafkaPvcs.putAll(createPvcs(clusterNamespace, updatedKafkaCluster.getStorage(), updatedKafkaCluster.getReplicas(),
            (replica, storageId) -> {
                String name = VolumeUtils.getVolumePrefix(storageId);
                return name + "-" + KafkaCluster.kafkaPodName(clusterName, replica);
            }));

        when(mockPvcOps.get(eq(clusterNamespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(originalZookeeperCluster.getName())) {
                        return zkPvcs.get(pvcName);
                    } else if (pvcName.contains(originalKafkaCluster.getName())) {
                        return kafkaPvcs.get(pvcName);
                    }
                    return null;
                });

        when(mockPvcOps.getAsync(eq(clusterNamespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(originalZookeeperCluster.getName())) {
                        return Future.succeededFuture(zkPvcs.get(pvcName));
                    } else if (pvcName.contains(originalKafkaCluster.getName())) {
                        return Future.succeededFuture(kafkaPvcs.get(pvcName));
                    }
                    return Future.succeededFuture(null);
                });

        when(mockPvcOps.listAsync(eq(clusterNamespace), ArgumentMatchers.any(Labels.class)))
                .thenAnswer(invocation -> {
                    Labels labels = invocation.getArgument(1);
                    if (labels.toMap().get(Labels.STRIMZI_NAME_LABEL).contains("kafka")) {
                        return Future.succeededFuture(new ArrayList<PersistentVolumeClaim>(kafkaPvcs.values()));
                    } else if (labels.toMap().get(Labels.STRIMZI_NAME_LABEL).contains("zookeeper")) {
                        return Future.succeededFuture(new ArrayList<PersistentVolumeClaim>(zkPvcs.values()));
                    }
                    return Future.succeededFuture(Collections.EMPTY_LIST);
                });

        when(mockPvcOps.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());

        // Mock CM get
        when(mockKafkaOps.get(clusterNamespace, clusterName)).thenReturn(updatedAssembly);
        when(mockKafkaOps.getAsync(eq(clusterNamespace), eq(clusterName))).thenReturn(Future.succeededFuture(updatedAssembly));
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());
        ConfigMap metricsCm = originalKafkaCluster.generateAncillaryConfigMap(null, emptySet(), emptySet());
        when(mockCmOps.get(clusterNamespace, KafkaCluster.metricAndLogConfigsName(clusterName))).thenReturn(metricsCm);
        when(mockCmOps.getAsync(clusterNamespace, KafkaCluster.metricAndLogConfigsName(clusterName))).thenReturn(Future.succeededFuture(metricsCm));

        ConfigMap zkMetricsCm = new ConfigMapBuilder().withNewMetadata()
                .withName(ZookeeperCluster.zookeeperMetricAndLogConfigsName(clusterName))
                .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, TestUtils.toYamlString(METRICS_CONFIG)))
                .build();
        when(mockCmOps.get(clusterNamespace, ZookeeperCluster.zookeeperMetricAndLogConfigsName(clusterName))).thenReturn(zkMetricsCm);

        ConfigMap logCm = new ConfigMapBuilder().withNewMetadata()
                .withName(KafkaCluster.metricAndLogConfigsName(clusterName))
                .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG,
                        updatedKafkaCluster.parseLogging(LOG_KAFKA_CONFIG, null)))
                .build();
        when(mockCmOps.get(clusterNamespace, KafkaCluster.metricAndLogConfigsName(clusterName))).thenReturn(logCm);

        ConfigMap zklogsCm = new ConfigMapBuilder().withNewMetadata()
                .withName(ZookeeperCluster.zookeeperMetricAndLogConfigsName(clusterName))
                .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(AbstractModel.ANCILLARY_CM_KEY_LOG_CONFIG,
                        updatedZookeeperCluster.parseLogging(LOG_ZOOKEEPER_CONFIG, null)))
                .build();
        when(mockCmOps.get(clusterNamespace, ZookeeperCluster.zookeeperMetricAndLogConfigsName(clusterName))).thenReturn(zklogsCm);

        // Mock pod ops
        when(mockPodOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockPodOps.listAsync(anyString(), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock node ops
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock Service gets
        when(mockServiceOps.get(clusterNamespace, KafkaCluster.kafkaClusterName(clusterName))).thenReturn(
                originalKafkaCluster.generateService()
        );
        when(mockServiceOps.get(clusterNamespace, KafkaCluster.headlessServiceName(clusterName))).thenReturn(
                originalKafkaCluster.generateHeadlessService()
        );
        when(mockServiceOps.get(clusterNamespace, ZookeeperCluster.zookeeperClusterName(clusterName))).thenReturn(
                originalKafkaCluster.generateService()
        );
        when(mockServiceOps.get(clusterNamespace, ZookeeperCluster.headlessServiceName(clusterName))).thenReturn(
                originalZookeeperCluster.generateHeadlessService()
        );
        when(mockServiceOps.endpointReadiness(eq(clusterNamespace), any(), anyLong(), anyLong())).thenReturn(
                Future.succeededFuture()
        );

        // Mock Secret gets
        when(mockSecretOps.list(anyString(), any())).thenReturn(
                emptyList()
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaCluster.jmxSecretName(clusterName))).thenReturn(
                Future.succeededFuture(originalKafkaCluster.generateJmxSecret())
        );
        when(mockSecretOps.getAsync(clusterNamespace, ZookeeperCluster.nodesSecretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaCluster.brokersSecretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );
        when(mockSecretOps.getAsync(clusterNamespace, EntityOperator.secretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );
        when(mockSecretOps.getAsync(clusterNamespace, KafkaExporter.secretName(clusterName))).thenReturn(
                Future.succeededFuture()
        );

        // Mock NetworkPolicy get
        when(mockPolicyOps.get(clusterNamespace, KafkaCluster.policyName(clusterName))).thenReturn(originalKafkaCluster.generateNetworkPolicy(true));
        when(mockPolicyOps.get(clusterNamespace, ZookeeperCluster.policyName(clusterName))).thenReturn(originalZookeeperCluster.generateNetworkPolicy(true));

        // Mock PodDisruptionBudget get
        when(mockPdbOps.get(clusterNamespace, KafkaCluster.kafkaClusterName(clusterName))).thenReturn(originalKafkaCluster.generatePodDisruptionBudget());
        when(mockPdbOps.get(clusterNamespace, ZookeeperCluster.zookeeperClusterName(clusterName))).thenReturn(originalZookeeperCluster.generatePodDisruptionBudget());

        // Mock StatefulSet get
        when(mockKsOps.get(clusterNamespace, KafkaCluster.kafkaClusterName(clusterName))).thenReturn(
                originalKafkaCluster.generateStatefulSet(openShift, null, null)
        );
        when(mockZsOps.get(clusterNamespace, ZookeeperCluster.zookeeperClusterName(clusterName))).thenReturn(
                originalZookeeperCluster.generateStatefulSet(openShift, null, null)
        );
        // Mock Deployment get
        if (originalTopicOperator != null) {
            when(mockDepOps.get(clusterNamespace, TopicOperator.topicOperatorName(clusterName))).thenReturn(
                    originalTopicOperator.generateDeployment(true, null, null)
            );
            when(mockDepOps.getAsync(clusterNamespace, TopicOperator.topicOperatorName(clusterName))).thenReturn(
                    Future.succeededFuture(originalTopicOperator.generateDeployment(true, null, null))
            );
        }

        if (originalEntityOperator != null) {
            when(mockDepOps.get(clusterNamespace, EntityOperator.entityOperatorName(clusterName))).thenReturn(
                    originalEntityOperator.generateDeployment(true, Collections.EMPTY_MAP, null, null)
            );
            when(mockDepOps.getAsync(clusterNamespace, EntityOperator.entityOperatorName(clusterName))).thenReturn(
                    Future.succeededFuture(originalEntityOperator.generateDeployment(true, Collections.EMPTY_MAP, null, null))
            );
            when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
            when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
        }

        if (metrics) {
            when(mockDepOps.get(clusterNamespace, KafkaExporter.kafkaExporterName(clusterName))).thenReturn(
                    originalKafkaExporter.generateDeployment(true, null, null)
            );
            when(mockDepOps.getAsync(clusterNamespace, KafkaExporter.kafkaExporterName(clusterName))).thenReturn(
                    Future.succeededFuture(originalKafkaExporter.generateDeployment(true, null, null))
            );
            when(mockDepOps.waitForObserved(anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
            when(mockDepOps.readiness(anyString(), anyString(), anyLong(), anyLong())).thenReturn(
                    Future.succeededFuture()
            );
        }

        // Mock CM patch
        Set<String> metricsCms = set();
        doAnswer(invocation -> {
            metricsCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(clusterNamespace), any(), any());

        Set<String> logCms = set();
        doAnswer(invocation -> {
            logCms.add(invocation.getArgument(1));
            return Future.succeededFuture();
        }).when(mockCmOps).reconcile(eq(clusterNamespace), any(), any());

        // Mock Service patch (both service and headless service
        ArgumentCaptor<String> patchedServicesCaptor = ArgumentCaptor.forClass(String.class);
        when(mockServiceOps.reconcile(eq(clusterNamespace), patchedServicesCaptor.capture(), any())).thenReturn(Future.succeededFuture());
        // Mock Secrets patch
        when(mockSecretOps.reconcile(eq(clusterNamespace), any(), any())).thenReturn(Future.succeededFuture());

        // Mock NetworkPolicy patch
        when(mockPolicyOps.reconcile(eq(clusterNamespace), any(), any())).thenReturn(Future.succeededFuture());

        // Mock PodDisruptionBudget patch
        when(mockPdbOps.reconcile(eq(clusterNamespace), any(), any())).thenReturn(Future.succeededFuture());

        // Mock StatefulSet patch
        when(mockZsOps.reconcile(anyString(), anyString(), any())).thenAnswer(invocation -> {
            StatefulSet sts = invocation.getArgument(2);
            return Future.succeededFuture(ReconcileResult.patched(sts));
        });
        when(mockKsOps.reconcile(anyString(), anyString(), any())).thenAnswer(invocation -> {
            StatefulSet sts = invocation.getArgument(2);
            return Future.succeededFuture(ReconcileResult.patched(sts));
        });
        when(mockZsOps.maybeRollingUpdate(any(), any(Predicate.class))).thenReturn(Future.succeededFuture());
        when(mockKsOps.maybeRollingUpdate(any(), any(Predicate.class))).thenReturn(Future.succeededFuture());

        when(mockZsOps.getAsync(clusterNamespace, ZookeeperCluster.zookeeperClusterName(clusterName))).thenReturn(
                Future.succeededFuture(originalZookeeperCluster.generateStatefulSet(openShift, null, null))
        );
        when(mockKsOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        // Mock StatefulSet scaleUp
        ArgumentCaptor<String> scaledUpCaptor = ArgumentCaptor.forClass(String.class);
        when(mockZsOps.scaleUp(anyString(), scaledUpCaptor.capture(), anyInt())).thenReturn(
                Future.succeededFuture(42)
        );
        // Mock StatefulSet scaleDown
        ArgumentCaptor<String> scaledDownCaptor = ArgumentCaptor.forClass(String.class);
        when(mockZsOps.scaleDown(anyString(), scaledDownCaptor.capture(), anyInt())).thenReturn(
                Future.succeededFuture(42)
        );
        //ArgumentCaptor<String> scaledUpCaptor = ArgumentCaptor.forClass(String.class);
        when(mockKsOps.scaleUp(anyString(), scaledUpCaptor.capture(), anyInt())).thenReturn(
                Future.succeededFuture(42)
        );
        // Mock StatefulSet scaleDown
        //ArgumentCaptor<String> scaledDownCaptor = ArgumentCaptor.forClass(String.class);
        when(mockKsOps.scaleDown(anyString(), scaledDownCaptor.capture(), anyInt())).thenReturn(
                Future.succeededFuture(42)
        );

        // Mock Deployment patch
        ArgumentCaptor<String> depCaptor = ArgumentCaptor.forClass(String.class);
        when(mockDepOps.reconcile(anyString(), depCaptor.capture(), any())).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config) {
            @Override
            public ReconciliationState createReconciliationState(Reconciliation r, Kafka ka) {
                return new ReconciliationState(r, ka) {
                    @Override
                    public Future<Void> waitForQuiescence(StatefulSet sts) {
                        return Future.succeededFuture();
                    }
                };
            }
        };

        // Now try to update a KafkaCluster based on this CM
        Checkpoint async = context.checkpoint();
        ops.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, clusterNamespace, clusterName) {

        }, updatedAssembly).setHandler(createResult -> {
            if (createResult.failed()) createResult.cause().printStackTrace();
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));

            int steps = updatedAssembly.getSpec().getZookeeper().getReplicas();
            // rolling restart
            Set<String> expectedRollingRestarts = set();
            if (KafkaSetOperator.needsRollingUpdate(
                    new StatefulSetDiff(originalKafkaCluster.generateStatefulSet(openShift, null, null),
                    updatedKafkaCluster.generateStatefulSet(openShift, null, null)))) {
                expectedRollingRestarts.add(originalKafkaCluster.getName());
            }
            if (ZookeeperSetOperator.needsRollingUpdate(
                    new StatefulSetDiff(originalZookeeperCluster.generateStatefulSet(openShift, null, null),
                            updatedZookeeperCluster.generateStatefulSet(openShift, null, null)))) {
                expectedRollingRestarts.add(originalZookeeperCluster.getName());
            }

            // Check that ZK scale-up happens when it should
            verify(mockZsOps, times(updatedAssembly.getSpec().getZookeeper().getReplicas() > originalAssembly.getSpec().getZookeeper().getReplicas() ? 1 : 0)).scaleUp(anyString(), scaledUpCaptor.capture(), anyInt());

            // No metrics config  => no CMs created
            verify(mockCmOps, never()).createOrUpdate(any());

            async.flag();
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    public void testReconcile(Params params, VertxTestContext context) throws InterruptedException {
        setFields(params);

        // create CM, Service, headless service, statefulset
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ZookeeperSetOperator mockZsOps = supplier.zkSetOperations;
        KafkaSetOperator mockKsOps = supplier.kafkaSetOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSao = supplier.serviceAccountOperations;
        RoleBindingOperator mockRbo = supplier.roleBindingOperations;
        ClusterRoleBindingOperator mockCrbo = supplier.clusterRoleBindingOperator;
        String clusterCmNamespace = "test";

        Kafka foo = getKafkaAssembly("foo");
        Kafka bar = getKafkaAssembly("bar");
        when(mockKafkaOps.listAsync(eq(clusterCmNamespace), any(Optional.class))).thenReturn(
            Future.succeededFuture(asList(foo, bar))
        );
        // when requested Custom Resource for a specific Kafka cluster
        when(mockKafkaOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockKafkaOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);
        when(mockKafkaOps.getAsync(eq(clusterCmNamespace), eq("foo"))).thenReturn(Future.succeededFuture(foo));
        when(mockKafkaOps.getAsync(eq(clusterCmNamespace), eq("bar"))).thenReturn(Future.succeededFuture(bar));
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());

        // providing certificates Secrets for existing clusters
        List<Secret> fooSecrets = ResourceUtils.createKafkaClusterInitialSecrets(clusterCmNamespace, "foo");
        //ClusterCa fooCerts = ResourceUtils.createInitialClusterCa("foo", ModelUtils.findSecretWithName(fooSecrets, AbstractModel.clusterCaCertSecretName("foo")));
        List<Secret> barSecrets = ResourceUtils.createKafkaClusterSecretsWithReplicas(clusterCmNamespace, "bar",
                bar.getSpec().getKafka().getReplicas(),
                bar.getSpec().getZookeeper().getReplicas());
        ClusterCa barClusterCa = ResourceUtils.createInitialClusterCa("bar",
                findSecretWithName(barSecrets, AbstractModel.clusterCaCertSecretName("bar")),
                findSecretWithName(barSecrets, AbstractModel.clusterCaKeySecretName("bar")));
        ClientsCa barClientsCa = ResourceUtils.createInitialClientsCa("bar",
                findSecretWithName(barSecrets, KafkaCluster.clientsCaCertSecretName("bar")),
                findSecretWithName(barSecrets, KafkaCluster.clientsCaKeySecretName("bar")));

        // providing the list of ALL StatefulSets for all the Kafka clusters
        Labels newLabels = Labels.forKind(Kafka.RESOURCE_KIND);
        when(mockKsOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaCluster.fromCrd(bar, VERSIONS).generateStatefulSet(openShift, null, null))
        );

        when(mockSecretOps.get(eq(clusterCmNamespace), eq(AbstractModel.clusterCaCertSecretName(foo.getMetadata().getName()))))
                .thenReturn(
                        fooSecrets.get(0));
        when(mockSecretOps.reconcile(eq(clusterCmNamespace), eq(AbstractModel.clusterCaCertSecretName(foo.getMetadata().getName())), any(Secret.class))).thenReturn(Future.succeededFuture());

        // providing the list StatefulSets for already "existing" Kafka clusters
        Labels barLabels = Labels.forCluster("bar");
        KafkaCluster barCluster = KafkaCluster.fromCrd(bar, VERSIONS);
        when(mockKsOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(barCluster.generateStatefulSet(openShift, null, null))
        );
        when(mockSecretOps.list(eq(clusterCmNamespace), eq(barLabels))).thenAnswer(
            invocation -> new ArrayList<>(asList(
                    barClientsCa.caKeySecret(),
                    barClientsCa.caCertSecret(),
                    barCluster.generateBrokersSecret(),
                    barClusterCa.caCertSecret()))
        );
        when(mockSecretOps.get(eq(clusterCmNamespace), eq(AbstractModel.clusterCaCertSecretName(bar.getMetadata().getName())))).thenReturn(barSecrets.get(0));
        when(mockSecretOps.reconcile(eq(clusterCmNamespace), eq(AbstractModel.clusterCaCertSecretName(bar.getMetadata().getName())), any(Secret.class))).thenReturn(Future.succeededFuture());

        CountDownLatch fooLatch = new CountDownLatch(1);
        CountDownLatch barLatch = new CountDownLatch(1);

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config) {
            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
                String name = kafkaAssembly.getMetadata().getName();
                if ("foo".equals(name)) {
                    fooLatch.countDown();
                } else if ("bar".equals(name)) {
                    barLatch.countDown();
                } else {
                    context.failNow(new AssertionError("Unexpected name " + name));
                }
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka clusters
        ops.reconcileAll("test", clusterCmNamespace, context.succeeding());

        if (!fooLatch.await(2, TimeUnit.MINUTES)) {
            if (barLatch.getCount() > 0) {
                fail("Neither foo nor bar seen");
            } else {
                fail("foo not seen");
            }
        }
        if (!barLatch.await(2, TimeUnit.MINUTES)) {
            fail("bar not seen");
        }
        context.completeNow();
    }

    @ParameterizedTest
    @MethodSource("data")
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    public void testReconcileAllNamespaces(Params params, VertxTestContext context) throws InterruptedException {
        setFields(params);

        // create CM, Service, headless service, statefulset
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(openShift);
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        KafkaSetOperator mockKsOps = supplier.kafkaSetOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;

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
        when(mockKafkaOps.updateStatusAsync(any(Kafka.class))).thenReturn(Future.succeededFuture());

        // providing certificates Secrets for existing clusters
        List<Secret> fooSecrets = ResourceUtils.createKafkaClusterInitialSecrets("namespace1", "foo");
        List<Secret> barSecrets = ResourceUtils.createKafkaClusterSecretsWithReplicas("namespace2", "bar",
                bar.getSpec().getKafka().getReplicas(),
                bar.getSpec().getZookeeper().getReplicas());
        ClusterCa barClusterCa = ResourceUtils.createInitialClusterCa("bar",
                findSecretWithName(barSecrets, AbstractModel.clusterCaCertSecretName("bar")),
                findSecretWithName(barSecrets, AbstractModel.clusterCaKeySecretName("bar")));
        ClientsCa barClientsCa = ResourceUtils.createInitialClientsCa("bar",
                findSecretWithName(barSecrets, KafkaCluster.clientsCaCertSecretName("bar")),
                findSecretWithName(barSecrets, KafkaCluster.clientsCaKeySecretName("bar")));

        // providing the list of ALL StatefulSets for all the Kafka clusters
        Labels newLabels = Labels.forKind(Kafka.RESOURCE_KIND);
        when(mockKsOps.list(eq("*"), eq(newLabels))).thenReturn(
                asList(KafkaCluster.fromCrd(bar, VERSIONS).generateStatefulSet(openShift, null, null))
        );

        // providing the list StatefulSets for already "existing" Kafka clusters
        Labels barLabels = Labels.forCluster("bar");
        KafkaCluster barCluster = KafkaCluster.fromCrd(bar, VERSIONS);
        when(mockKsOps.list(eq("*"), eq(barLabels))).thenReturn(
                asList(barCluster.generateStatefulSet(openShift, null, null))
        );
        when(mockSecretOps.list(eq("*"), eq(barLabels))).thenAnswer(
            invocation -> new ArrayList<>(asList(
                    barClientsCa.caKeySecret(),
                    barClientsCa.caCertSecret(),
                    barCluster.generateBrokersSecret(),
                    barClusterCa.caCertSecret()))
        );

        CountDownLatch fooLatch = new CountDownLatch(1);
        CountDownLatch barLatch = new CountDownLatch(1);
        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(openShift, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config) {
            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
                String name = kafkaAssembly.getMetadata().getName();
                if ("foo".equals(name)) {
                    fooLatch.countDown();
                } else if ("bar".equals(name)) {
                    barLatch.countDown();
                } else {
                    context.failNow(new AssertionError("Unexpected name " + name));
                }
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka clusters
        ops.reconcileAll("test", "*", context.succeeding());

        if (!fooLatch.await(2, TimeUnit.MINUTES)) {
            if (barLatch.getCount() > 0) {
                fail("Neither foo nor bar seen");
            } else {
                fail("foo not seen");
            }
        }
        if (!barLatch.await(2, TimeUnit.MINUTES)) {
            fail("bar not seen");
        }
        context.completeNow();
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
