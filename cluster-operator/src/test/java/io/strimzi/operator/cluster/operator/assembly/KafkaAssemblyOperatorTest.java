/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicy;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaListeners;
import io.strimzi.api.kafka.model.KafkaListenersBuilder;
import io.strimzi.api.kafka.model.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.api.kafka.model.TopicOperatorSpec;
import io.strimzi.api.kafka.model.TopicOperatorSpecBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.EntityOperator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetDiff;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.test.TestUtils;

import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class KafkaAssemblyOperatorTest {

    public static final Map<String, Object> METRICS_CONFIG = singletonMap("foo", "bar");
    public static final InlineLogging LOG_KAFKA_CONFIG = new InlineLogging();
    public static final InlineLogging LOG_ZOOKEEPER_CONFIG = new InlineLogging();
    public static final InlineLogging LOG_CONNECT_CONFIG = new InlineLogging();
    static {
        LOG_KAFKA_CONFIG.setLoggers(singletonMap("kafka.root.logger.level", "INFO"));
        LOG_ZOOKEEPER_CONFIG.setLoggers(singletonMap("zookeeper.root.logger", "INFO"));
        LOG_CONNECT_CONFIG.setLoggers(singletonMap("connect.root.logger.level", "INFO"));
    }
    private final boolean openShift;
    private final boolean metrics;
    private final KafkaListeners kafkaListeners;
    private final Map<String, Object> kafkaConfig;
    private final Map<String, Object> zooConfig;
    private final Storage storage;
    private final TopicOperatorSpec toConfig;
    private final EntityOperatorSpec eoConfig;
    private final boolean deleteClaim;
    private MockCertManager certManager = new MockCertManager();

    public static class Params {
        private final boolean openShift;
        private final boolean metrics;
        private final KafkaListeners kafkaListeners;
        private final Map<String, Object> kafkaConfig;
        private final Map<String, Object> zooConfig;
        private final Storage storage;
        private final TopicOperatorSpec toConfig;
        private final EntityOperatorSpec eoConfig;

        public Params(boolean openShift, boolean metrics, KafkaListeners kafkaListeners, Map<String, Object> kafkaConfig, Map<String, Object> zooConfig, Storage storage, TopicOperatorSpec toConfig, EntityOperatorSpec eoConfig) {
            this.openShift = openShift;
            this.metrics = metrics;
            this.kafkaConfig = kafkaConfig;
            this.kafkaListeners = kafkaListeners;
            this.zooConfig = zooConfig;
            this.storage = storage;
            this.toConfig = toConfig;
            this.eoConfig = eoConfig;
        }

        public String toString() {
            return "openShift=" + openShift +
                    ",metrics=" + metrics +
                    ",kafkaListeners=" + kafkaListeners +
                    ",kafkaConfig=" + kafkaConfig +
                    ",zooConfig=" + zooConfig +
                    ",storage=" + storage +
                    ",toConfig=" + toConfig +
                    ",eoConfig=" + eoConfig;
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Params> data() {
        boolean[] shiftiness = {true, false};
        boolean[] metrics = {true, false};
        Storage[] storageConfigs = {
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
                        for (Storage storage : storageConfigs) {
                            for (TopicOperatorSpec toConfig : toConfigs) {
                                for (EntityOperatorSpec eoConfig : eoConfigs) {
                                    KafkaListeners listeners;
                                    if (shift)   {
                                        listeners = new KafkaListenersBuilder()
                                                .withNewPlain()
                                                    .withNewKafkaListenerAuthenticationScramSha512Authentication()
                                                    .endKafkaListenerAuthenticationScramSha512Authentication()
                                                .endPlain()
                                                .withNewTls()
                                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerAuthenticationTlsAuth()
                                                .endTls()
                                                .withNewKafkaListenerExternalRouteExternal()
                                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerAuthenticationTlsAuth()
                                                .endKafkaListenerExternalRouteExternal()
                                                .build();
                                    } else {
                                        listeners = new KafkaListenersBuilder()
                                                .withNewPlain()
                                                    .withNewKafkaListenerAuthenticationScramSha512Authentication()
                                                    .endKafkaListenerAuthenticationScramSha512Authentication()
                                                .endPlain()
                                                .withNewTls()
                                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerAuthenticationTlsAuth()
                                                .endTls()
                                                .withNewKafkaListenerExternalNodePortExternal()
                                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                                    .endKafkaListenerAuthenticationTlsAuth()
                                                .endKafkaListenerExternalNodePortExternal()
                                                .build();
                                    }

                                    // TO and EO cannot be deployed together so no need for testing this case
                                    if (!(toConfig != null && eoConfig != null)) {
                                        result.add(new Params(shift, metric, listeners, kafkaConfig, zooConfig, storage, toConfig, eoConfig));
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

    public KafkaAssemblyOperatorTest(Params params) {
        this.openShift = params.openShift;
        this.metrics = params.metrics;
        this.kafkaListeners = params.kafkaListeners;
        this.kafkaConfig = params.kafkaConfig;
        this.zooConfig = params.zooConfig;
        this.storage = params.storage;
        this.toConfig = params.toConfig;
        this.eoConfig = params.eoConfig;
        this.deleteClaim = Storage.deleteClaim(params.storage);
    }

    protected static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateCluster(TestContext context) {
        createCluster(context, getKafkaAssembly("foo"), getInitialSecrets(getKafkaAssembly("foo").getMetadata().getName()));
    }

    private void createCluster(TestContext context, Kafka clusterCm, List<Secret> secrets) {
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(clusterCm);
        kafkaCluster.generateCertificates(certManager, clusterCm, secrets,  null, Collections.EMPTY_MAP);
        ZookeeperCluster zookeeperCluster = ZookeeperCluster.fromCrd(certManager, clusterCm, secrets);
        TopicOperator topicOperator = TopicOperator.fromCrd(certManager, clusterCm, secrets);
        EntityOperator entityOperator = EntityOperator.fromCrd(certManager, clusterCm, secrets);

        // create CM, Service, headless service, statefulset and so on
        ResourceOperatorSupplier supplier = supplierWithMocks();
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ZookeeperSetOperator mockZsOps = supplier.zkSetOperations;
        KafkaSetOperator mockKsOps = supplier.kafkaSetOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator mockPolicyOps = supplier.networkPolicyOperator;
        RouteOperator mockRotueOps = supplier.routeOperations;

        // Create a CM
        String clusterCmName = clusterCm.getMetadata().getName();
        String clusterCmNamespace = clusterCm.getMetadata().getNamespace();
        when(mockKafkaOps.get(clusterCmNamespace, clusterCmName)).thenReturn(null);
        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        ArgumentCaptor<NetworkPolicy> policyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockServiceOps.reconcile(anyString(), anyString(), serviceCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockServiceOps.endpointReadiness(anyString(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<StatefulSet> ssCaptor = ArgumentCaptor.forClass(StatefulSet.class);
        when(mockZsOps.reconcile(anyString(), anyString(), ssCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockZsOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(null));
        when(mockZsOps.maybeRollingUpdate(any(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockZsOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockKsOps.reconcile(anyString(), anyString(), ssCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        when(mockKsOps.scaleDown(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(null));
        when(mockKsOps.maybeRollingUpdate(any(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockKsOps.scaleUp(anyString(), anyString(), anyInt())).thenReturn(Future.succeededFuture(42));
        when(mockPolicyOps.reconcile(anyString(), anyString(), policyCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        Set<String> expectedSecrets = set(
                KafkaCluster.clientsCASecretName(clusterCmName),
                KafkaCluster.clientsPublicKeyName(clusterCmName),
                KafkaCluster.clusterPublicKeyName(clusterCmName),
                KafkaCluster.brokersSecretName(clusterCmName),
                ZookeeperCluster.nodesSecretName(clusterCmName));
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
            Deployment desired = invocation.getArgument(2);
            if (desired != null) {
                if (topicOperator != null) {
                    context.assertEquals(TopicOperator.topicOperatorName(clusterCmName), desired.getMetadata().getName());
                } else if (entityOperator != null) {
                    context.assertEquals(EntityOperator.entityOperatorName(clusterCmName), desired.getMetadata().getName());
                }
            }
            return Future.succeededFuture(ReconcileResult.created(desired));
        });

        when(mockSecretOps.list(anyString(), any())).thenReturn(
                secrets
        );
        Set<String> createdOrUpdatedSecrets = new HashSet<>();
        when(mockSecretOps.reconcile(anyString(), anyString(), any())).thenAnswer(invocation -> {
            Secret desired = invocation.getArgument(2);
            if (desired != null) {
                createdOrUpdatedSecrets.add(desired.getMetadata().getName());
            }
            return Future.succeededFuture(ReconcileResult.created(null));
        });

        ArgumentCaptor<ConfigMap> metricsCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        ArgumentCaptor<String> metricsNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.reconcile(anyString(), metricsNameCaptor.capture(), metricsCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        ArgumentCaptor<ConfigMap> logCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        ArgumentCaptor<String> logNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.reconcile(anyString(), logNameCaptor.capture(), logCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));

        ArgumentCaptor<Route> routeCaptor = ArgumentCaptor.forClass(Route.class);
        ArgumentCaptor<String> routeNameCaptor = ArgumentCaptor.forClass(String.class);
        if (openShift) {
            when(mockRotueOps.reconcile(eq(clusterCmNamespace), routeNameCaptor.capture(), routeCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(null)));
        }

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, openShift,
                ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS,
                certManager,
                supplier);

        // Now try to create a KafkaCluster based on this CM
        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.KAFKA, clusterCmNamespace, clusterCmName), clusterCm).setHandler(createResult -> {
            if (createResult.failed()) {
                createResult.cause().printStackTrace();
            }
            context.assertTrue(createResult.succeeded());

            // No metrics config  => no CMs created
            Set<String> logsAndMetricsNames = new HashSet<>();
            logsAndMetricsNames.add(KafkaCluster.metricAndLogConfigsName(clusterCmName));

            // We expect a headless and headful service
            Set<String> expectedServices = set(
                    ZookeeperCluster.headlessServiceName(clusterCmName),
                    ZookeeperCluster.serviceName(clusterCmName),
                    KafkaCluster.serviceName(clusterCmName),
                    KafkaCluster.headlessServiceName(clusterCmName));

            if (kafkaListeners != null && kafkaListeners.getExternal() != null) {
                expectedServices.add(KafkaCluster.externalBootstrapServiceName(clusterCmName));

                for (int i = 0; i < kafkaCluster.getReplicas(); i++) {
                    expectedServices.add(KafkaCluster.externalServiceName(clusterCmName, i));
                }
            }

            List<Service> capturedServices = serviceCaptor.getAllValues();
            context.assertEquals(expectedServices.size(), capturedServices.size());
            context.assertEquals(expectedServices, capturedServices.stream().filter(svc -> svc != null).map(svc -> svc.getMetadata().getName()).collect(Collectors.toSet()));

            // Assertions on the statefulset
            List<StatefulSet> capturedSs = ssCaptor.getAllValues();
            // We expect a statefulSet for kafka and zookeeper...
            context.assertEquals(set(KafkaCluster.kafkaClusterName(clusterCmName), ZookeeperCluster.zookeeperClusterName(clusterCmName)),
                    capturedSs.stream().map(ss -> ss.getMetadata().getName()).collect(Collectors.toSet()));

            // expected Secrets with certificates
            context.assertEquals(expectedSecrets, createdOrUpdatedSecrets);

            // Verify deleted routes
            if (openShift) {
                Set<String> expectedRoutes = set(KafkaCluster.serviceName(clusterCmName));

                for (int i = 0; i < kafkaCluster.getReplicas(); i++)    {
                    expectedRoutes.add(KafkaCluster.externalServiceName(clusterCmName, i));
                }

                context.assertEquals(expectedRoutes,
                        captured(routeNameCaptor));
            } else {
                context.assertEquals(0, routeNameCaptor.getAllValues().size());
            }

            verifyNoMoreInteractions(mockPvcOps);
            async.complete();
        });
    }

    private Kafka getKafkaAssembly(String clusterName) {
        String clusterNamespace = "test";
        int replicas = 3;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;
        Map<String, Object> metricsCmJson = metrics ? METRICS_CONFIG : null;

        Kafka resource = ResourceUtils.createKafkaCluster(clusterNamespace, clusterName, replicas, image, healthDelay, healthTimeout, metricsCmJson, kafkaConfig, zooConfig, storage, null, LOG_KAFKA_CONFIG, LOG_ZOOKEEPER_CONFIG);

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

    private List<Secret> getInitialSecrets(String clusterName) {
        String clusterCmNamespace = "test";
        return ResourceUtils.createKafkaClusterInitialSecrets(clusterCmNamespace, clusterName);
    }

    private List<Secret> getClusterSecrets(String clusterCmName, int kafkaReplicas, int zkReplicas) {
        String clusterCmNamespace = "test";
        return ResourceUtils.createKafkaClusterSecretsWithReplicas(clusterCmNamespace, clusterCmName, kafkaReplicas, zkReplicas);
    }

    private static <T> Set<T> captured(ArgumentCaptor<T> captor) {
        return new HashSet<>(captor.getAllValues());
    }

    @Test
    public void testUpdateClusterNoop(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateKafkaClusterChangeImage(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setImage("a-changed-image");
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateZookeeperClusterChangeImage(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setImage("a-changed-image");
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateZookeeperClusterChangeStunnelImage(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly = new KafkaBuilder(kafkaAssembly)
                .editSpec().editZookeeper()
                    .editOrNewTlsSidecar().withImage("a-changed-tls-sidecar-image")
                    .endTlsSidecar().endZookeeper().endSpec().build();
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateKafkaClusterScaleUp(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setReplicas(4);
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateKafkaClusterScaleDown(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setReplicas(2);
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateZookeeperClusterScaleUp(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setReplicas(4);
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateZookeeperClusterScaleDown(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setReplicas(2);
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateClusterMetricsConfig(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getKafka().setMetrics(singletonMap("something", "changed"));
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateClusterLogConfig(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        InlineLogging logger = new InlineLogging();
        logger.setLoggers(singletonMap("kafka.root.logger.level", "DEBUG"));
        kafkaAssembly.getSpec().getKafka().setLogging(logger);
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateZkClusterMetricsConfig(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        kafkaAssembly.getSpec().getZookeeper().setMetrics(singletonMap("something", "changed"));
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateZkClusterLogConfig(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        InlineLogging logger = new InlineLogging();
        logger.setLoggers(singletonMap("zookeeper.root.logger", "DEBUG"));
        kafkaAssembly.getSpec().getZookeeper().setLogging(logger);
        List<Secret> secrets = getClusterSecrets("bar",
                kafkaAssembly.getSpec().getKafka().getReplicas(),
                kafkaAssembly.getSpec().getZookeeper().getReplicas());
        updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
    }

    @Test
    public void testUpdateTopicOperatorConfig(TestContext context) {
        Kafka kafkaAssembly = getKafkaAssembly("bar");
        if (toConfig != null) {
            kafkaAssembly.getSpec().getTopicOperator().setImage("some/other:image");
            kafkaAssembly = new KafkaBuilder(kafkaAssembly)
                    .editSpec().editTopicOperator()
                    .editOrNewTlsSidecar().withImage("a-changed-tls-sidecar-image")
                    .endTlsSidecar().endTopicOperator().endSpec().build();
            List<Secret> secrets = getClusterSecrets("bar",
                    kafkaAssembly.getSpec().getKafka().getReplicas(),
                    kafkaAssembly.getSpec().getZookeeper().getReplicas());
            updateCluster(context, getKafkaAssembly("bar"), kafkaAssembly, secrets);
        }
    }


    private void updateCluster(TestContext context, Kafka originalAssembly, Kafka updatedAssembly, List<Secret> secrets) {
        KafkaCluster originalKafkaCluster = KafkaCluster.fromCrd(originalAssembly);
        originalKafkaCluster.generateCertificates(certManager, originalAssembly, secrets,  null, Collections.EMPTY_MAP);
        KafkaCluster updatedKafkaCluster = KafkaCluster.fromCrd(updatedAssembly);
        updatedKafkaCluster.generateCertificates(certManager, updatedAssembly, secrets,  null, Collections.EMPTY_MAP);
        ZookeeperCluster originalZookeeperCluster = ZookeeperCluster.fromCrd(certManager, originalAssembly, secrets);
        ZookeeperCluster updatedZookeeperCluster = ZookeeperCluster.fromCrd(certManager, updatedAssembly, secrets);
        TopicOperator originalTopicOperator = TopicOperator.fromCrd(certManager, originalAssembly, secrets);
        EntityOperator originalEntityOperator = EntityOperator.fromCrd(certManager, originalAssembly, secrets);

        // create CM, Service, headless service, statefulset and so on
        ResourceOperatorSupplier supplier = supplierWithMocks();
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ZookeeperSetOperator mockZsOps = supplier.zkSetOperations;
        KafkaSetOperator mockKsOps = supplier.kafkaSetOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        NetworkPolicyOperator mockPolicyOps = supplier.networkPolicyOperator;
        ServiceAccountOperator mockSao = supplier.serviceAccountOperator;
        RoleBindingOperator mockRbo = supplier.roleBindingOperator;
        ClusterRoleBindingOperator mockCrbo = supplier.clusterRoleBindingOperator;
        RouteOperator mockRouteOps = supplier.routeOperations;

        String clusterName = updatedAssembly.getMetadata().getName();
        String clusterNamespace = updatedAssembly.getMetadata().getNamespace();

        // Mock CM get
        when(mockKafkaOps.get(clusterNamespace, clusterName)).thenReturn(updatedAssembly);
        ConfigMap metricsCm = new ConfigMapBuilder().withNewMetadata()
                .withName(KafkaCluster.metricAndLogConfigsName(clusterName))
                    .withNamespace(clusterNamespace)
                .endMetadata()
                .withData(singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, TestUtils.toYamlString(METRICS_CONFIG)))
                .build();
        when(mockCmOps.get(clusterNamespace, KafkaCluster.metricAndLogConfigsName(clusterName))).thenReturn(metricsCm);

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
                secrets
        );
        when(mockSecretOps.get(clusterNamespace, KafkaCluster.clientsCASecretName(clusterName))).thenReturn(
                originalKafkaCluster.generateClientsCASecret()
        );
        when(mockSecretOps.get(clusterNamespace, KafkaCluster.clientsPublicKeyName(clusterName))).thenReturn(
                originalKafkaCluster.generateClientsPublicKeySecret()
        );
        when(mockSecretOps.get(clusterNamespace, KafkaCluster.clusterPublicKeyName(clusterNamespace))).thenReturn(
                originalKafkaCluster.generateClusterPublicKeySecret()
        );
        when(mockSecretOps.get(clusterNamespace, KafkaCluster.brokersSecretName(clusterName))).thenReturn(
                originalKafkaCluster.generateBrokersSecret()
        );

        // Mock NetworkPolicy get
        when(mockPolicyOps.get(clusterNamespace, KafkaCluster.policyName(clusterName))).thenReturn(originalKafkaCluster.generateNetworkPolicy());
        when(mockPolicyOps.get(clusterNamespace, ZookeeperCluster.policyName(clusterName))).thenReturn(originalZookeeperCluster.generateNetworkPolicy());

        // Mock StatefulSet get
        when(mockKsOps.get(clusterNamespace, KafkaCluster.kafkaClusterName(clusterName))).thenReturn(
                originalKafkaCluster.generateStatefulSet(openShift)
        );
        when(mockZsOps.get(clusterNamespace, ZookeeperCluster.zookeeperClusterName(clusterName))).thenReturn(
                originalZookeeperCluster.generateStatefulSet(openShift)
        );
        // Mock Deployment get
        if (originalTopicOperator != null) {
            when(mockDepOps.get(clusterNamespace, TopicOperator.topicOperatorName(clusterName))).thenReturn(
                    originalTopicOperator.generateDeployment()
            );
        }
        if (originalEntityOperator != null) {
            when(mockDepOps.get(clusterNamespace, EntityOperator.entityOperatorName(clusterName))).thenReturn(
                    originalEntityOperator.generateDeployment()
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

        // Mock Secrets patch
        when(mockPolicyOps.reconcile(eq(clusterNamespace), any(), any())).thenReturn(Future.succeededFuture());

        // Mock StatefulSet patch
        when(mockZsOps.reconcile(anyString(), anyString(), any())).thenAnswer(invocation -> {
            StatefulSet ss = invocation.getArgument(2);
            return Future.succeededFuture(ReconcileResult.patched(ss));
        });
        when(mockKsOps.reconcile(anyString(), anyString(), any())).thenAnswer(invocation -> {
            StatefulSet ss = invocation.getArgument(2);
            return Future.succeededFuture(ReconcileResult.patched(ss));
        });
        when(mockZsOps.maybeRollingUpdate(any(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockKsOps.maybeRollingUpdate(any(), anyBoolean())).thenReturn(Future.succeededFuture());

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

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, openShift,
                ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS,
                certManager,
                supplier);

        // Now try to update a KafkaCluster based on this CM
        Async async = context.async();
        ops.createOrUpdate(new Reconciliation("test-trigger", ResourceType.KAFKA, clusterNamespace, clusterName), updatedAssembly).setHandler(createResult -> {
            if (createResult.failed()) createResult.cause().printStackTrace();
            context.assertTrue(createResult.succeeded());

            // rolling restart
            Set<String> expectedRollingRestarts = set();
            if (KafkaSetOperator.needsRollingUpdate(
                    new StatefulSetDiff(originalKafkaCluster.generateStatefulSet(openShift),
                    updatedKafkaCluster.generateStatefulSet(openShift)))) {
                expectedRollingRestarts.add(originalKafkaCluster.getName());
            }
            if (ZookeeperSetOperator.needsRollingUpdate(
                    new StatefulSetDiff(originalZookeeperCluster.generateStatefulSet(openShift),
                            updatedZookeeperCluster.generateStatefulSet(openShift)))) {
                expectedRollingRestarts.add(originalZookeeperCluster.getName());
            }

            // No metrics config  => no CMs created
            verify(mockCmOps, never()).createOrUpdate(any());
            verifyNoMoreInteractions(mockPvcOps);
            async.complete();
        });
    }

    @Test
    public void testReconcile(TestContext context) throws InterruptedException {
        Async async = context.async(2);

        // create CM, Service, headless service, statefulset
        ResourceOperatorSupplier supplier = supplierWithMocks();
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        ZookeeperSetOperator mockZsOps = supplier.zkSetOperations;
        KafkaSetOperator mockKsOps = supplier.kafkaSetOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        DeploymentOperator mockDepOps = supplier.deploymentOperations;
        SecretOperator mockSecretOps = supplier.secretOperations;
        ServiceAccountOperator mockSao = supplier.serviceAccountOperator;
        RoleBindingOperator mockRbo = supplier.roleBindingOperator;
        ClusterRoleBindingOperator mockCrbo = supplier.clusterRoleBindingOperator;
        String clusterCmNamespace = "myNamespace";

        Kafka foo = getKafkaAssembly("foo");
        Kafka bar = getKafkaAssembly("bar");
        when(mockKafkaOps.list(eq(clusterCmNamespace), any())).thenReturn(
            asList(foo, bar)
        );
        // when requested Custom Resource for a specific Kafka cluster
        when(mockKafkaOps.get(eq(clusterCmNamespace), eq("foo"))).thenReturn(foo);
        when(mockKafkaOps.get(eq(clusterCmNamespace), eq("bar"))).thenReturn(bar);

        // providing certificates Secrets for existing clusters
        List<Secret> fooSecrets = ResourceUtils.createKafkaClusterInitialSecrets(clusterCmNamespace, "foo");
        List<Secret> barSecrets = ResourceUtils.createKafkaClusterSecretsWithReplicas(clusterCmNamespace, "bar",
                bar.getSpec().getKafka().getReplicas(),
                bar.getSpec().getZookeeper().getReplicas());

        // providing the list of ALL StatefulSets for all the Kafka clusters
        Labels newLabels = Labels.forKind(Kafka.RESOURCE_KIND);
        when(mockKsOps.list(eq(clusterCmNamespace), eq(newLabels))).thenReturn(
                asList(KafkaCluster.fromCrd(bar).generateStatefulSet(openShift))
        );

        when(mockSecretOps.get(eq(clusterCmNamespace), eq(AbstractModel.getClusterCaName(foo.getMetadata().getName())))).thenReturn(fooSecrets.get(0));
        when(mockSecretOps.reconcile(eq(clusterCmNamespace), eq(AbstractModel.getClusterCaName(foo.getMetadata().getName())), any(Secret.class))).thenReturn(Future.succeededFuture());

        // providing the list StatefulSets for already "existing" Kafka clusters
        Labels barLabels = Labels.forCluster("bar");
        KafkaCluster barCluster = KafkaCluster.fromCrd(bar);
        barCluster.generateCertificates(certManager, bar, barSecrets,  null, Collections.EMPTY_MAP);
        when(mockKsOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                asList(barCluster.generateStatefulSet(openShift))
        );
        when(mockSecretOps.list(eq(clusterCmNamespace), eq(barLabels))).thenReturn(
                new ArrayList<>(asList(barCluster.generateClientsCASecret(), barCluster.generateClientsPublicKeySecret(),
                        barCluster.generateBrokersSecret(), barCluster.generateClusterPublicKeySecret()))
        );
        when(mockSecretOps.get(eq(clusterCmNamespace), eq(AbstractModel.getClusterCaName(bar.getMetadata().getName())))).thenReturn(barSecrets.get(0));
        when(mockSecretOps.reconcile(eq(clusterCmNamespace), eq(AbstractModel.getClusterCaName(bar.getMetadata().getName())), any(Secret.class))).thenReturn(Future.succeededFuture());

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();
        Set<String> deleted = new CopyOnWriteArraySet<>();

        KafkaAssemblyOperator ops = new KafkaAssemblyOperator(vertx, openShift,
                ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS,
                certManager,
                supplier) {
            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, Kafka kafkaAssembly) {
                createdOrUpdated.add(kafkaAssembly.getMetadata().getName());
                async.countDown();
                return Future.succeededFuture();
            }
        };

        // Now try to reconcile all the Kafka clusters
        ops.reconcileAll("test", clusterCmNamespace).await();

        async.await();

        context.assertEquals(new HashSet(asList("foo", "bar")), createdOrUpdated);
    }

    private ResourceOperatorSupplier supplierWithMocks() {
        RouteOperator routeOps = openShift ? mock(RouteOperator.class) : null;

        ResourceOperatorSupplier supplier = new ResourceOperatorSupplier(
                mock(ServiceOperator.class), routeOps, mock(ZookeeperSetOperator.class),
                mock(KafkaSetOperator.class), mock(ConfigMapOperator.class), mock(SecretOperator.class),
                mock(PvcOperator.class), mock(DeploymentOperator.class),
                mock(ServiceAccountOperator.class), mock(RoleBindingOperator.class), mock(ClusterRoleBindingOperator.class),
                mock(NetworkPolicyOperator.class), mock(CrdOperator.class));
        when(supplier.serviceAccountOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.roleBindingOperator.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.clusterRoleBindingOperator.reconcile(anyString(), any())).thenReturn(Future.succeededFuture());

        if (openShift) {
            when(supplier.routeOperations.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
            when(supplier.routeOperations.hasAddress(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
            when(supplier.routeOperations.get(anyString(), anyString())).thenAnswer(i -> {
                return new RouteBuilder()
                        .withNewSpec()
                        .withHost(i.getArgument(0) + "." + i.getArgument(1) + ".mydomain.com")
                        .endSpec()
                        .build();
            });
        }

        when(supplier.serviceOperations.hasIngressAddress(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(supplier.serviceOperations.hasNodePort(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(supplier.serviceOperations.get(anyString(), anyString())).thenAnswer(i -> {
            return new ServiceBuilder()
                    .withNewStatus()
                        .withNewLoadBalancer()
                            .withIngress(new LoadBalancerIngressBuilder().withHostname(i.getArgument(0) + "." + i.getArgument(1) + ".mydomain.com").build())
                        .endLoadBalancer()
                    .endStatus()
                    .withNewSpec()
                        .withPorts(new ServicePortBuilder().withNodePort(31245).build())
                    .endSpec()
                    .build();
        });

        return supplier;
    }

    @AfterClass
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
