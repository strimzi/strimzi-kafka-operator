/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.operator.cluster.ClusterOperatorConfig.ClusterOperatorConfigBuilder;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.operator.assembly.BrokersInUseCheck;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClient;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.BuildConfigOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.BuildOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ClusterRoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ImageStreamOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.IngressOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NodeOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RoleOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RouteOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StorageClassOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.server.common.MetadataVersion;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({
    "checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:ClassFanOutComplexity"
})
public class ResourceUtils {
    private ResourceUtils() { }

    public static Secret createInitialCaCertSecret(String clusterNamespace, String clusterName, String secretName,
                                                   String caCert, String caStore, String caStorePassword) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(clusterNamespace)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0")
                    .withLabels(Labels.forStrimziCluster(clusterName).withStrimziKind(Kafka.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData("ca.crt", caCert)
                .addToData("ca.p12", caStore)
                .addToData("ca.password", caStorePassword)
                .build();
    }

    public static Secret createInitialCaKeySecret(String clusterNamespace, String clusterName, String secretName, String caKey) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(clusterNamespace)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0")
                    .withLabels(Labels.forStrimziCluster(clusterName).withStrimziKind(Kafka.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData("ca.key", caKey)
                .build();
    }

    /**
     * Create an empty Kafka Connect custom resource
     */
    public static KafkaConnect createEmptyKafkaConnect(String namespace, String name) {
        return new KafkaConnectBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(Map.of(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .withAnnotations(emptyMap())
                        .build())
                .withNewSpec()
                .endSpec()
                .build();
    }

    /**
     * Create an empty Kafka Bridge custom resource
     */
    public static KafkaBridge createEmptyKafkaBridge(String namespace, String name) {
        return new KafkaBridgeBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(Map.of(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withNewHttp(8080)
                .endSpec()
                .build();
    }

    public static KafkaBridge createKafkaBridge(String namespace, String name, String image, int replicas, String bootstrapServers, KafkaBridgeProducerSpec producer, KafkaBridgeConsumerSpec consumer, KafkaBridgeHttpConfig http, boolean enableMetrics) {
        return new KafkaBridgeBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(Map.of(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withImage(image)
                    .withReplicas(replicas)
                    .withBootstrapServers(bootstrapServers)
                    .withProducer(producer)
                    .withConsumer(consumer)
                    .withEnableMetrics(enableMetrics)
                    .withHttp(http)
                .endSpec()
                .build();
    }

    /**
     * Create an empty Kafka MirrorMaker 2 custom resource
     */
    public static KafkaMirrorMaker2 createEmptyKafkaMirrorMaker2(String namespace, String name, Integer replicas) {
        KafkaMirrorMaker2Builder kafkaMirrorMaker2Builder = new KafkaMirrorMaker2Builder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(Map.of(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withClusters(List.of())
                    .withMirrors(List.of())
                .endSpec();

        if (replicas != null) {
            kafkaMirrorMaker2Builder
                    .editOrNewSpec()
                        .withReplicas(replicas)
                    .endSpec();
        }

        return kafkaMirrorMaker2Builder.build();
    }

    public static KafkaMirrorMaker2 createEmptyKafkaMirrorMaker2(String namespace, String name) {
        return createEmptyKafkaMirrorMaker2(namespace, name, null);
    }

    public static void cleanUpTemporaryTLSFiles() {
        String tmpString = "/tmp";
        try (Stream<Path> files = Files.list(Paths.get(tmpString))) {
            files.filter(path -> path.toString().startsWith(tmpString + "/tls")).forEach(delPath -> {
                try {
                    Files.deleteIfExists(delPath);
                } catch (IOException e) {
                    // Nothing to do
                }
            });
        } catch (IOException e) {
            // Nothing to do
        }
    }

    public static Admin adminClient()   {
        Admin mock = mock(AdminClient.class);
        DescribeClusterResult dcr;
        try {
            Constructor<DescribeClusterResult> declaredConstructor = DescribeClusterResult.class.getDeclaredConstructor(KafkaFuture.class, KafkaFuture.class, KafkaFuture.class, KafkaFuture.class);
            declaredConstructor.setAccessible(true);
            KafkaFuture<Node> objectKafkaFuture = KafkaFuture.completedFuture(new Node(0, "localhost", 9091));
            KafkaFuture<String> stringKafkaFuture = KafkaFuture.completedFuture("CLUSTERID");
            dcr = declaredConstructor.newInstance(null, objectKafkaFuture, stringKafkaFuture, null);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        when(mock.describeCluster()).thenReturn(dcr);

        ListTopicsResult ltr;
        try {
            Constructor<ListTopicsResult> declaredConstructor = ListTopicsResult.class.getDeclaredConstructor(KafkaFuture.class);
            declaredConstructor.setAccessible(true);
            KafkaFuture<Map<String, TopicListing>> future = KafkaFuture.completedFuture(emptyMap());
            ltr = declaredConstructor.newInstance(future);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        when(mock.listTopics(any())).thenReturn(ltr);

        DescribeTopicsResult dtr;
        try {
            Constructor<DescribeTopicsResult> declaredConstructor = DescribeTopicsResult.class.getDeclaredConstructor(Map.class, Map.class);
            declaredConstructor.setAccessible(true);
            dtr = declaredConstructor.newInstance(null, emptyMap());
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        when(mock.describeTopics(anyCollection())).thenReturn(dtr);

        DescribeConfigsResult dcfr;
        try {
            Constructor<DescribeConfigsResult> declaredConstructor = DescribeConfigsResult.class.getDeclaredConstructor(Map.class);
            declaredConstructor.setAccessible(true);
            dcfr = declaredConstructor.newInstance(emptyMap());
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        when(mock.describeConfigs(any())).thenReturn(dcfr);

        // Mocks the describeFeatures() call used in KRaft to manage metadata version
        DescribeFeaturesResult dfr;
        try {
            Constructor<DescribeFeaturesResult> declaredConstructor = DescribeFeaturesResult.class.getDeclaredConstructor(KafkaFuture.class);
            declaredConstructor.setAccessible(true);
            Constructor<FeatureMetadata> declaredConstructor2 = FeatureMetadata.class.getDeclaredConstructor(Map.class, Optional.class, Map.class);
            declaredConstructor2.setAccessible(true);
            Constructor<FinalizedVersionRange> declaredConstructor3 = FinalizedVersionRange.class.getDeclaredConstructor(Short.TYPE, Short.TYPE);
            declaredConstructor3.setAccessible(true);

            short metadataLevel = MetadataVersion.fromVersionString(KafkaVersionTestUtils.getKafkaVersionLookup().defaultVersion().metadataVersion()).featureLevel();
            FinalizedVersionRange finalizedVersionRange = declaredConstructor3.newInstance(metadataLevel, metadataLevel);
            FeatureMetadata featureMetadata = declaredConstructor2.newInstance(Map.of(MetadataVersion.FEATURE_NAME, finalizedVersionRange), Optional.empty(), Map.of());
            KafkaFuture<FeatureMetadata> kafkaFuture = KafkaFutureImpl.completedFuture(featureMetadata);
            dfr = declaredConstructor.newInstance(kafkaFuture);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        when(mock.describeFeatures()).thenReturn(dfr);

        DescribeMetadataQuorumResult dmqr;
        try {
            Constructor<DescribeMetadataQuorumResult> declaredConstructor = DescribeMetadataQuorumResult.class.getDeclaredConstructor(KafkaFuture.class);
            QuorumInfo qrminfo = mock(QuorumInfo.class);
            KafkaFuture<QuorumInfo> future = KafkaFuture.completedFuture(qrminfo);
            when(qrminfo.leaderId()).thenReturn(0);
            QuorumInfo.ReplicaState replicaState0 = mock(QuorumInfo.ReplicaState.class);
            QuorumInfo.ReplicaState replicaState1 = mock(QuorumInfo.ReplicaState.class);
            QuorumInfo.ReplicaState replicaState2 = mock(QuorumInfo.ReplicaState.class);
            when(qrminfo.voters()).thenReturn(List.of(replicaState0, replicaState1, replicaState2));
            when(replicaState0.replicaId()).thenReturn(0);
            when(replicaState1.replicaId()).thenReturn(1);
            when(replicaState2.replicaId()).thenReturn(2);
            when(replicaState0.lastCaughtUpTimestamp()).thenReturn(OptionalLong.of(0L));
            when(replicaState1.lastCaughtUpTimestamp()).thenReturn(OptionalLong.of(0L));
            when(replicaState2.lastCaughtUpTimestamp()).thenReturn(OptionalLong.of(0L));
            declaredConstructor.setAccessible(true);
            dmqr = declaredConstructor.newInstance(future);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        when(mock.describeMetadataQuorum()).thenReturn(dmqr);

        DescribeClientQuotasResult dcqr;
        try {
            Constructor<DescribeClientQuotasResult> declaredConstructor = DescribeClientQuotasResult.class.getDeclaredConstructor(KafkaFuture.class);

            final ClientQuotaEntity defaultUserEntity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, null));
            KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> future = KafkaFuture.completedFuture(Map.of(defaultUserEntity, Map.of()));

            declaredConstructor.setAccessible(true);
            dcqr = declaredConstructor.newInstance(future);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        when(mock.describeClientQuotas(any())).thenReturn(dcqr);

        // Mock KRaft node unregistration
        UnregisterBrokerResult ubr = mock(UnregisterBrokerResult.class);
        when(ubr.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(mock.unregisterBroker(anyInt())).thenReturn(ubr);

        return mock;
    }

    public static AdminClientProvider adminClientProvider() {
        return adminClientProvider(adminClient());
    }

    public static AdminClientProvider adminClientProvider(Admin mockAdminClient) {
        return new AdminClientProvider() {
            @Override
            public Admin createAdminClient(String bootstrapHostnames, PemTrustSet kafkaCaTrustSet, PemAuthIdentity authIdentity) {
                return createAdminClient(bootstrapHostnames, kafkaCaTrustSet, authIdentity, new Properties());
            }

            @Override
            public Admin createAdminClient(String bootstrapHostnames, PemTrustSet kafkaCaTrustSet, PemAuthIdentity authIdentity, Properties config) {
                return mockAdminClient;
            }
        };
    }

    public static KafkaAgentClient kafkaAgentClient() {
        return mock(KafkaAgentClient.class);
    }

    public static KafkaAgentClientProvider kafkaAgentClientProvider() {
        return kafkaAgentClientProvider(kafkaAgentClient());
    }

    public static KafkaAgentClientProvider kafkaAgentClientProvider(KafkaAgentClient mockKafkaAgentClient) {
        return (reconciliation, tlsPemIdentity) -> mockKafkaAgentClient;
    }

    public static MetricsProvider metricsProvider() {
        return new MicrometerMetricsProvider(new SimpleMeterRegistry());
    }

    @SuppressWarnings("unchecked")
    public static ResourceOperatorSupplier supplierWithMocks(boolean openShift) {
        RouteOperator routeOps = openShift ? mock(RouteOperator.class) : null;
        ImageStreamOperator imageOps = openShift ? mock(ImageStreamOperator.class) : null;

        ResourceOperatorSupplier supplier = new ResourceOperatorSupplier(
                mock(ServiceOperator.class),
                routeOps,
                imageOps,
                mock(ConfigMapOperator.class),
                mock(SecretOperator.class),
                mock(PvcOperator.class),
                mock(DeploymentOperator.class),
                mock(ServiceAccountOperator.class),
                mock(RoleBindingOperator.class),
                mock(RoleOperator.class),
                mock(ClusterRoleBindingOperator.class),
                mock(NetworkPolicyOperator.class),
                mock(PodDisruptionBudgetOperator.class),
                mock(PodOperator.class),
                mock(IngressOperator.class),
                mock(BuildConfigOperator.class),
                mock(BuildOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(CrdOperator.class),
                mock(StrimziPodSetOperator.class),
                mock(StorageClassOperator.class),
                mock(NodeOperator.class),
                kafkaAgentClientProvider(),
                metricsProvider(),
                adminClientProvider(),
                mock(KubernetesRestartEventPublisher.class),
                new MockSharedEnvironmentProvider(),
                mock(BrokersInUseCheck.class));

        when(supplier.secretOperations.getAsync(any(), any())).thenReturn(Future.succeededFuture());
        when(supplier.secretOperations.getAsync(any(), or(endsWith("ca-cert"), endsWith("certs")))).thenReturn(Future.succeededFuture(
                new SecretBuilder()
                        .withNewMetadata()
                            .withName("cert-secret")
                            .withNamespace("namespace")
                        .endMetadata()
                        .addToData("cluster-operator.key", "key")
                        .addToData("cluster-operator.crt", "cert")
                        .build()));
        when(supplier.serviceAccountOperations.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.roleBindingOperations.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.roleOperations.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.clusterRoleBindingOperator.reconcile(any(), anyString(), any())).thenReturn(Future.succeededFuture());

        if (openShift) {
            when(supplier.routeOperations.reconcile(any(), anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
            when(supplier.routeOperations.hasAddress(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
            when(supplier.routeOperations.get(anyString(), anyString())).thenAnswer(i -> new RouteBuilder()
                    .withNewStatus()
                    .addNewIngress()
                    .withHost(i.getArgument(0) + "." + i.getArgument(1) + ".mydomain.com")
                    .endIngress()
                    .endStatus()
                    .build());
        }

        when(supplier.serviceOperations.hasIngressAddress(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(supplier.serviceOperations.hasNodePort(any(), anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(supplier.serviceOperations.get(anyString(), anyString())).thenAnswer(i ->
             new ServiceBuilder()
                    .withNewStatus()
                        .withNewLoadBalancer()
                            .withIngress(new LoadBalancerIngressBuilder().withHostname(i.getArgument(0) + "." + i.getArgument(1) + ".mydomain.com").build())
                        .endLoadBalancer()
                    .endStatus()
                    .withNewSpec()
                        .withPorts(new ServicePortBuilder().withNodePort(31245).build())
                    .endSpec()
                    .build());

        return supplier;
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig() {

        Map<String, String> envVars = new HashMap<>();

        return ClusterOperatorConfig.buildFromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(KafkaVersion.Lookup versions) {
        return new ClusterOperatorConfigBuilder(dummyClusterOperatorConfig(), versions)
                .build();
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(String featureGates) {
        return new ClusterOperatorConfigBuilder(dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup())
                .with(ClusterOperatorConfig.FEATURE_GATES.key(), featureGates)
                .build();
    }
}
