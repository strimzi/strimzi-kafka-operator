/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.MetricsConfig;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScaler;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScalerProvider;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.BuildOperator;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RoleOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({
    "checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:ClassFanOutComplexity"
})
public class ResourceUtils {

    private ResourceUtils() {

    }

    public static Kafka createKafka(String namespace, String name, int replicas,
                                    String image, int healthDelay, int healthTimeout) {
        Probe probe = new ProbeBuilder()
                .withInitialDelaySeconds(healthDelay)
                .withTimeoutSeconds(healthTimeout)
                .withFailureThreshold(10)
                .withSuccessThreshold(4)
                .withPeriodSeconds(33)
                .build();

        ObjectMeta meta = new ObjectMetaBuilder()
            .withNamespace(namespace)
            .withName(name)
            .withLabels(Labels.fromMap(singletonMap("my-user-label", "cromulent")).toMap())
            .build();

        KafkaBuilder builder = new KafkaBuilder();
        return builder.withMetadata(meta)
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(replicas)
                        .withImage(image)
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                        .withLivenessProbe(probe)
                        .withReadinessProbe(probe)
                        .withStorage(new EphemeralStorage())
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(replicas)
                        .withImage(image + "-zk")
                        .withLivenessProbe(probe)
                        .withReadinessProbe(probe)
                    .endZookeeper()
                .endSpec()
                .build();
    }

    public static Kafka createKafka(String namespace, String name, int replicas,
                                    String image, int healthDelay, int healthTimeout,
                                    MetricsConfig metricsConfig,
                                    Map<String, Object> kafkaConfigurationJson,
                                    Map<String, Object> zooConfigurationJson) {
        return new KafkaBuilder(createKafka(namespace, name, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editKafka()
                        .withConfig(kafkaConfigurationJson)
                        .withMetricsConfig(metricsConfig)
                    .endKafka()
                    .editZookeeper()
                        .withConfig(zooConfigurationJson)
                        .withMetricsConfig(metricsConfig)
                    .endZookeeper()
                .endSpec()
                .build();
    }

    /**
     * Create a mock Secret for brokers certificates that uses the same cert and key for each replica
     * @param clusterNamespace Namespace of the Kafka cluster
     * @param clusterName Kafka cluster name
     * @param replicas Number of broker replicas
     * @param secretName Name of the secret
     * @param brokerCert Broker x509 certificate
     * @param brokerKey Broker private key
     * @param p12KeyStore P12 keystore for cert & key
     * @param p12KeyStorePassword P12 keystore password
     * @return A Secret object
     */
    public static Secret createMockBrokersCertsSecret(String clusterNamespace, String clusterName, int replicas, String secretName,
                                                      String brokerCert, String brokerKey, String p12KeyStore, String p12KeyStorePassword) {
        Map<String, String> data = new HashMap<>((int) (1 + replicas * 4 / 0.75));
        for (int i = 0; i < replicas; i++) {
            data.put(Ca.secretEntryNameForPod(KafkaResources.kafkaPodName(clusterName, i), Ca.SecretEntry.KEY), brokerKey);
            data.put(Ca.secretEntryNameForPod(KafkaResources.kafkaPodName(clusterName, i), Ca.SecretEntry.CRT), brokerCert);
            data.put(Ca.secretEntryNameForPod(KafkaResources.kafkaPodName(clusterName, i), Ca.SecretEntry.P12_KEYSTORE), p12KeyStore);
            data.put(Ca.secretEntryNameForPod(KafkaResources.kafkaPodName(clusterName, i), Ca.SecretEntry.P12_KEYSTORE_PASSWORD), p12KeyStorePassword);
        }
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(clusterNamespace)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0")
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0")
                    .withLabels(Labels.forStrimziCluster(clusterName).withStrimziKind(Kafka.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData(data)
                .build();
    }

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

    public static Kafka createKafka(String namespace, String name, int replicas,
                                    String image, int healthDelay, int healthTimeout,
                                    MetricsConfig metricsConfig,
                                    Map<String, Object> kafkaConfigurationJson,
                                    Logging kafkaLogging, Logging zkLogging) {
        return new KafkaBuilder(createKafka(namespace, name, replicas, image, healthDelay,
                    healthTimeout, metricsConfig, kafkaConfigurationJson, emptyMap()))
                .editSpec()
                    .editKafka()
                        .withLogging(kafkaLogging)
                    .endKafka()
                    .editZookeeper()
                        .withLogging(zkLogging)
                    .endZookeeper()
                .endSpec()
                .build();
    }

    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public static Kafka createKafka(String namespace, String name, int replicas,
                                    String image, int healthDelay, int healthTimeout,
                                    MetricsConfig metricsConfig,
                                    Map<String, Object> kafkaConfiguration,
                                    Map<String, Object> zooConfiguration,
                                    Storage kafkaStorage,
                                    SingleVolumeStorage zkStorage,
                                    Logging kafkaLogging, Logging zkLogging,
                                    KafkaExporterSpec keSpec,
                                    CruiseControlSpec ccSpec) {

        Kafka result = new Kafka();
        ObjectMeta meta = new ObjectMetaBuilder()
            .withNamespace(namespace)
            .withName(name)
            .withLabels(Labels.fromMap(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests", "my-user-label", "cromulent")).toMap())
            .build();
        result.setMetadata(meta);

        KafkaSpec spec = new KafkaSpec();

        KafkaClusterSpec kafkaClusterSpec = new KafkaClusterSpec();
        kafkaClusterSpec.setReplicas(replicas);
        kafkaClusterSpec.setListeners(singletonList(new GenericKafkaListenerBuilder().withName("plain").withPort(9092).withTls(false).withType(KafkaListenerType.INTERNAL).build()));
        kafkaClusterSpec.setImage(image);
        if (kafkaLogging != null) {
            kafkaClusterSpec.setLogging(kafkaLogging);
        }
        Probe livenessProbe = new Probe();
        livenessProbe.setInitialDelaySeconds(healthDelay);
        livenessProbe.setTimeoutSeconds(healthTimeout);
        livenessProbe.setSuccessThreshold(4);
        livenessProbe.setFailureThreshold(10);
        livenessProbe.setPeriodSeconds(33);
        kafkaClusterSpec.setLivenessProbe(livenessProbe);
        kafkaClusterSpec.setReadinessProbe(livenessProbe);
        kafkaClusterSpec.setMetricsConfig(metricsConfig);

        if (kafkaConfiguration != null) {
            kafkaClusterSpec.setConfig(kafkaConfiguration);
        }
        kafkaClusterSpec.setStorage(kafkaStorage);
        spec.setKafka(kafkaClusterSpec);

        ZookeeperClusterSpec zk = new ZookeeperClusterSpec();
        zk.setReplicas(replicas);
        zk.setImage(image + "-zk");
        if (zkLogging != null) {
            zk.setLogging(zkLogging);
        }
        zk.setLivenessProbe(livenessProbe);
        zk.setReadinessProbe(livenessProbe);
        if (zooConfiguration != null) {
            zk.setConfig(zooConfiguration);
        }
        zk.setStorage(zkStorage);
        zk.setMetricsConfig(metricsConfig);

        spec.setKafkaExporter(keSpec);
        spec.setCruiseControl(ccSpec);
        spec.setZookeeper(zk);
        result.setSpec(spec);

        return result;
    }

    /**
     * Create an empty Kafka Connect custom resource
     */
    public static KafkaConnect createEmptyKafkaConnect(String namespace, String name) {
        return new KafkaConnectBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
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
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withNewHttp(8080)
                .endSpec()
                .build();
    }

    /**
     * Create an empty Kafka MirrorMaker custom resource
     */
    public static KafkaMirrorMaker createEmptyKafkaMirrorMaker(String namespace, String name) {
        return new KafkaMirrorMakerBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                .endSpec()
                .build();
    }

    @SuppressWarnings("deprecation")
    public static KafkaMirrorMaker createKafkaMirrorMaker(String namespace, String name, String image, KafkaMirrorMakerProducerSpec producer,
                                                          KafkaMirrorMakerConsumerSpec consumer, String include) {
        return createKafkaMirrorMaker(namespace, name, image, null, producer, consumer, include);
    }

    @SuppressWarnings("deprecation")
    public static KafkaMirrorMaker createKafkaMirrorMaker(String namespace, String name, String image, Integer replicas,
                                                          KafkaMirrorMakerProducerSpec producer, KafkaMirrorMakerConsumerSpec consumer,
                                                          String include) {

        KafkaMirrorMakerBuilder builder = new KafkaMirrorMakerBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withImage(image)
                    .withProducer(producer)
                    .withConsumer(consumer)
                    .withInclude(include)
                .endSpec();

        if (replicas != null) {
            builder.editOrNewSpec()
                        .withReplicas(replicas)
                    .endSpec();
        }

        return builder.build();
    }

    public static KafkaBridge createKafkaBridge(String namespace, String name, String image, int replicas, String bootstrapservers, KafkaBridgeProducerSpec producer, KafkaBridgeConsumerSpec consumer, KafkaBridgeHttpConfig http, boolean enableMetrics) {
        return new KafkaBridgeBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withImage(image)
                    .withReplicas(replicas)
                    .withBootstrapServers(bootstrapservers)
                    .withProducer(producer)
                    .withConsumer(consumer)
                    .withEnableMetrics(enableMetrics)
                    .withHttp(http)
                .endSpec()
                .build();
    }

    /**
     * Create an empty Kafka MirrorMaker 2.0 custom resource
     */
    public static KafkaMirrorMaker2 createEmptyKafkaMirrorMaker2(String namespace, String name, Integer replicas) {
        KafkaMirrorMaker2Builder kafkaMirrorMaker2Builder = new KafkaMirrorMaker2Builder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec().endSpec();

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
        try {
            Files.list(Paths.get(tmpString)).filter(path -> path.toString().startsWith(tmpString + "/tls")).forEach(delPath -> {
                try {
                    Files.deleteIfExists(delPath);
                } catch (IOException e) {
                }
            });
        } catch (IOException e) {
        }
    }

    public static ZookeeperLeaderFinder zookeeperLeaderFinder(Vertx vertx, KubernetesClient client) {
        return new ZookeeperLeaderFinder(vertx, () -> new BackOff(5_000, 2, 4)) {
                @Override
                protected Future<Boolean> isLeader(Reconciliation reconciliation, String podName, NetClientOptions options) {
                    return Future.succeededFuture(true);
                }

                @Override
                protected PemTrustOptions trustOptions(Reconciliation reconciliation, Secret s) {
                    return new PemTrustOptions();
                }

                @Override
                protected PemKeyCertOptions keyCertOptions(Secret s) {
                    return new PemKeyCertOptions();
                }
            };
    }

    public static AdminClientProvider adminClientProvider() {
        return new AdminClientProvider() {
            @Override
            public Admin createAdminClient(String bootstrapHostnames, Secret clusterCaCertSecret, Secret keyCertSecret, String keyCertName) {
                return createAdminClient(bootstrapHostnames, clusterCaCertSecret, keyCertSecret, keyCertName, new Properties());
            }

            @Override
            public Admin createAdminClient(String bootstrapHostnames, Secret clusterCaCertSecret, Secret keyCertSecret, String keyCertName, Properties config) {
                Admin mock = mock(AdminClient.class);
                DescribeClusterResult dcr;
                try {
                    Constructor<DescribeClusterResult> declaredConstructor = DescribeClusterResult.class.getDeclaredConstructor(KafkaFuture.class, KafkaFuture.class, KafkaFuture.class, KafkaFuture.class);
                    declaredConstructor.setAccessible(true);
                    KafkaFuture<Node> objectKafkaFuture = KafkaFutureImpl.completedFuture(new Node(0, "localhost", 9091));
                    KafkaFuture<String> stringKafkaFuture = KafkaFutureImpl.completedFuture("CLUSTERID");
                    dcr = declaredConstructor.newInstance(null, objectKafkaFuture, stringKafkaFuture, null);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
                when(mock.describeCluster()).thenReturn(dcr);

                ListTopicsResult ltr;
                try {
                    Constructor<ListTopicsResult> declaredConstructor = ListTopicsResult.class.getDeclaredConstructor(KafkaFuture.class);
                    declaredConstructor.setAccessible(true);
                    KafkaFuture<Map<String, TopicListing>> future = KafkaFutureImpl.completedFuture(emptyMap());
                    ltr = declaredConstructor.newInstance(future);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
                when(mock.listTopics(any())).thenReturn(ltr);

                DescribeTopicsResult dtr;
                try {
                    Constructor<DescribeTopicsResult> declaredConstructor = DescribeTopicsResult.class.getDeclaredConstructor(Map.class);
                    declaredConstructor.setAccessible(true);
                    dtr = declaredConstructor.newInstance(emptyMap());
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
                when(mock.describeTopics(any(Collection.class))).thenReturn(dtr);

                DescribeConfigsResult dcfr;
                try {
                    Constructor<DescribeConfigsResult> declaredConstructor = DescribeConfigsResult.class.getDeclaredConstructor(Map.class);
                    declaredConstructor.setAccessible(true);
                    dcfr = declaredConstructor.newInstance(emptyMap());
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
                when(mock.describeConfigs(any())).thenReturn(dcfr);
                return mock;
            }
        };
    }

    public static ZookeeperScalerProvider zookeeperScalerProvider() {
        return (reconciliation, vertx, zookeeperConnectionString, zkNodeAddress, clusterCaCertSecret, coKeySecret, operationTimeoutMs, zkAdminSessionTimoutMs) -> {
            ZookeeperScaler mockZooScaler = mock(ZookeeperScaler.class);
            when(mockZooScaler.scale(anyInt())).thenReturn(Future.succeededFuture());
            return mockZooScaler;
        };
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
                mock(StatefulSetOperator.class),
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
                mock(PodDisruptionBudgetV1Beta1Operator.class),
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
                zookeeperScalerProvider(),
                metricsProvider(),
                adminClientProvider(),
                mock(ZookeeperLeaderFinder.class),
                mock(KubernetesRestartEventPublisher.class));

        when(supplier.secretOperations.getAsync(any(), any())).thenReturn(Future.succeededFuture());
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

    public static ClusterOperatorConfig dummyClusterOperatorConfig(KafkaVersion.Lookup versions, long operationTimeoutMs, String featureGates) {
        return new ClusterOperatorConfig(
                singleton("dummy"),
                60_000,
                operationTimeoutMs,
                300_000,
                false,
                true,
                versions,
                null,
                null,
                null,
                null,
                null,
                featureGates,
                10,
                10_000,
                30,
                false,
                1024,
                "cluster-operator-name",
                ClusterOperatorConfig.DEFAULT_POD_SECURITY_PROVIDER_CLASS, null);
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(KafkaVersion.Lookup versions, long operationTimeoutMs) {
        return dummyClusterOperatorConfig(versions, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "");
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(KafkaVersion.Lookup versions) {
        return dummyClusterOperatorConfig(versions, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "");
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(long operationTimeoutMs) {
        return dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup(), operationTimeoutMs, "");
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig() {
        return dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup(), ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "");
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(String featureGates) {
        return dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup(), ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, featureGates);
    }

    /**
     * Find the first resource in the given resources with the given name.
     * @param resources The resources to search.
     * @param name The name of the resource.
     * @return The first resource with that name. Names should be unique per namespace.
     */
    public static <T extends HasMetadata> T findResourceWithName(List<T> resources, String name) {
        return resources.stream()
                .filter(s -> s.getMetadata().getName().equals(name)).findFirst()
                .orElse(null);
    }
}
