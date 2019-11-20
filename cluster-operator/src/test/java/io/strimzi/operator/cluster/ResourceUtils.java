/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.RouteBuilder;
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
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.TopicOperatorSpec;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.listener.KafkaListenerPlain;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.AdminClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentConfigOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
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

    public static Kafka createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                           String image, int healthDelay, int healthTimeout) {
        Probe probe = new ProbeBuilder()
                .withInitialDelaySeconds(healthDelay)
                .withTimeoutSeconds(healthTimeout)
                .withFailureThreshold(10)
                .withSuccessThreshold(4)
                .withPeriodSeconds(33)
                .build();

        ObjectMetaBuilder meta = new ObjectMetaBuilder();
        meta.withNamespace(clusterCmNamespace);
        meta.withName(clusterCmName);
        meta.withLabels(Labels.userLabels(singletonMap("my-user-label", "cromulent")).toMap());
        KafkaBuilder builder = new KafkaBuilder();
        return builder.withMetadata(meta.build())
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(replicas)
                        .withImage(image)
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

    public static Kafka createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                           String image, int healthDelay, int healthTimeout,
                                           Map<String, Object> metricsCm,
                                           Map<String, Object> kafkaConfigurationJson,
                                           Map<String, Object> zooConfigurationJson) {
        return new KafkaBuilder(createKafkaCluster(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout)).editSpec()
                    .editKafka()
                        .withMetrics(metricsCm)
                        .withConfig(kafkaConfigurationJson)
                    .endKafka()
                    .editZookeeper()
                        .withConfig(zooConfigurationJson)
                        .withMetrics(metricsCm)
                    .endZookeeper()
                .endSpec().build();
    }

    public static List<Secret> createKafkaClusterInitialSecrets(String clusterNamespace, String clusterName) {
        List<Secret> secrets = new ArrayList<>();
        secrets.add(createInitialCaCertSecret(clusterNamespace, clusterName,
                AbstractModel.clusterCaCertSecretName(clusterName), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"));
        secrets.add(createInitialCaKeySecret(clusterNamespace, clusterName,
                AbstractModel.clusterCaKeySecretName(clusterName), MockCertManager.clusterCaKey()));
        return secrets;
    }

    public static ClusterCa createInitialClusterCa(String clusterNamespace, String clusterName) {
        Secret initialClusterCaCertSecret = createInitialCaCertSecret(clusterNamespace, clusterName,
                AbstractModel.clusterCaCertSecretName(clusterName), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456");
        Secret initialClusterCaKeySecret = createInitialCaKeySecret(clusterNamespace, clusterName,
                AbstractModel.clusterCaKeySecretName(clusterName), MockCertManager.clusterCaKey());
        return createInitialClusterCa(clusterName, initialClusterCaCertSecret, initialClusterCaKeySecret);
    }

    public static ClusterCa createInitialClusterCa(String clusterName, Secret initialClusterCaCert, Secret initialClusterCaKey) {
        return new ClusterCa(new MockCertManager(), new PasswordGenerator(10, "a", "a"), clusterName, initialClusterCaCert, initialClusterCaKey);
    }

    public static ClientsCa createInitialClientsCa(String clusterNamespace, String clusterName) {
        Secret initialClientsCaCert = createInitialCaCertSecret(clusterNamespace, clusterName,
                KafkaCluster.clientsCaCertSecretName(clusterName), MockCertManager.clientsCaCert(), MockCertManager.clientsCaCertStore(), "123456");
        Secret initialClientsCaKey = createInitialCaKeySecret(clusterNamespace, clusterName,
                KafkaCluster.clientsCaKeySecretName(clusterName), MockCertManager.clientsCaKey());
        return createInitialClientsCa(clusterName, initialClientsCaCert, initialClientsCaKey);
    }

    public static ClientsCa createInitialClientsCa(String clusterName, Secret initialClientsCaCert, Secret initialClientsCaKey) {
        return new ClientsCa(new MockCertManager(), new PasswordGenerator(10, "a", "a"),
                KafkaCluster.clientsCaCertSecretName(clusterName),
                initialClientsCaCert,
                KafkaCluster.clientsCaKeySecretName(clusterName),
                initialClientsCaKey,
                365, 30, true, null);
    }

    public static Secret createInitialCaCertSecret(String clusterNamespace, String clusterName, String secretName,
                                                   String caCert, String caStore, String caStorePassword) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(clusterNamespace)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0")
                    .withLabels(Labels.forCluster(clusterName).withKind(Kafka.RESOURCE_KIND).toMap())
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
                    .withLabels(Labels.forCluster(clusterName).withKind(Kafka.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData("ca.key", caKey)
                .build();
    }

    public static List<Secret> createKafkaClusterSecretsWithReplicas(String clusterCmNamespace, String clusterName, int kafkaReplicas, int zkReplicas) {
        List<Secret> secrets = new ArrayList<>();

        secrets.add(createInitialCaKeySecret(clusterCmNamespace, clusterName,
                AbstractModel.clusterCaKeySecretName(clusterName), MockCertManager.clusterCaKey()));
        secrets.add(createInitialCaCertSecret(clusterCmNamespace, clusterName,
                AbstractModel.clusterCaCertSecretName(clusterName), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"));

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.clientsCaKeySecretName(clusterName))
                        .withNamespace(clusterCmNamespace)
                        .withLabels(Labels.forCluster(clusterName).toMap())
                        .endMetadata()
                        .addToData("clients-ca.key", MockCertManager.clientsCaKey())
                        .addToData("clients-ca.crt", MockCertManager.clientsCaCert())
                        .build()
        );

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.clientsCaCertSecretName(clusterName))
                        .withNamespace(clusterCmNamespace)
                        .withLabels(Labels.forCluster(clusterName).toMap())
                        .endMetadata()
                        .addToData("clients-ca.crt", MockCertManager.clientsCaCert())
                        .build()
        );

        SecretBuilder builder =
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.brokersSecretName(clusterName))
                        .withNamespace(clusterCmNamespace)
                        .withLabels(Labels.forCluster(clusterName).toMap())
                        .endMetadata()
                        .addToData("cluster-ca.crt", MockCertManager.clusterCaCert());

        for (int i = 0; i < kafkaReplicas; i++) {
            builder.addToData(KafkaCluster.kafkaPodName(clusterName, i) + ".key", Base64.getEncoder().encodeToString("brokers-internal-base64key".getBytes()))
                    .addToData(KafkaCluster.kafkaPodName(clusterName, i) + ".crt", Base64.getEncoder().encodeToString("brokers-internal-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        builder = new SecretBuilder()
                        .withNewMetadata()
                            .withName(KafkaCluster.clusterCaCertSecretName(clusterName))
                            .withNamespace(clusterCmNamespace)
                            .withLabels(Labels.forCluster(clusterName).toMap())
                        .endMetadata()
                        .addToData("ca.crt", Base64.getEncoder().encodeToString("cluster-ca-base64crt".getBytes()));

        for (int i = 0; i < kafkaReplicas; i++) {
            builder.addToData(KafkaCluster.kafkaPodName(clusterName, i) + ".key", Base64.getEncoder().encodeToString("brokers-clients-base64key".getBytes()))
                    .addToData(KafkaCluster.kafkaPodName(clusterName, i) + ".crt", Base64.getEncoder().encodeToString("brokers-clients-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        builder = new SecretBuilder()
                        .withNewMetadata()
                            .withName(ZookeeperCluster.nodesSecretName(clusterName))
                            .withNamespace(clusterCmNamespace)
                            .withLabels(Labels.forCluster(clusterName).toMap())
                        .endMetadata()
                        .addToData("cluster-ca.crt", Base64.getEncoder().encodeToString("cluster-ca-base64crt".getBytes()));

        for (int i = 0; i < zkReplicas; i++) {
            builder.addToData(ZookeeperCluster.zookeeperPodName(clusterName, i) + ".key", Base64.getEncoder().encodeToString("nodes-base64key".getBytes()))
                    .addToData(ZookeeperCluster.zookeeperPodName(clusterName, i) + ".crt", Base64.getEncoder().encodeToString("nodes-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        return secrets;
    }

    public static Kafka createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                           String image, int healthDelay, int healthTimeout,
                                           Map<String, Object> metricsCm,
                                           Map<String, Object> kafkaConfigurationJson,
                                           Logging kafkaLogging, Logging zkLogging) {
        return new KafkaBuilder(createKafkaCluster(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCm, kafkaConfigurationJson, emptyMap()))
                .editSpec()
                .editKafka()
                    .withLogging(kafkaLogging)
                    .withNewListeners()
                        .withPlain(new KafkaListenerPlain())
                        .withTls(new KafkaListenerTls())
                    .endListeners()
                .endKafka()
                .editZookeeper()
                    .withLogging(zkLogging)
                .endZookeeper()
            .endSpec()
        .build();
    }

    public static Kafka createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                           String image, int healthDelay, int healthTimeout,
                                           Map<String, Object> metricsCm,
                                           Map<String, Object> kafkaConfiguration,
                                           Map<String, Object> zooConfiguration,
                                           Storage kafkaStorage,
                                           SingleVolumeStorage zkStorage,
                                           TopicOperatorSpec topicOperatorSpec,
                                           Logging kafkaLogging, Logging zkLogging,
                                           KafkaExporterSpec keSpec) {
        Kafka result = new Kafka();
        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace(clusterCmNamespace);
        meta.setName(clusterCmName);
        meta.setLabels(Labels.userLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests", "my-user-label", "cromulent")).toMap());
        result.setMetadata(meta);

        KafkaSpec spec = new KafkaSpec();

        KafkaClusterSpec kafkaClusterSpec = new KafkaClusterSpec();
        kafkaClusterSpec.setReplicas(replicas);
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
        if (metricsCm != null) {
            kafkaClusterSpec.setMetrics(metricsCm);
        }
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
        if (metricsCm != null) {
            zk.setMetrics(metricsCm);
        }

        spec.setTopicOperator(topicOperatorSpec);
        spec.setKafkaExporter(keSpec);

        spec.setZookeeper(zk);
        result.setSpec(spec);
        return result;
    }


    /**
     * Generate ConfigMap for Kafka Connect S2I cluster
     */
    public static KafkaConnectS2I createKafkaConnectS2ICluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                               String image, int healthDelay, int healthTimeout, String metricsCmJson,
                                                               String connectConfig, boolean insecureSourceRepo, String bootstrapServers,
                                                               ResourceRequirements builResourceRequirements) {

        return new KafkaConnectS2IBuilder(createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName))
                .withNewSpec()
                    .withImage(image)
                    .withReplicas(replicas)
                    .withBootstrapServers(bootstrapServers)
                    .withLivenessProbe(new Probe(healthDelay, healthTimeout))
                    .withReadinessProbe(new Probe(healthDelay, healthTimeout))
                    .withMetrics((Map<String, Object>) TestUtils.fromJson(metricsCmJson, Map.class))
                    .withConfig((Map<String, Object>) TestUtils.fromJson(connectConfig, Map.class))
                    .withInsecureSourceRepository(insecureSourceRepo)
                    .withBuildResources(builResourceRequirements)
                .endSpec().build();
    }

    /**
     * Generate empty Kafka Connect S2I ConfigMap
     */
    public static KafkaConnectS2I createEmptyKafkaConnectS2ICluster(String clusterCmNamespace, String clusterCmName) {
        return new KafkaConnectS2IBuilder()
                .withMetadata(new ObjectMetaBuilder()
                .withName(clusterCmName)
                .withNamespace(clusterCmNamespace)
                .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                        "my-user-label", "cromulent"))
                .build())
                .withNewSpec().endSpec()
                .build();
    }

    /**
     * Generate empty Kafka Connect ConfigMap
     */
    public static KafkaConnect createEmptyKafkaConnectCluster(String clusterCmNamespace, String clusterCmName) {
        return new KafkaConnectBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(clusterCmName)
                        .withNamespace(clusterCmNamespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec().endSpec()
                .build();
    }

    /**
     * Generate empty Kafka Bridge ConfigMap
     */
    public static KafkaBridge createEmptyKafkaBridgeCluster(String clusterCmNamespace, String clusterCmName) {
        return new KafkaBridgeBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(clusterCmName)
                        .withNamespace(clusterCmNamespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                .withNewHttp(8080).endSpec()
                .build();
    }

    /**
     * Generate empty Kafka MirrorMaker ConfigMap
     */
    public static KafkaMirrorMaker createEmptyKafkaMirrorMakerCluster(String clusterCmNamespace, String clusterCmName) {
        return new KafkaMirrorMakerBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(clusterCmName)
                        .withNamespace(clusterCmNamespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec().endSpec()
                .build();
    }

    public static KafkaMirrorMaker createKafkaMirrorMakerCluster(String clusterCmNamespace, String clusterCmName, String image, KafkaMirrorMakerProducerSpec producer, KafkaMirrorMakerConsumerSpec consumer, String whitelist, Map<String, Object> metricsCm) {
        return new KafkaMirrorMakerBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(clusterCmName)
                        .withNamespace(clusterCmNamespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                .withImage(image)
                .withProducer(producer)
                .withConsumer(consumer)
                .withWhitelist(whitelist)
                .withMetrics(metricsCm)
                .endSpec()
                .build();
    }

    public static KafkaBridge createKafkaBridgeCluster(String clusterCmNamespace, String clusterCmName, String image, int replicas, String bootstrapservers, KafkaBridgeProducerSpec producer, KafkaBridgeConsumerSpec consumer, KafkaBridgeHttpConfig http, Map<String, Object> metricsCm) {
        return new KafkaBridgeBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(clusterCmName)
                        .withNamespace(clusterCmNamespace)
                        .withLabels(TestUtils.map(Labels.KUBERNETES_DOMAIN + "part-of", "tests",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec()
                    .withImage(image)
                    .withReplicas(replicas)
                    .withBootstrapServers(bootstrapservers)
                    .withProducer(producer)
                    .withConsumer(consumer)
                    .withMetrics(metricsCm)
                    .withHttp(http)
                .endSpec()
                .build();
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
        return new ZookeeperLeaderFinder(vertx, new SecretOperator(vertx, client),
            () -> new BackOff(5_000, 2, 4)) {
                @Override
                protected Future<Boolean> isLeader(Pod pod, NetClientOptions options) {
                    return Future.succeededFuture(true);
                }

                @Override
                protected PemTrustOptions trustOptions(Secret s) {
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
            public AdminClient createAdminClient(String hostname, Secret clusterCaCertSecret, Secret coKeySecret) {
                AdminClient mock = mock(AdminClient.class);
                DescribeClusterResult dcr;
                try {
                    Constructor<DescribeClusterResult> declaredConstructor = DescribeClusterResult.class.getDeclaredConstructor(KafkaFuture.class, KafkaFuture.class, KafkaFuture.class, KafkaFuture.class);
                    declaredConstructor.setAccessible(true);
                    KafkaFuture<Node> objectKafkaFuture = KafkaFutureImpl.completedFuture(new Node(0, "localhost", 9091));
                    dcr = declaredConstructor.newInstance(null, objectKafkaFuture, null, null);
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
                when(mock.describeTopics(any())).thenReturn(dtr);

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

    public static ResourceOperatorSupplier supplierWithMocks(boolean openShift) {
        RouteOperator routeOps = openShift ? mock(RouteOperator.class) : null;

        ResourceOperatorSupplier supplier = new ResourceOperatorSupplier(
                mock(ServiceOperator.class), routeOps, mock(ZookeeperSetOperator.class),
                mock(KafkaSetOperator.class), mock(ConfigMapOperator.class), mock(SecretOperator.class),
                mock(PvcOperator.class), mock(DeploymentOperator.class),
                mock(ServiceAccountOperator.class), mock(RoleBindingOperator.class), mock(ClusterRoleBindingOperator.class),
                mock(NetworkPolicyOperator.class), mock(PodDisruptionBudgetOperator.class), mock(PodOperator.class),
                mock(IngressOperator.class), mock(ImageStreamOperator.class), mock(BuildConfigOperator.class),
                mock(DeploymentConfigOperator.class), mock(CrdOperator.class), mock(CrdOperator.class), mock(CrdOperator.class),
                mock(CrdOperator.class), mock(CrdOperator.class),
                mock(StorageClassOperator.class));
        when(supplier.serviceAccountOperations.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.roleBindingOperations.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
        when(supplier.clusterRoleBindingOperator.reconcile(anyString(), any())).thenReturn(Future.succeededFuture());

        if (openShift) {
            when(supplier.routeOperations.reconcile(anyString(), anyString(), any())).thenReturn(Future.succeededFuture());
            when(supplier.routeOperations.hasAddress(anyString(), anyString(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
            when(supplier.routeOperations.get(anyString(), anyString())).thenAnswer(i -> {
                return new RouteBuilder()
                        .withNewStatus()
                        .addNewIngress()
                        .withHost(i.getArgument(0) + "." + i.getArgument(1) + ".mydomain.com")
                        .endIngress()
                        .endStatus()
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

    public static ClusterOperatorConfig dummyClusterOperatorConfig(KafkaVersion.Lookup versions, long operationTimeoutMs) {
        ClusterOperatorConfig config = new ClusterOperatorConfig(
                singleton("dummy"),
                60_000,
                operationTimeoutMs,
                false,
                versions,
                null,
                null);

        return config;
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(KafkaVersion.Lookup versions) {
        return dummyClusterOperatorConfig(versions, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig(long operationTimeoutMs) {
        return dummyClusterOperatorConfig(new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap()), operationTimeoutMs);
    }

    public static ClusterOperatorConfig dummyClusterOperatorConfig() {
        return dummyClusterOperatorConfig(new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap()));
    }
}
