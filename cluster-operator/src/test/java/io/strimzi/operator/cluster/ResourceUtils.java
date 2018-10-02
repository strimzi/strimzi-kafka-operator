/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.KafkaListenerPlain;
import io.strimzi.api.kafka.model.KafkaListenerTls;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerConsumerSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMakerProducerSpec;
import io.strimzi.api.kafka.model.KafkaSpec;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.api.kafka.model.TopicOperatorSpec;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.TestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class ResourceUtils {

    private ResourceUtils() {

    }

    public static Kafka createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                           String image, int healthDelay, int healthTimeout) {
        Probe probe = new ProbeBuilder()
                .withInitialDelaySeconds(healthDelay)
                .withTimeoutSeconds(healthTimeout)
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
                    .endZookeeper()
                .endSpec().build();
    }

    public static List<Secret> createKafkaClusterInitialSecrets(String clusterCmNamespace, String clusterName) {
        List<Secret> secrets = new ArrayList<>();
        secrets.add(createInitialClusterCaCertSecret(clusterCmNamespace, clusterName,
                MockCertManager.clusterCaCert()));
        secrets.add(createInitialClusterCaKeySecret(clusterCmNamespace, clusterName,
                MockCertManager.clusterCaKey()));
        return secrets;
    }

    public static ClusterCa createInitialClusterCa(String clusterCmNamespace, String clusterName) {
        Secret initialClusterCaCertSecret = createInitialClusterCaCertSecret(clusterCmNamespace, clusterName, MockCertManager.clusterCaCert());
        Secret initialClusterCaKeySecret = createInitialClusterCaKeySecret(clusterCmNamespace, clusterName, MockCertManager.clusterCaKey());
        return createInitialClusterCa(clusterName, initialClusterCaCertSecret, initialClusterCaKeySecret);
    }

    public static ClusterCa createInitialClusterCa(String clusterName, Secret initialClusterCaCert, Secret initialClusterCaKey) {
        return new ClusterCa(new MockCertManager(), clusterName, initialClusterCaCert, initialClusterCaKey);
    }

    public static ClientsCa createInitialClientsCa(String clusterCmNamespace, String clusterName) {
        Secret initialClusterCaCertSecret = createInitialClusterCaCertSecret(clusterCmNamespace, clusterName, MockCertManager.clusterCaCert());
        Secret initialClusterCaKeySecret = createInitialClusterCaKeySecret(clusterCmNamespace, clusterName, MockCertManager.clusterCaKey());
        return createInitialClientsCa(clusterName, initialClusterCaCertSecret, initialClusterCaKeySecret);
    }

    public static ClientsCa createInitialClientsCa(String clusterName, Secret initialClientsCaCert, Secret initialClientsCaKey) {
        return new ClientsCa(new MockCertManager(),
                KafkaCluster.clientsPublicKeyName(clusterName),
                initialClientsCaCert,
                KafkaCluster.clientsCASecretName(clusterName),
                initialClientsCaKey,
                365, 30, true);
    }

    public static Secret createInitialClusterCaCertSecret(String clusterCmNamespace, String clusterName, String caCert) {
        return new SecretBuilder()
        .withNewMetadata()
            .withName(AbstractModel.getClusterCaName(clusterName))
            .withNamespace(clusterCmNamespace)
            .withLabels(Labels.forCluster(clusterName).withKind("Kafka").toMap())
        .endMetadata()
        .addToData("ca.crt", caCert)
        .build();
    }

    public static Secret createInitialClusterCaKeySecret(String clusterCmNamespace, String clusterName, String caKey) {
        return new SecretBuilder()
        .withNewMetadata()
            .withName(AbstractModel.getClusterCaKeyName(clusterName))
            .withNamespace(clusterCmNamespace)
            .withLabels(Labels.forCluster(clusterName).withKind("Kafka").toMap())
        .endMetadata()
        .addToData("ca.key", caKey)
        .build();
    }

    public static List<Secret> createKafkaClusterSecretsWithReplicas(String clusterCmNamespace, String clusterName, int kafkaReplicas, int zkReplicas) {
        List<Secret> secrets = new ArrayList<>();

        secrets.add(createInitialClusterCaKeySecret(clusterCmNamespace, clusterName,
                MockCertManager.clusterCaKey()));
        secrets.add(createInitialClusterCaCertSecret(clusterCmNamespace, clusterName,
                MockCertManager.clusterCaCert()));

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.clientsCASecretName(clusterName))
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
                        .withName(KafkaCluster.clientsPublicKeyName(clusterName))
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
                            .withName(KafkaCluster.clusterPublicKeyName(clusterName))
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
                                           Storage storage,
                                           TopicOperatorSpec topicOperatorSpec,
                                           Logging kafkaLogging, Logging zkLogging) {
        Kafka result = new Kafka();
        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace(clusterCmNamespace);
        meta.setName(clusterCmName);
        meta.setLabels(Labels.userLabels(singletonMap("my-user-label", "cromulent")).toMap());
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
        kafkaClusterSpec.setLivenessProbe(livenessProbe);
        kafkaClusterSpec.setReadinessProbe(livenessProbe);
        ObjectMapper om = new ObjectMapper();
        if (metricsCm != null) {
            kafkaClusterSpec.setMetrics(metricsCm);
        }
        if (kafkaConfiguration != null) {
            kafkaClusterSpec.setConfig(kafkaConfiguration);
        }
        kafkaClusterSpec.setStorage(storage);
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
        zk.setStorage(storage);
        if (metricsCm != null) {
            zk.setMetrics(metricsCm);
        }

        spec.setTopicOperator(topicOperatorSpec);

        spec.setZookeeper(zk);
        result.setSpec(spec);
        return result;
    }


    /**
     * Generate ConfigMap for Kafka Connect S2I cluster
     */
    public static KafkaConnectS2I createKafkaConnectS2ICluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                                       String image, int healthDelay, int healthTimeout, String metricsCmJson,
                                                                       String connectConfig, boolean insecureSourceRepo, String bootstrapServers) {

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
                .withLabels(TestUtils.map(Labels.STRIMZI_KIND_LABEL, "cluster",
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
                        .withLabels(TestUtils.map(Labels.STRIMZI_KIND_LABEL, "cluster",
                                "my-user-label", "cromulent"))
                        .build())
                .withNewSpec().endSpec()
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
                        .withLabels(TestUtils.map(Labels.STRIMZI_KIND_LABEL, "cluster",
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
                        .withLabels(TestUtils.map(Labels.STRIMZI_KIND_LABEL, "cluster",
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
}
