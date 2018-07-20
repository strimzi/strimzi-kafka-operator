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
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.KafkaAssemblyBuilder;
import io.strimzi.api.kafka.model.KafkaAssemblySpec;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;
import io.strimzi.api.kafka.model.KafkaConnectAssemblyBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2IAssembly;
import io.strimzi.api.kafka.model.KafkaConnectS2IAssemblyBuilder;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.api.kafka.model.TopicOperator;
import io.strimzi.api.kafka.model.Zookeeper;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.test.TestUtils;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class ResourceUtils {

    private ResourceUtils() {

    }

    public static KafkaAssembly createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                   String image, int healthDelay, int healthTimeout) {
        Probe probe = new ProbeBuilder()
                .withInitialDelaySeconds(healthDelay)
                .withTimeoutSeconds(healthTimeout)
                .build();

        ObjectMetaBuilder meta = new ObjectMetaBuilder();
        meta.withNamespace(clusterCmNamespace);
        meta.withName(clusterCmName);
        meta.withLabels(Labels.userLabels(singletonMap("my-user-label", "cromulent")).toMap());
        KafkaAssemblyBuilder builder = new KafkaAssemblyBuilder();
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

    public static KafkaAssembly createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                        String image, int healthDelay, int healthTimeout,
                                                        Map<String, Object> metricsCm,
                                                        Map<String, Object> kafkaConfigurationJson,
                                                        Map<String, Object> zooConfigurationJson) {
        return new KafkaAssemblyBuilder(createKafkaCluster(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
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

        secrets.add(
                new SecretBuilder()
                .withNewMetadata()
                    .withName(AbstractModel.getClusterCaName(clusterName))
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .addToData("cluster-ca.key", Base64.getEncoder().encodeToString("cluster-ca-base64key".getBytes()))
                .addToData("cluster-ca.crt", Base64.getEncoder().encodeToString("cluster-ca-base64crt".getBytes()))
                .build()
        );
        return secrets;
    }

    public static List<Secret> createKafkaClusterSecretsWithReplicas(String clusterCmNamespace, String clusterName, int kafkaReplicas, int zkReplicas) {
        List<Secret> secrets = new ArrayList<>();

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(AbstractModel.getClusterCaName(clusterName))
                        .withNamespace(clusterCmNamespace)
                        .endMetadata()
                        .addToData("cluster-ca.key", Base64.getEncoder().encodeToString("cluster-ca-base64key".getBytes()))
                        .addToData("cluster-ca.crt", Base64.getEncoder().encodeToString("cluster-ca-base64crt".getBytes()))
                        .build()
        );

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.clientsCASecretName(clusterName))
                        .withNamespace(clusterCmNamespace)
                        .withLabels(Labels.forCluster(clusterName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .addToData("clients-ca.key", Base64.getEncoder().encodeToString("clients-ca-base64key".getBytes()))
                        .addToData("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-base64crt".getBytes()))
                        .build()
        );

        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.clientsPublicKeyName(clusterName))
                        .withNamespace(clusterCmNamespace)
                        .withLabels(Labels.forCluster(clusterName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .addToData("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-base64crt".getBytes()))
                        .build()
        );

        SecretBuilder builder =
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(KafkaCluster.brokersSecretName(clusterName))
                        .withNamespace(clusterCmNamespace)
                        .withLabels(Labels.forCluster(clusterName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .addToData("cluster-ca.crt", Base64.getEncoder().encodeToString("cluster-ca-base64crt".getBytes()));

        for (int i = 0; i < kafkaReplicas; i++) {
            builder.addToData(KafkaCluster.kafkaPodName(clusterName, i) + ".key", Base64.getEncoder().encodeToString("brokers-internal-base64key".getBytes()))
                    .addToData(KafkaCluster.kafkaPodName(clusterName, i) + ".crt", Base64.getEncoder().encodeToString("brokers-internal-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        builder = new SecretBuilder()
                        .withNewMetadata()
                            .withName(KafkaCluster.clusterPublicKeyName(clusterName))
                            .withNamespace(clusterCmNamespace)
                            .withLabels(Labels.forCluster(clusterName).withType(AssemblyType.KAFKA).toMap())
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
                            .withLabels(Labels.forCluster(clusterName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .addToData("cluster-ca.crt", Base64.getEncoder().encodeToString("cluster-ca-base64crt".getBytes()));

        for (int i = 0; i < zkReplicas; i++) {
            builder.addToData(ZookeeperCluster.zookeeperPodName(clusterName, i) + ".key", Base64.getEncoder().encodeToString("nodes-base64key".getBytes()))
                    .addToData(ZookeeperCluster.zookeeperPodName(clusterName, i) + ".crt", Base64.getEncoder().encodeToString("nodes-base64crt".getBytes()));
        }
        secrets.add(builder.build());

        return secrets;
    }

    public static KafkaAssembly createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                   String image, int healthDelay, int healthTimeout,
                                                   Map<String, Object> metricsCm,
                                                   Map<String, Object> kafkaConfigurationJson,
                                                   Logging kafkaLogging, Logging zkLogging) {
        return new KafkaAssemblyBuilder(createKafkaCluster(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCm, kafkaConfigurationJson, emptyMap()))
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

    public static KafkaAssembly createKafkaCluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                   String image, int healthDelay, int healthTimeout,
                                                   Map<String, Object> metricsCm,
                                                   Map<String, Object> kafkaConfiguration,
                                                   Map<String, Object> zooConfiguration,
                                                   Storage storage,
                                                   TopicOperator topicOperator,
                                                   Logging kafkaLogging, Logging zkLogging) {
        KafkaAssembly result = new KafkaAssembly();
        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace(clusterCmNamespace);
        meta.setName(clusterCmName);
        meta.setLabels(Labels.userLabels(singletonMap("my-user-label", "cromulent")).toMap());
        result.setMetadata(meta);

        KafkaAssemblySpec spec = new KafkaAssemblySpec();

        Kafka kafka = new Kafka();
        kafka.setReplicas(replicas);
        kafka.setImage(image);
        if (kafkaLogging != null) {
            kafka.setLogging(kafkaLogging);
        }
        Probe livenessProbe = new Probe();
        livenessProbe.setInitialDelaySeconds(healthDelay);
        livenessProbe.setTimeoutSeconds(healthTimeout);
        kafka.setLivenessProbe(livenessProbe);
        kafka.setReadinessProbe(livenessProbe);
        ObjectMapper om = new ObjectMapper();
        if (metricsCm != null) {
            kafka.setMetrics(metricsCm);
        }
        if (kafkaConfiguration != null) {
            kafka.setConfig(kafkaConfiguration);
        }
        kafka.setStorage(storage);
        spec.setKafka(kafka);

        Zookeeper zk = new Zookeeper();
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

        spec.setTopicOperator(topicOperator);

        spec.setZookeeper(zk);
        result.setSpec(spec);
        return result;
    }


    /**
     * Generate ConfigMap for Kafka Connect S2I cluster
     */
    public static KafkaConnectS2IAssembly createKafkaConnectS2ICluster(String clusterCmNamespace, String clusterCmName, int replicas,
                                                                       String image, int healthDelay, int healthTimeout, String metricsCmJson,
                                                                       String connectConfig, boolean insecureSourceRepo) {

        return new KafkaConnectS2IAssemblyBuilder(createEmptyKafkaConnectS2ICluster(clusterCmNamespace, clusterCmName))
                .withNewSpec()
                    .withImage(image)
                    .withReplicas(replicas)
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
    public static KafkaConnectS2IAssembly createEmptyKafkaConnectS2ICluster(String clusterCmNamespace, String clusterCmName) {
        return new KafkaConnectS2IAssemblyBuilder()
                .withMetadata(new ObjectMetaBuilder()
                .withName(clusterCmName)
                .withNamespace(clusterCmNamespace)
                .withLabels(TestUtils.map(Labels.STRIMZI_KIND_LABEL, "cluster", Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i", "my-user-label", "cromulent"))
                .build())
                .withNewSpec().endSpec()
                .build();
    }

    /**
     * Generate empty Kafka Connect ConfigMap
     */
    public static KafkaConnectAssembly createEmptyKafkaConnectCluster(String clusterCmNamespace, String clusterCmName) {
        return new KafkaConnectAssemblyBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(clusterCmName)
                        .withNamespace(clusterCmNamespace)
                        .withLabels(TestUtils.map(Labels.STRIMZI_KIND_LABEL, "cluster", Labels.STRIMZI_TYPE_LABEL, "kafka-connect", "my-user-label", "cromulent"))
                        .build())
                .withNewSpec().endSpec()
                .build();
    }

}
