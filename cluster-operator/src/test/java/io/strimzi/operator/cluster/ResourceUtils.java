/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.KafkaConnectS2ICluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.model.TopicOperator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.assembly.AbstractAssemblyOperator;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

public class ResourceUtils {

    private ResourceUtils() {

    }

    /**
     * Creates a map of labels
     * @param pairs (key, value) pairs. There must be an even number, obviously.
     * @return a map of labels
     */
    public static Map<String, String> labels(String... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        HashMap<String, String> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1]);
        }
        return map;
    }

    /**
     * Creates a cluster ConfigMap
     * @param clusterCmNamespace
     * @param clusterCmName
     * @param replicas
     * @param image
     * @param healthDelay
     * @param healthTimeout
     * @return
     */
    public static ConfigMap createKafkaClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                        String image, int healthDelay, int healthTimeout,
                                                        String metricsCmJson) {
        return createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCmJson, "{}", "{}",
                "{\"type\": \"ephemeral\"}", null, null);
    }
    public static ConfigMap createKafkaClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                        String image, int healthDelay, int healthTimeout,
                                                        String metricsCmJson, String kafkaConfigurationJson) {
        return createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCmJson, kafkaConfigurationJson, "{}",
                "{\"type\": \"ephemeral\"}", null, null);
    }
    public static ConfigMap createKafkaClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                        String image, int healthDelay, int healthTimeout,
                                                        String metricsCmJson, String kafkaConfigurationJson,
                                                        String zooConfigurationJson) {
        return createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCmJson, kafkaConfigurationJson, zooConfigurationJson,
                "{\"type\": \"ephemeral\"}", null, null);
    }
    public static ConfigMap createKafkaClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                        String image, int healthDelay, int healthTimeout,
                                                        String metricsCmJson, String kafkaConfigurationJson,
                                                        String zooConfigurationJson, String storage) {
        return createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay,
                healthTimeout, metricsCmJson, kafkaConfigurationJson, zooConfigurationJson,
                storage, null, null);
    }

    public static ConfigMap createKafkaClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                        String image, int healthDelay, int healthTimeout, String metricsCmJson,
                                                        String kafkaConfigurationJson, String zooConfigurationJson,
                                                        String storage, String topicOperator, String rackJson) {
        Map<String, String> cmData = new HashMap<>();
        cmData.put(KafkaCluster.KEY_REPLICAS, Integer.toString(replicas));
        cmData.put(KafkaCluster.KEY_IMAGE, image);
        cmData.put(KafkaCluster.KEY_HEALTHCHECK_DELAY, Integer.toString(healthDelay));
        cmData.put(KafkaCluster.KEY_HEALTHCHECK_TIMEOUT, Integer.toString(healthTimeout));
        cmData.put(KafkaCluster.KEY_STORAGE, storage);
        cmData.put(KafkaCluster.KEY_METRICS_CONFIG, metricsCmJson);
        if (kafkaConfigurationJson != null) {
            cmData.put(KafkaCluster.KEY_KAFKA_CONFIG, kafkaConfigurationJson);
        }
        cmData.put(ZookeeperCluster.KEY_REPLICAS, Integer.toString(replicas));
        cmData.put(ZookeeperCluster.KEY_IMAGE, image + "-zk");
        cmData.put(ZookeeperCluster.KEY_HEALTHCHECK_DELAY, Integer.toString(healthDelay));
        cmData.put(ZookeeperCluster.KEY_HEALTHCHECK_TIMEOUT, Integer.toString(healthTimeout));
        if (zooConfigurationJson != null) {
            cmData.put(ZookeeperCluster.KEY_ZOOKEEPER_CONFIG, zooConfigurationJson);
        }
        cmData.put(ZookeeperCluster.KEY_STORAGE, storage);
        cmData.put(ZookeeperCluster.KEY_METRICS_CONFIG, metricsCmJson);
        if (topicOperator != null) {
            cmData.put(TopicOperator.KEY_CONFIG, topicOperator);
        }
        if (rackJson != null) {
            cmData.put(KafkaCluster.KEY_RACK, rackJson);
        }
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(clusterCmName)
                    .withNamespace(clusterCmNamespace)
                    .withLabels(Labels.userLabels(singletonMap("my-user-label", "cromulent")).withKind("cluster").withType(AssemblyType.KAFKA).toMap())
                .endMetadata()
                .withData(cmData)
                .build();
    }

    public static List<Secret> createKafkaClusterInitialSecrets(String clusterCmNamespace) {

        List<Secret> secrets = new ArrayList<>();

        Map<String, String> data = new HashMap<>();
        data.put("internal-ca.key", Base64.getEncoder().encodeToString("internal-ca-base64key".getBytes()));
        data.put("internal-ca.crt", Base64.getEncoder().encodeToString("internal-ca-base64crt".getBytes()));
        secrets.add(
                new SecretBuilder()
                .withNewMetadata()
                    .withName(AbstractAssemblyOperator.INTERNAL_CA_NAME)
                    .withNamespace(clusterCmNamespace)
                .endMetadata()
                .withData(data)
                .build()
        );
        return secrets;
    }

    public static List<Secret> createKafkaClusterSecretsWithReplicas(String clusterCmNamespace, String clusterCmName, int replicas) {

        List<Secret> secrets = new ArrayList<>();

        Map<String, String> data = new HashMap<>();
        data.put("internal-ca.key", Base64.getEncoder().encodeToString("internal-ca-base64key".getBytes()));
        data.put("internal-ca.crt", Base64.getEncoder().encodeToString("internal-ca-base64crt".getBytes()));
        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                            .withName(AbstractAssemblyOperator.INTERNAL_CA_NAME)
                            .withNamespace(clusterCmNamespace)
                        .endMetadata()
                        .withData(data)
                        .build()
        );

        data = new HashMap<>();
        data.put("clients-ca.key", Base64.getEncoder().encodeToString("clients-ca-base64key".getBytes()));
        data.put("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-base64crt".getBytes()));
        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                            .withName(KafkaCluster.clientsCASecretName(clusterCmName))
                            .withNamespace(clusterCmNamespace)
                            .withLabels(Labels.forCluster(clusterCmName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .withData(data)
                        .build()
        );

        data = new HashMap<>();
        data.put("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-base64crt".getBytes()));
        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                            .withName(KafkaCluster.clientsPublicKeyName(clusterCmName))
                            .withNamespace(clusterCmNamespace)
                            .withLabels(Labels.forCluster(clusterCmName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .withData(data)
                        .build()
        );

        data = new HashMap<>();
        data.put("internal-ca.crt", Base64.getEncoder().encodeToString("internal-ca-base64crt".getBytes()));
        for (int i = 0; i < replicas; i++) {
            data.put(KafkaCluster.kafkaPodName(clusterCmName, i) + ".key", Base64.getEncoder().encodeToString("brokers-internal-base64key".getBytes()));
            data.put(KafkaCluster.kafkaPodName(clusterCmName, i) + ".crt", Base64.getEncoder().encodeToString("brokers-internal-base64crt".getBytes()));
        }
        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                            .withName(KafkaCluster.brokersInternalSecretName(clusterCmName))
                            .withNamespace(clusterCmNamespace)
                            .withLabels(Labels.forCluster(clusterCmName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .withData(data)
                        .build()
        );

        data = new HashMap<>();
        data.put("internal-ca.crt", Base64.getEncoder().encodeToString("internal-ca-base64crt".getBytes()));
        data.put("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-base64crt".getBytes()));
        for (int i = 0; i < replicas; i++) {
            data.put(KafkaCluster.kafkaPodName(clusterCmName, i) + ".key", Base64.getEncoder().encodeToString("brokers-clients-base64key".getBytes()));
            data.put(KafkaCluster.kafkaPodName(clusterCmName, i) + ".crt", Base64.getEncoder().encodeToString("brokers-clients-base64crt".getBytes()));
        }
        secrets.add(
                new SecretBuilder()
                        .withNewMetadata()
                            .withName(KafkaCluster.brokersClientsSecret(clusterCmName))
                            .withNamespace(clusterCmNamespace)
                            .withLabels(Labels.forCluster(clusterCmName).withType(AssemblyType.KAFKA).toMap())
                        .endMetadata()
                        .withData(data)
                        .build()
        );
        return secrets;
    }


    /**
     * Generate ConfigMap for Kafka Connect S2I cluster
     */
    public static ConfigMap createKafkaConnectS2IClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                                  String image, int healthDelay, int healthTimeout, String metricsCmJson,
                                                                  String connectConfig, boolean insecureSourceRepo) {
        Map<String, String> cmData = new HashMap<>();
        cmData.put(KafkaConnectS2ICluster.KEY_IMAGE, image);
        cmData.put(KafkaConnectS2ICluster.KEY_REPLICAS, Integer.toString(replicas));
        cmData.put(KafkaConnectS2ICluster.KEY_HEALTHCHECK_DELAY, Integer.toString(healthDelay));
        cmData.put(KafkaConnectS2ICluster.KEY_HEALTHCHECK_TIMEOUT, Integer.toString(healthTimeout));
        cmData.put(KafkaConnectCluster.KEY_METRICS_CONFIG, metricsCmJson);
        if (connectConfig != null) {
            cmData.put(KafkaConnectS2ICluster.KEY_CONNECT_CONFIG, connectConfig);
        }
        cmData.put(KafkaConnectS2ICluster.KEY_INSECURE_SOURCE_REPO, String.valueOf(insecureSourceRepo));

        ConfigMap cm = createEmptyKafkaConnectS2IClusterConfigMap(clusterCmNamespace, clusterCmName);
        cm.setData(cmData);

        return cm;
    }

    /**
     * Generate empty Kafka Connect S2I ConfigMap
     */
    public static ConfigMap createEmptyKafkaConnectS2IClusterConfigMap(String clusterCmNamespace, String clusterCmName) {
        Map<String, String> cmData = new HashMap<>();

        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(clusterCmName)
                .withNamespace(clusterCmNamespace)
                .withLabels(labels(Labels.STRIMZI_KIND_LABEL, "cluster",
                        Labels.STRIMZI_TYPE_LABEL, "kafka-connect-s2i",
                        "my-user-label", "cromulent"))
                .endMetadata()
                .withData(cmData)
                .build();
    }

    /**
     * Generate ConfigMap for Kafka Connect cluster
     */
    public static ConfigMap createKafkaConnectClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                                  String image, int healthDelay, int healthTimeout, String metricsCmJson, String connectConfig) {
        Map<String, String> cmData = new HashMap<>();
        cmData.put(KafkaConnectCluster.KEY_IMAGE, image);
        cmData.put(KafkaConnectCluster.KEY_REPLICAS, Integer.toString(replicas));
        cmData.put(KafkaConnectCluster.KEY_HEALTHCHECK_DELAY, Integer.toString(healthDelay));
        cmData.put(KafkaConnectCluster.KEY_HEALTHCHECK_TIMEOUT, Integer.toString(healthTimeout));
        cmData.put(KafkaConnectCluster.KEY_METRICS_CONFIG, metricsCmJson);
        if (connectConfig != null) {
            cmData.put(KafkaConnectCluster.KEY_CONNECT_CONFIG, connectConfig);
        }

        ConfigMap cm = createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName);
        cm.setData(cmData);

        return cm;
    }

    /**
     * Generate empty Kafka Connect ConfigMap
     */
    public static ConfigMap createEmptyKafkaConnectClusterConfigMap(String clusterCmNamespace, String clusterCmName) {
        Map<String, String> cmData = new HashMap<>();

        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(clusterCmName)
                .withNamespace(clusterCmNamespace)
                .withLabels(labels(Labels.STRIMZI_KIND_LABEL, "cluster",
                        Labels.STRIMZI_TYPE_LABEL, "kafka-connect",
                        "my-user-label", "cromulent"))
                .endMetadata()
                .withData(cmData)
                .build();
    }

    public static <T> Set<T> set(T... elements) {
        return new HashSet(asList(elements));
    }

    public static <T> Map<T, T> map(T... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        Map<T, T> result = new HashMap<>(pairs.length / 2);
        for (int i = 0; i < pairs.length; i += 2) {
            result.put(pairs[i], pairs[i + 1]);
        }
        return result;
    }
}
