/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.KafkaConnectS2ICluster;
import io.strimzi.controller.cluster.resources.Labels;
import io.strimzi.controller.cluster.resources.TopicController;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
                                                        String image, int healthDelay, int healthTimeout, String metricsCmJson) {
        return createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay, healthTimeout, metricsCmJson,
                "{\"type\": \"ephemeral\"}", null);
    }
    public static ConfigMap createKafkaClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                        String image, int healthDelay, int healthTimeout, String metricsCmJson, String storage) {
        return createKafkaClusterConfigMap(clusterCmNamespace, clusterCmName, replicas, image, healthDelay, healthTimeout, metricsCmJson,
                storage, null);
    }

    public static ConfigMap createKafkaClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                        String image, int healthDelay, int healthTimeout, String metricsCmJson,
                                                        String storage, String topicController) {
        Map<String, String> cmData = new HashMap<>();
        cmData.put(KafkaCluster.KEY_REPLICAS, Integer.toString(replicas));
        cmData.put(KafkaCluster.KEY_IMAGE, image);
        cmData.put(KafkaCluster.KEY_HEALTHCHECK_DELAY, Integer.toString(healthDelay));
        cmData.put(KafkaCluster.KEY_HEALTHCHECK_TIMEOUT, Integer.toString(healthTimeout));
        cmData.put(KafkaCluster.KEY_STORAGE, storage);
        cmData.put(KafkaCluster.KEY_METRICS_CONFIG, metricsCmJson);
        cmData.put(ZookeeperCluster.KEY_REPLICAS, Integer.toString(replicas));
        cmData.put(ZookeeperCluster.KEY_IMAGE, image + "-zk");
        cmData.put(ZookeeperCluster.KEY_HEALTHCHECK_DELAY, Integer.toString(healthDelay));
        cmData.put(ZookeeperCluster.KEY_HEALTHCHECK_TIMEOUT, Integer.toString(healthTimeout));
        cmData.put(ZookeeperCluster.KEY_STORAGE, storage);
        cmData.put(ZookeeperCluster.KEY_METRICS_CONFIG, metricsCmJson);
        if (topicController != null) {
            cmData.put(TopicController.KEY_CONFIG, topicController);
        }
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(clusterCmName)
                    .withNamespace(clusterCmNamespace)
                    .withLabels(Labels.userLabels(singletonMap("my-user-label", "cromulent")).withKind("cluster").withType("kafka").toMap())
                .endMetadata()
                .withData(cmData)
                .build();
    }


    /**
     * Generate ConfigMap for Kafka Connect S2I cluster
     */
    public static ConfigMap createKafkaConnectS2IClusterConfigMap(String clusterCmNamespace, String clusterCmName, int replicas,
                                                                  String image, int healthDelay, int healthTimeout, String bootstrapServers,
                                                                  String groupID, int configReplicationFactor, int offsetReplicationFactor,
                                                                  int statusReplicationFactor, String keyConverter, String valueConverter,
                                                                  boolean keyConverterSchemas, boolean valuesConverterSchema) {
        Map<String, String> cmData = new HashMap<>();
        cmData.put(KafkaConnectS2ICluster.KEY_IMAGE, image);
        cmData.put(KafkaConnectS2ICluster.KEY_REPLICAS, Integer.toString(replicas));
        cmData.put(KafkaConnectS2ICluster.KEY_HEALTHCHECK_DELAY, Integer.toString(healthDelay));
        cmData.put(KafkaConnectS2ICluster.KEY_HEALTHCHECK_TIMEOUT, Integer.toString(healthTimeout));
        cmData.put(KafkaConnectS2ICluster.KEY_BOOTSTRAP_SERVERS, bootstrapServers);
        cmData.put(KafkaConnectS2ICluster.KEY_GROUP_ID, groupID);
        cmData.put(KafkaConnectS2ICluster.KEY_CONFIG_STORAGE_REPLICATION_FACTOR, Integer.toString(configReplicationFactor));
        cmData.put(KafkaConnectS2ICluster.KEY_OFFSET_STORAGE_REPLICATION_FACTOR, Integer.toString(offsetReplicationFactor));
        cmData.put(KafkaConnectS2ICluster.KEY_STATUS_STORAGE_REPLICATION_FACTOR, Integer.toString(statusReplicationFactor));
        cmData.put(KafkaConnectS2ICluster.KEY_KEY_CONVERTER, keyConverter);
        cmData.put(KafkaConnectS2ICluster.KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, Boolean.toString(keyConverterSchemas));
        cmData.put(KafkaConnectS2ICluster.KEY_VALUE_CONVERTER, valueConverter);
        cmData.put(KafkaConnectS2ICluster.KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, Boolean.toString(valuesConverterSchema));

        ConfigMap cm = createEmptyKafkaConnectS2IClusterConfigMap(clusterCmNamespace, clusterCmName);
        cm.setData(cmData);

        return cm;
    }

    /**
     * Generate empty Kafka Connect S2I config map
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
                                                                  String image, int healthDelay, int healthTimeout, String bootstrapServers,
                                                                  String groupID, int configReplicationFactor, int offsetReplicationFactor,
                                                                  int statusReplicationFactor, String keyConverter, String valueConverter,
                                                                  boolean keyConverterSchemas, boolean valuesConverterSchema) {
        Map<String, String> cmData = new HashMap<>();
        cmData.put(KafkaConnectCluster.KEY_IMAGE, image);
        cmData.put(KafkaConnectCluster.KEY_REPLICAS, Integer.toString(replicas));
        cmData.put(KafkaConnectCluster.KEY_HEALTHCHECK_DELAY, Integer.toString(healthDelay));
        cmData.put(KafkaConnectCluster.KEY_HEALTHCHECK_TIMEOUT, Integer.toString(healthTimeout));
        cmData.put(KafkaConnectCluster.KEY_BOOTSTRAP_SERVERS, bootstrapServers);
        cmData.put(KafkaConnectCluster.KEY_GROUP_ID, groupID);
        cmData.put(KafkaConnectCluster.KEY_CONFIG_STORAGE_REPLICATION_FACTOR, Integer.toString(configReplicationFactor));
        cmData.put(KafkaConnectCluster.KEY_OFFSET_STORAGE_REPLICATION_FACTOR, Integer.toString(offsetReplicationFactor));
        cmData.put(KafkaConnectCluster.KEY_STATUS_STORAGE_REPLICATION_FACTOR, Integer.toString(statusReplicationFactor));
        cmData.put(KafkaConnectCluster.KEY_KEY_CONVERTER, keyConverter);
        cmData.put(KafkaConnectCluster.KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, Boolean.toString(keyConverterSchemas));
        cmData.put(KafkaConnectCluster.KEY_VALUE_CONVERTER, valueConverter);
        cmData.put(KafkaConnectCluster.KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, Boolean.toString(valuesConverterSchema));

        ConfigMap cm = createEmptyKafkaConnectClusterConfigMap(clusterCmNamespace, clusterCmName);
        cm.setData(cmData);

        return cm;
    }

    /**
     * Generate empty Kafka Connect config map
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
}
