/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.labels;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.model.Labels;

import java.util.HashMap;
import java.util.Map;

/**
 * Class containing {@link LabelSelector} builders for all resources (used in tests).
 */
public class LabelSelectors {
    /**
     * Private constructor to prevent instantiation.
     */
    private LabelSelectors() {
        // private constructor
    }

    /**
     * Method for returning {@link LabelSelector} matching Bridge Pods for particular {@param clusterName}.
     *
     * @param clusterName       name of the Bridge cluster.
     * @param componentName     name of the Bridge component.
     *
     * @return  {@link LabelSelector} matching Bridge Pods for particular {@param clusterName}.
     */
    public static LabelSelector bridgeLabelSelector(String clusterName, String componentName) {
        return new LabelSelectorBuilder()
            .withMatchLabels(commonMatchLabels(clusterName, KafkaBridge.RESOURCE_KIND, componentName))
            .build();
    }

    /**
     * Method for returning {@link LabelSelector} matching Connect Pods for particular {@param clusterName}.
     *
     * @param clusterName       name of the Connect cluster.
     * @param componentName     name of the Connect component.
     *
     * @return  {@link LabelSelector} matching Connect Pods for particular {@param clusterName}.
     */
    public static LabelSelector connectLabelSelector(String clusterName, String componentName) {
        return new LabelSelectorBuilder()
            .withMatchLabels(commonMatchLabels(clusterName, KafkaConnect.RESOURCE_KIND, componentName))
            .build();
    }

    /**
     * Method for returning {@link LabelSelector} matching MM2 Pods for particular {@param clusterName}.
     *
     * @param clusterName       name of the MM2 cluster.
     * @param componentName     name of the MM2 component.
     *
     * @return  {@link LabelSelector} matching MM2 Pods for particular {@param clusterName}.
     */
    public static LabelSelector mirrorMaker2LabelSelector(String clusterName, String componentName) {
        return new LabelSelectorBuilder()
            .withMatchLabels(commonMatchLabels(clusterName, KafkaMirrorMaker2.RESOURCE_KIND, componentName))
            .build();
    }

    /**
     * Method for returning {@link LabelSelector} matching Pods of particular {@link io.strimzi.api.kafka.model.nodepool.KafkaNodePool},
     * based on {@param clusterName}, {@param poolName} and its {@link ProcessRoles}.
     *
     * @param clusterName   name of the Kafka cluster.
     * @param poolName      name of the NodePool.
     * @param processRole   role of the NodePool.
     *
     * @return  {@link LabelSelector} matching Pods of particular {@link io.strimzi.api.kafka.model.nodepool.KafkaNodePool}.
     */
    public static LabelSelector nodePoolLabelSelector(String clusterName, String poolName, ProcessRoles processRole) {
        Map<String, String> matchLabels = new HashMap<>();

        matchLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        matchLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        matchLabels.put(Labels.STRIMZI_POOL_NAME_LABEL, poolName);

        switch (processRole) {
            case BROKER -> matchLabels.put(Labels.STRIMZI_BROKER_ROLE_LABEL, "true");
            case CONTROLLER -> matchLabels.put(Labels.STRIMZI_CONTROLLER_ROLE_LABEL, "true");
            default -> throw new RuntimeException("No role for KafkaNodePool specified");
        }

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }

    /**
     * Method for returning {@link LabelSelector} matching Kafka Pods for particular {@param clusterName} and {@param componentName}.
     * This is used when we want to list Kafka Pods just for controllers or brokers.
     *
     * @param clusterName       name of the Kafka cluster.
     * @param componentName     name of the Kafka component.
     * @return  {@link LabelSelector} matching Kafka Pods for particular {@param clusterName} and {@param componentName}.
     */
    public static LabelSelector kafkaLabelSelector(String clusterName, String componentName) {
        Map<String, String> labels = new HashMap<>();

        labels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        labels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        labels.put(Labels.STRIMZI_CONTROLLER_NAME_LABEL, componentName);
        
        return new LabelSelectorBuilder()
            .withMatchLabels(labels)
            .build();
    }

    /**
     * Method for returning {@link LabelSelector} matching EntityOperator for particular {@param clusterName}.
     *
     * @param clusterName   name of the Kafka cluster.
     *
     * @return  {@link LabelSelector} matching EntityOperator for particular {@param clusterName}.
     */
    public static LabelSelector entityOperatorLabelSelector(final String clusterName) {
        Map<String, String> matchLabels = new HashMap<>();
        matchLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        matchLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        matchLabels.put(Labels.STRIMZI_COMPONENT_TYPE_LABEL, "entity-operator");

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }

    /**
     * Method for returning {@link LabelSelector} matching all Kafka pods with particular {@param clusterName}.
     *
     * @param clusterName   name of the Kafka cluster.
     *
     * @return  {@link LabelSelector} matching all Kafka pods with particular {@param clusterName}.
     */
    public static LabelSelector allKafkaPodsLabelSelector(String clusterName) {
        return new LabelSelectorBuilder()
            .withMatchLabels(commonMatchLabels(clusterName, Kafka.RESOURCE_KIND, KafkaResources.kafkaComponentName(clusterName)))
            .build();
    }

    /**
     * Method for returning common match labels - {@link Labels#STRIMZI_CLUSTER_LABEL}, {@link Labels#STRIMZI_KIND_LABEL}, {@link Labels#STRIMZI_CLUSTER_LABEL}.
     *
     * @param clusterName       name of the cluster.
     * @param kind              kind of resource.
     * @param componentName     name of the component.
     *
     * @return  map of match labels.
     */
    private static Map<String, String> commonMatchLabels(String clusterName, String kind, String componentName) {
        Map<String, String> labels = new HashMap<>();

        labels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        labels.put(Labels.STRIMZI_KIND_LABEL, kind);
        labels.put(Labels.STRIMZI_NAME_LABEL, componentName);

        return labels;
    }
}
