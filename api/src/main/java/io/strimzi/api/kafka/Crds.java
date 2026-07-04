/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectList;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicList;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;

/**
 * "Static" information about the CRDs defined in this package
 */
public class Crds {
    private Crds() { }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOperation(KubernetesClient client) {
        return client.resources(Kafka.class, KafkaList.class);
    }

    public static MixedOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectOperation(KubernetesClient client) {
        return client.resources(KafkaConnect.class, KafkaConnectList.class);
    }

    public static MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorOperation(KubernetesClient client) {
        return client.resources(KafkaConnector.class, KafkaConnectorList.class);
    }

    public static MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> topicOperation(KubernetesClient client) {
        return client.resources(KafkaTopic.class, KafkaTopicList.class);
    }

    public static MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> kafkaUserOperation(KubernetesClient client) {
        return client.resources(KafkaUser.class, KafkaUserList.class);
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeOperation(KubernetesClient client) {
        return client.resources(KafkaBridge.class, KafkaBridgeList.class);
    }

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> kafkaMirrorMaker2Operation(KubernetesClient client) {
        return client.resources(KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class);
    }

    public static MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> kafkaRebalanceOperation(KubernetesClient client) {
        return client.resources(KafkaRebalance.class, KafkaRebalanceList.class);
    }
    public static MixedOperation<StrimziPodSet, StrimziPodSetList, Resource<StrimziPodSet>> strimziPodSetOperation(KubernetesClient client) {
        return client.resources(StrimziPodSet.class, StrimziPodSetList.class);
    }

    public static MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> kafkaNodePoolOperation(KubernetesClient client) {
        return client.resources(KafkaNodePool.class, KafkaNodePoolList.class);
    }
}
