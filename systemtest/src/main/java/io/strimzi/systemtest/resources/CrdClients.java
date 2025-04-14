/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.Crds;
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

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Class containing encapsulations of clients for Strimzi's CRs.
 */
public class CrdClients {
    /**
     * Private constructor to prevent instantiation.
     */
    private CrdClients() {
        // private constructor
    }

    /**
     * Encapsulation for {@link KafkaBridge} client.
     *
     * @return  {@link KafkaBridge} client.
     */
    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeClient() {
        return Crds.kafkaBridgeOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    /**
     * Encapsulation for {@link KafkaConnector} client.
     *
     * @return  {@link KafkaConnector} client.
     */
    public static MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorClient() {
        return Crds.kafkaConnectorOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    /**
     * Encapsulation for {@link KafkaConnect} client.
     *
     * @return  {@link KafkaConnect} client.
     */
    public static MixedOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectClient() {
        return Crds.kafkaConnectOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    /**
     * Encapsulation for {@link KafkaMirrorMaker2} client.
     *
     * @return  {@link KafkaMirrorMaker2} client.
     */
    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> kafkaMirrorMaker2Client() {
        return Crds.kafkaMirrorMaker2Operation(KubeResourceManager.get().kubeClient().getClient());
    }

    /**
     * Encapsulation for {@link KafkaNodePool} client.
     *
     * @return  {@link KafkaNodePool} client.
     */
    public static MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> kafkaNodePoolClient() {
        return Crds.kafkaNodePoolOperation(kubeClient().getClient());
    }

    /**
     * Encapsulation for {@link KafkaRebalance} client.
     *
     * @return  {@link KafkaRebalance} client.
     */
    public static MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> kafkaRebalanceClient() {
        return Crds.kafkaRebalanceOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    /**
     * Encapsulation for {@link KafkaTopic} client.
     *
     * @return  {@link KafkaTopic} client.
     */
    public static MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> kafkaTopicClient() {
        return Crds.topicOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    /**
     * Encapsulation for {@link KafkaUser} client.
     *
     * @return  {@link KafkaUser} client.
     */
    public static MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> kafkaUserClient() {
        return Crds.kafkaUserOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    /**
     * Encapsulation for {@link Kafka} client.
     *
     * @return  {@link Kafka} client.
     */
    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaClient() {
        return Crds.kafkaOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    /**
     * Encapsulation for {@link StrimziPodSet} client.
     *
     * @return  {@link StrimziPodSet} client.
     */
    public static MixedOperation<StrimziPodSet, StrimziPodSetList, Resource<StrimziPodSet>> strimziPodSetClient() {
        return Crds.strimziPodSetOperation(KubeResourceManager.get().kubeClient().getClient());
    }
}
