/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.utils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaBridgeList;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;

/**
 * "Static" information about the CRDs defined in this package
 */
@SuppressWarnings("deprecation")
public class LegacyCrds {
    private static CustomResourceDefinitionContext crdCtx(io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition crd, String version)   {
        return new CustomResourceDefinitionContext.Builder()
                .withKind(crd.getSpec().getNames().getKind())
                .withName(crd.getSpec().getNames().getSingular())
                .withPlural(crd.getSpec().getNames().getPlural())
                .withScope(crd.getSpec().getScope())
                .withGroup(crd.getSpec().getGroup())
                .withVersion(version)
                .build();
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOperation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafka(), version), Kafka.class, KafkaList.class);
    }

    public static MixedOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectOperation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafkaConnect(), version), KafkaConnect.class, KafkaConnectList.class);
    }

    public static MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorOperation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafkaConnector(), version), KafkaConnector.class, KafkaConnectorList.class);
    }

    public static MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, Resource<KafkaConnectS2I>> kafkaConnectS2iOperation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafkaConnectS2I(), version), KafkaConnectS2I.class, KafkaConnectS2IList.class);
    }

    public static MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> topicOperation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafkaTopic(), version), KafkaTopic.class, KafkaTopicList.class);
    }

    public static MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> kafkaUserOperation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafkaUser(), version), KafkaUser.class, KafkaUserList.class);
    }

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, Resource<KafkaMirrorMaker>> mirrorMakerOperation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafkaMirrorMaker(), version), KafkaMirrorMaker.class, KafkaMirrorMakerList.class);
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeOperation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafkaBridge(), version), KafkaBridge.class, KafkaBridgeList.class);
    }

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> kafkaMirrorMaker2Operation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafkaMirrorMaker2(), version), KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class);
    }

    public static MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> kafkaRebalanceOperation(KubernetesClient client, String version) {
        return client.customResources(crdCtx(Crds.kafkaRebalance(), version), KafkaRebalance.class, KafkaRebalanceList.class);
    }
}
