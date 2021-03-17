/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceSubresourceStatus;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.internal.KubernetesDeserializer;
import io.strimzi.api.kafka.model.Constants;
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

import java.util.List;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.Constants.CRD_KIND;
import static java.util.Collections.singletonList;

/**
 * "Static" information about the CRDs defined in this package
 */
public class Crds {

    @SuppressWarnings("unchecked")
    private static final Class<? extends CustomResource>[] CRDS = new Class[] {
        Kafka.class,
        KafkaConnect.class,
        KafkaConnectS2I.class,
        KafkaTopic.class,
        KafkaUser.class,
        KafkaMirrorMaker.class,
        KafkaBridge.class,
        KafkaConnector.class,
        KafkaMirrorMaker2.class,
        KafkaRebalance.class
    };

    private Crds() {
    }

    /**
     * Register custom resource kinds with {@link KubernetesDeserializer} so Fabric8 knows how to deserialize them.
     */
    public static void registerCustomKinds() {
        for (Class<? extends CustomResource> crdClass : CRDS) {
            for (String version : apiVersions(crdClass)) {
                KubernetesDeserializer.registerCustomKind(version, kind(crdClass), crdClass);
            }
        }
    }

    private static CustomResourceDefinition crd(Class<? extends CustomResource> cls) {
        String version = null;
        if (cls.equals(Kafka.class)) {
            version = Kafka.CONSUMED_VERSION;
        } else if (cls.equals(KafkaConnect.class)) {
            version = KafkaConnect.CONSUMED_VERSION;
        } else if (cls.equals(KafkaConnectS2I.class)) {
            version = KafkaConnectS2I.CONSUMED_VERSION;
        } else if (cls.equals(KafkaTopic.class)) {
            version = KafkaTopic.CONSUMED_VERSION;
        } else if (cls.equals(KafkaUser.class)) {
            version = KafkaUser.CONSUMED_VERSION;
        } else if (cls.equals(KafkaMirrorMaker.class)) {
            version = KafkaMirrorMaker.CONSUMED_VERSION;
        } else if (cls.equals(KafkaBridge.class)) {
            version = KafkaBridge.CONSUMED_VERSION;
        } else if (cls.equals(KafkaConnector.class)) {
            version = KafkaConnector.CONSUMED_VERSION;
        } else if (cls.equals(KafkaMirrorMaker2.class)) {
            version = KafkaMirrorMaker2.CONSUMED_VERSION;
        } else if (cls.equals(KafkaRebalance.class)) {
            version = KafkaRebalance.CONSUMED_VERSION;
        } else {
            throw new RuntimeException();
        }

        return crd(cls, version);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:JavaNCSS"})
    private static CustomResourceDefinition crd(Class<? extends CustomResource> cls, String version) {
        String scope, crdApiVersion, plural, singular, group, kind, listKind;
        CustomResourceSubresourceStatus status = null;

        if (cls.equals(Kafka.class)) {
            scope = Kafka.SCOPE;
            crdApiVersion = Kafka.CRD_API_VERSION;
            plural = Kafka.RESOURCE_PLURAL;
            singular = Kafka.RESOURCE_SINGULAR;
            group = Kafka.RESOURCE_GROUP;
            kind = Kafka.RESOURCE_KIND;
            listKind = Kafka.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!Kafka.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaConnect.class)) {
            scope = KafkaConnect.SCOPE;
            crdApiVersion = KafkaConnect.CRD_API_VERSION;
            plural = KafkaConnect.RESOURCE_PLURAL;
            singular = KafkaConnect.RESOURCE_SINGULAR;
            group = KafkaConnect.RESOURCE_GROUP;
            kind = KafkaConnect.RESOURCE_KIND;
            listKind = KafkaConnect.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaConnect.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaConnectS2I.class)) {
            scope = KafkaConnectS2I.SCOPE;
            crdApiVersion = KafkaConnectS2I.CRD_API_VERSION;
            plural = KafkaConnectS2I.RESOURCE_PLURAL;
            singular = KafkaConnectS2I.RESOURCE_SINGULAR;
            group = KafkaConnectS2I.RESOURCE_GROUP;
            kind = KafkaConnectS2I.RESOURCE_KIND;
            listKind = KafkaConnectS2I.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaConnectS2I.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaTopic.class)) {
            scope = KafkaTopic.SCOPE;
            crdApiVersion = KafkaTopic.CRD_API_VERSION;
            plural = KafkaTopic.RESOURCE_PLURAL;
            singular = KafkaTopic.RESOURCE_SINGULAR;
            group = KafkaTopic.RESOURCE_GROUP;
            kind = KafkaTopic.RESOURCE_KIND;
            listKind = KafkaTopic.RESOURCE_LIST_KIND;
            if (!KafkaTopic.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaUser.class)) {
            scope = KafkaUser.SCOPE;
            crdApiVersion = KafkaUser.CRD_API_VERSION;
            plural = KafkaUser.RESOURCE_PLURAL;
            singular = KafkaUser.RESOURCE_SINGULAR;
            group = KafkaUser.RESOURCE_GROUP;
            kind = KafkaUser.RESOURCE_KIND;
            listKind = KafkaUser.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaUser.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaMirrorMaker.class)) {
            scope = KafkaMirrorMaker.SCOPE;
            crdApiVersion = KafkaMirrorMaker.CRD_API_VERSION;
            plural = KafkaMirrorMaker.RESOURCE_PLURAL;
            singular = KafkaMirrorMaker.RESOURCE_SINGULAR;
            group = KafkaMirrorMaker.RESOURCE_GROUP;
            kind = KafkaMirrorMaker.RESOURCE_KIND;
            listKind = KafkaMirrorMaker.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaMirrorMaker.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaBridge.class)) {
            scope = KafkaBridge.SCOPE;
            crdApiVersion = KafkaBridge.CRD_API_VERSION;
            plural = KafkaBridge.RESOURCE_PLURAL;
            singular = KafkaBridge.RESOURCE_SINGULAR;
            group = KafkaBridge.RESOURCE_GROUP;
            kind = KafkaBridge.RESOURCE_KIND;
            listKind = KafkaBridge.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaBridge.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaConnector.class)) {
            scope = KafkaConnector.SCOPE;
            crdApiVersion = KafkaConnector.CRD_API_VERSION;
            plural = KafkaConnector.RESOURCE_PLURAL;
            singular = KafkaConnector.RESOURCE_SINGULAR;
            group = KafkaConnector.RESOURCE_GROUP;
            kind = KafkaConnector.RESOURCE_KIND;
            listKind = KafkaConnector.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaConnector.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaMirrorMaker2.class)) {
            scope = KafkaMirrorMaker2.SCOPE;
            crdApiVersion = KafkaMirrorMaker2.CRD_API_VERSION;
            plural = KafkaMirrorMaker2.RESOURCE_PLURAL;
            singular = KafkaMirrorMaker2.RESOURCE_SINGULAR;
            group = KafkaMirrorMaker2.RESOURCE_GROUP;
            kind = KafkaMirrorMaker2.RESOURCE_KIND;
            listKind = KafkaMirrorMaker2.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaMirrorMaker2.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaRebalance.class)) {
            scope = KafkaRebalance.SCOPE;
            crdApiVersion = KafkaRebalance.CRD_API_VERSION;
            plural = KafkaRebalance.RESOURCE_PLURAL;
            singular = KafkaRebalance.RESOURCE_SINGULAR;
            group = KafkaRebalance.RESOURCE_GROUP;
            kind = KafkaRebalance.RESOURCE_KIND;
            listKind = KafkaRebalance.RESOURCE_LIST_KIND;
            status = new CustomResourceSubresourceStatus();
            if (!KafkaRebalance.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else {
            throw new RuntimeException();
        }

        return new CustomResourceDefinitionBuilder()
                .withApiVersion(crdApiVersion)
                .withKind(CRD_KIND)
                .withNewMetadata()
                    .withName(plural + "." + group)
                .endMetadata()
                .withNewSpec()
                    .withScope(scope)
                    .withGroup(group)
                    .withVersion(version)
                    .withNewNames()
                        .withSingular(singular)
                        .withPlural(plural)
                        .withKind(kind)
                        .withListKind(listKind)
                    .endNames()
                    .withNewSubresources()
                        .withStatus(status)
                    .endSubresources()
                .endSpec()
                .build();
    }

    public static CustomResourceDefinition kafka() {
        return crd(Kafka.class);
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOperation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafka()), Kafka.class, KafkaList.class);
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOperation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(Kafka.class, version)), Kafka.class, KafkaList.class);
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaV1Alpha1Operation(KubernetesClient client) {
        return kafkaOperation(client, Constants.V1ALPHA1);
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaV1Beta1Operation(KubernetesClient client) {
        return kafkaOperation(client, Constants.V1BETA1);
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaV1Beta2Operation(KubernetesClient client) {
        return kafkaOperation(client, Constants.V1BETA2);
    }

    public static CustomResourceDefinition kafkaConnect() {
        return crd(KafkaConnect.class);
    }

    public static MixedOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectOperation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(KafkaConnect.class, version)), KafkaConnect.class, KafkaConnectList.class);
    }

    public static MixedOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectOperation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafkaConnect()), KafkaConnect.class, KafkaConnectList.class);
    }

    public static MixedOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectV1Beta2Operation(KubernetesClient client) {
        return kafkaConnectOperation(client, Constants.V1BETA2);
    }

    public static CustomResourceDefinition kafkaConnector() {
        return crd(KafkaConnector.class);
    }

    public static MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorOperation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(KafkaConnector.class, version)), KafkaConnector.class, KafkaConnectorList.class);
    }

    public static MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorOperation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafkaConnector()), KafkaConnector.class, KafkaConnectorList.class);
    }

    public static MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorV1Beta2Operation(KubernetesClient client) {
        return kafkaConnectorOperation(client, Constants.V1BETA2);
    }

    public static CustomResourceDefinition kafkaConnectS2I() {
        return crd(KafkaConnectS2I.class);
    }

    public static <T extends CustomResource> MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, Resource<KafkaConnectS2I>> kafkaConnectS2iOperation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(KafkaConnectS2I.class, version)), KafkaConnectS2I.class, KafkaConnectS2IList.class);
    }

    public static <T extends CustomResource> MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, Resource<KafkaConnectS2I>> kafkaConnectS2iOperation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafkaConnectS2I()), KafkaConnectS2I.class, KafkaConnectS2IList.class);
    }

    public static <T extends CustomResource> MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, Resource<KafkaConnectS2I>> kafkaConnectS2iV1Beta2Operation(KubernetesClient client) {
        return kafkaConnectS2iOperation(client, Constants.V1BETA2);
    }

    public static CustomResourceDefinition kafkaTopic() {
        return crd(KafkaTopic.class);
    }

    public static MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> topicOperation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafkaTopic()), KafkaTopic.class, KafkaTopicList.class);
    }

    public static MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> topicV1Beta2Operation(KubernetesClient client) {
        return topicOperation(client, Constants.V1BETA2);
    }

    public static MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> topicOperation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(KafkaTopic.class, version)), KafkaTopic.class, KafkaTopicList.class);
    }

    public static CustomResourceDefinition kafkaUser() {
        return crd(KafkaUser.class);
    }

    public static MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> kafkaUserOperation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafkaUser()), KafkaUser.class, KafkaUserList.class);
    }

    public static MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> kafkaUserV1Beta2Operation(KubernetesClient client) {
        return kafkaUserOperation(client, Constants.V1BETA2);
    }

    public static MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> kafkaUserOperation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(KafkaUser.class, version)), KafkaUser.class, KafkaUserList.class);
    }

    public static CustomResourceDefinition kafkaMirrorMaker() {
        return crd(KafkaMirrorMaker.class);
    }

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, Resource<KafkaMirrorMaker>> mirrorMakerOperation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(KafkaMirrorMaker.class, version)), KafkaMirrorMaker.class, KafkaMirrorMakerList.class);
    }

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, Resource<KafkaMirrorMaker>> mirrorMakerOperation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafkaMirrorMaker()), KafkaMirrorMaker.class, KafkaMirrorMakerList.class);
    }

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, Resource<KafkaMirrorMaker>> mirrorMakerV1Beta2Operation(KubernetesClient client) {
        return mirrorMakerOperation(client, Constants.V1BETA2);
    }

    public static CustomResourceDefinition kafkaBridge() {
        return crd(KafkaBridge.class);
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeOperation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafkaBridge()), KafkaBridge.class, KafkaBridgeList.class);
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeV1Beta2Operation(KubernetesClient client) {
        return kafkaBridgeOperation(client, Constants.V1BETA2);
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeOperation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(KafkaBridge.class, version)), KafkaBridge.class, KafkaBridgeList.class);
    }

    public static CustomResourceDefinition kafkaMirrorMaker2() {
        return crd(KafkaMirrorMaker2.class);
    }

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> kafkaMirrorMaker2Operation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(KafkaMirrorMaker2.class, version)), KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class);
    }

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> kafkaMirrorMaker2Operation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafkaMirrorMaker2()), KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class);
    }

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> kafkaMirrorMaker2V1Beta2Operation(KubernetesClient client) {
        return kafkaMirrorMaker2Operation(client, Constants.V1BETA2);
    }

    public static CustomResourceDefinition kafkaRebalance() {
        return crd(KafkaRebalance.class);
    }

    public static MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> kafkaRebalanceOperation(KubernetesClient client) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(kafkaRebalance()), KafkaRebalance.class, KafkaRebalanceList.class);
    }

    public static MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> kafkaRebalanceV1Beta2Operation(KubernetesClient client) {
        return kafkaRebalanceOperation(client, Constants.V1BETA2);
    }

    public static MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> kafkaRebalanceOperation(KubernetesClient client, String version) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(KafkaRebalance.class, version)), KafkaRebalance.class, KafkaRebalanceList.class);
    }

    public static <T extends CustomResource, L extends CustomResourceList<T>> MixedOperation<T, L, Resource<T>>
            operation(KubernetesClient client,
                      Class<T> cls,
                      Class<L> listCls) {
        return client.customResources(CustomResourceDefinitionContext.fromCrd(crd(cls)), cls, listCls);
    }

    public static <T extends CustomResource> String kind(Class<T> cls) {
        try {
            return cls.newInstance().getKind();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends CustomResource> List<String> apiVersions(Class<T> cls) {
        try {
            String group = (String) cls.getField("RESOURCE_GROUP").get(null);

            List<String> versions;
            try {
                versions = singletonList(group + "/" + (String) cls.getField("VERSION").get(null));
            } catch (NoSuchFieldException e) {
                versions = ((List<String>) cls.getField("VERSIONS").get(null)).stream().map(v ->
                        group + "/" + v).collect(Collectors.toList());
            }
            return versions;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static int getNumCrds() {
        return CRDS.length;
    }
}
