/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.internal.KubernetesDeserializer;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * "Static" information about the CRDs defined in this package
 */
public class Crds {

    public static final String CRD_KIND = "CustomResourceDefinition";

    private static final Class<? extends CustomResource>[] CRDS = new Class[] {
        Kafka.class,
        KafkaConnect.class,
        KafkaConnectS2I.class,
        KafkaTopic.class,
        KafkaUser.class
    };

    private Crds() {
    }

    /**
     * Register custom resource kinds with {@link KubernetesDeserializer} so Fabric8 knows how to deserialize them.
     */
    public static void registerCustomKinds() {
        for (Class<? extends CustomResource> c : CRDS) {
            KubernetesDeserializer.registerCustomKind(kind(c), c);
        }
    }

    private static CustomResourceDefinition crd(Class<? extends CustomResource> cls) {
        String kind;
        String crdApiVersion;
        String plural;
        String listKind;
        String singular;
        String version;
        String group;
        List<String> shortNames = emptyList();
        if (cls.equals(Kafka.class)) {
            kind = Kafka.RESOURCE_KIND;
            crdApiVersion = Kafka.CRD_API_VERSION;
            plural = Kafka.RESOURCE_PLURAL;
            singular = Kafka.RESOURCE_SINGULAR;
            listKind = Kafka.RESOURCE_LIST_KIND;
            group = Kafka.RESOURCE_GROUP;
            version = Kafka.VERSION;
        } else if (cls.equals(KafkaConnect.class)) {
            kind = KafkaConnect.RESOURCE_KIND;
            crdApiVersion = KafkaConnect.CRD_API_VERSION;
            plural = KafkaConnect.RESOURCE_PLURAL;
            singular = KafkaConnect.RESOURCE_SINGULAR;
            listKind = KafkaConnect.RESOURCE_LIST_KIND;
            group = KafkaConnect.RESOURCE_GROUP;
            version = KafkaConnect.VERSION;
        } else if (cls.equals(KafkaConnectS2I.class)) {
            kind = KafkaConnectS2I.RESOURCE_KIND;
            crdApiVersion = KafkaConnectS2I.CRD_API_VERSION;
            plural = KafkaConnectS2I.RESOURCE_PLURAL;
            singular = KafkaConnectS2I.RESOURCE_SINGULAR;
            listKind = KafkaConnectS2I.RESOURCE_LIST_KIND;
            group = KafkaConnectS2I.RESOURCE_GROUP;
            version = KafkaConnectS2I.VERSION;
        } else if (cls.equals(KafkaTopic.class)) {
            kind = KafkaTopic.RESOURCE_KIND;
            crdApiVersion = KafkaTopic.CRD_API_VERSION;
            plural = KafkaTopic.RESOURCE_PLURAL;
            singular = KafkaTopic.RESOURCE_SINGULAR;
            listKind = KafkaTopic.RESOURCE_LIST_KIND;
            group = KafkaTopic.RESOURCE_GROUP;
            version = KafkaTopic.VERSION;
            shortNames = KafkaTopic.RESOURCE_SHORTNAMES;
        } else if (cls.equals(KafkaUser.class)) {
            kind = KafkaUser.RESOURCE_KIND;
            crdApiVersion = KafkaUser.CRD_API_VERSION;
            plural = KafkaUser.RESOURCE_PLURAL;
            singular = KafkaUser.RESOURCE_SINGULAR;
            listKind = KafkaUser.RESOURCE_LIST_KIND;
            group = KafkaUser.RESOURCE_GROUP;
            version = KafkaUser.VERSION;
            shortNames = KafkaUser.RESOURCE_SHORTNAMES;
        } else if (cls.equals(KafkaMirrorMaker.class)) {
            kind = KafkaMirrorMaker.RESOURCE_KIND;
            crdApiVersion = KafkaMirrorMaker.CRD_API_VERSION;
            plural = KafkaMirrorMaker.RESOURCE_PLURAL;
            singular = KafkaMirrorMaker.RESOURCE_SINGULAR;
            listKind = KafkaMirrorMaker.RESOURCE_LIST_KIND;
            group = KafkaMirrorMaker.RESOURCE_GROUP;
            version = KafkaMirrorMaker.VERSION;
            shortNames = KafkaMirrorMaker.RESOURCE_SHORTNAMES;
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
                    .withGroup(group)
                    .withVersion(version)
                    .withNewNames()
                        .withKind(kind)
                        .withListKind(listKind)
                        .withPlural(plural)
                        .withSingular(singular)
                        .withShortNames(shortNames)
                    .endNames()
                .endSpec()
                .build();
    }

    public static CustomResourceDefinition kafka() {
        return crd(Kafka.class);
    }

    public static MixedOperation<Kafka, KafkaAssemblyList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafkaOperation(KubernetesClient client) {
        return client.customResources(kafka(), Kafka.class, KafkaAssemblyList.class, DoneableKafka.class);
    }

    public static CustomResourceDefinition kafkaConnect() {
        return crd(KafkaConnect.class);
    }

    public static MixedOperation<KafkaConnect, KafkaConnectAssemblyList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> kafkaConnectOperation(KubernetesClient client) {
        return client.customResources(kafkaConnect(), KafkaConnect.class, KafkaConnectAssemblyList.class, DoneableKafkaConnect.class);
    }

    public static CustomResourceDefinition kafkaConnectS2I() {
        return crd(KafkaConnectS2I.class);
    }

    public static <D extends CustomResourceDoneable<T>, T extends CustomResource> MixedOperation<KafkaConnectS2I, KafkaConnectS2IAssemblyList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> kafkaConnectS2iOperation(KubernetesClient client) {
        return client.customResources(Crds.kafkaConnectS2I(), KafkaConnectS2I.class, KafkaConnectS2IAssemblyList.class, DoneableKafkaConnectS2I.class);
    }

    public static CustomResourceDefinition topic() {
        return crd(KafkaTopic.class);
    }

    public static MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> topicOperation(KubernetesClient client) {
        return client.customResources(topic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    public static CustomResourceDefinition kafkaUser() {
        return crd(KafkaUser.class);
    }

    public static MixedOperation<KafkaUser, KafkaUserList, DoneableKafkaUser, Resource<KafkaUser, DoneableKafkaUser>> kafkaUserOperation(KubernetesClient client) {
        return client.customResources(kafkaUser(), KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class);
    }

    public static CustomResourceDefinition mirrorMaker() {
        return crd(KafkaMirrorMaker.class);
    }

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker, Resource<KafkaMirrorMaker, DoneableKafkaMirrorMaker>> mirrorMakerOperation(KubernetesClient client) {
        return client.customResources(mirrorMaker(), KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class);
    }

    public static <T extends CustomResource, L extends CustomResourceList<T>, D extends Doneable<T>> MixedOperation<T, L, D, Resource<T, D>>
            operation(KubernetesClient client,
                Class<T> cls,
                Class<L> listCls,
                Class<D> doneableCls) {
        return client.customResources(crd(cls), cls, listCls, doneableCls);
    }

    public static <T extends CustomResource> String kind(Class<T> cls) {
        try {
            return cls.newInstance().getKind();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
