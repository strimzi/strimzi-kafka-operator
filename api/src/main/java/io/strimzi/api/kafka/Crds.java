/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.internal.KubernetesDeserializer;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;
import io.strimzi.api.kafka.model.KafkaConnectS2IAssembly;
import io.strimzi.api.kafka.model.Topic;

/**
 * "Static" information about the CRDs defined in this package
 */
public class Crds {

    public static final String CRD_KIND = "CustomResourceDefinition";

    private static final Class<? extends CustomResource>[] CRDS = new Class[] {
        KafkaAssembly.class,
        KafkaConnectAssembly.class,
        KafkaConnectS2IAssembly.class
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
        String[] shortNames = new String[0];
        if (cls.equals(KafkaAssembly.class)) {
            kind = KafkaAssembly.RESOURCE_KIND;
            crdApiVersion = KafkaAssembly.CRD_API_VERSION;
            plural = KafkaAssembly.RESOURCE_PLURAL;
            singular = KafkaAssembly.RESOURCE_SINGULAR;
            listKind = KafkaAssembly.RESOURCE_LIST_KIND;
            group = KafkaAssembly.RESOURCE_GROUP;
            version = KafkaAssembly.VERSION;
        } else if (cls.equals(KafkaConnectAssembly.class)) {
            kind = KafkaConnectAssembly.RESOURCE_KIND;
            crdApiVersion = KafkaConnectAssembly.CRD_API_VERSION;
            plural = KafkaConnectAssembly.RESOURCE_PLURAL;
            singular = KafkaConnectAssembly.RESOURCE_SINGULAR;
            listKind = KafkaConnectAssembly.RESOURCE_LIST_KIND;
            group = KafkaConnectAssembly.RESOURCE_GROUP;
            version = KafkaConnectAssembly.VERSION;
        } else if (cls.equals(KafkaConnectS2IAssembly.class)) {
            kind = KafkaConnectS2IAssembly.RESOURCE_KIND;
            crdApiVersion = KafkaConnectS2IAssembly.CRD_API_VERSION;
            plural = KafkaConnectS2IAssembly.RESOURCE_PLURAL;
            singular = KafkaConnectS2IAssembly.RESOURCE_SINGULAR;
            listKind = KafkaConnectS2IAssembly.RESOURCE_LIST_KIND;
            group = KafkaConnectS2IAssembly.RESOURCE_GROUP;
            version = KafkaConnectS2IAssembly.VERSION;
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
        return crd(KafkaAssembly.class);
    }

    public static MixedOperation<KafkaAssembly, KafkaAssemblyList, DoneableKafkaAssembly, Resource<KafkaAssembly, DoneableKafkaAssembly>> kafkaOperation(KubernetesClient client) {
        return client.customResources(kafka(), KafkaAssembly.class, KafkaAssemblyList.class, DoneableKafkaAssembly.class);
    }

    public static CustomResourceDefinition kafkaConnect() {
        return crd(KafkaConnectAssembly.class);
    }

    public static MixedOperation<KafkaConnectAssembly, KafkaConnectAssemblyList, DoneableKafkaConnectAssembly, Resource<KafkaConnectAssembly, DoneableKafkaConnectAssembly>> kafkaConnectOperation(KubernetesClient client) {
        return client.customResources(kafkaConnect(), KafkaConnectAssembly.class, KafkaConnectAssemblyList.class, DoneableKafkaConnectAssembly.class);
    }

    public static CustomResourceDefinition kafkaConnectS2I() {
        return crd(KafkaConnectS2IAssembly.class);
    }

    public static <D extends CustomResourceDoneable<T>, T extends CustomResource> MixedOperation<KafkaConnectS2IAssembly, KafkaConnectS2IAssemblyList, DoneableKafkaConnectS2IAssembly, Resource<KafkaConnectS2IAssembly, DoneableKafkaConnectS2IAssembly>> kafkaConnectS2iOperation(KubernetesClient client) {
        return client.customResources(Crds.kafkaConnectS2I(), KafkaConnectS2IAssembly.class, KafkaConnectS2IAssemblyList.class, DoneableKafkaConnectS2IAssembly.class);
    }

    public static CustomResourceDefinition topic() {
        return crd(Topic.class);
    }

    public static MixedOperation<Topic, TopicList, DoneableTopic, Resource<Topic, DoneableTopic>> topicOperation(KubernetesClient client) {
        return client.customResources(topic(), Topic.class, TopicList.class, DoneableTopic.class);
    }

    public static <T extends CustomResource, L extends CustomResourceList<T>, D extends CustomResourceDoneable<T>> MixedOperation<T, L, D, Resource<T, D>>
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
