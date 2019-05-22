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
import io.strimzi.api.kafka.model.DoneableKafkaBridge;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

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
        KafkaUser.class,
        KafkaMirrorMaker.class,
        KafkaBridge.class
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
            version = Kafka.VERSIONS.get(0);
        } else if (cls.equals(KafkaConnect.class)) {
            version = KafkaConnect.VERSIONS.get(0);
        } else if (cls.equals(KafkaConnectS2I.class)) {
            version = KafkaConnectS2I.VERSIONS.get(0);
        } else if (cls.equals(KafkaTopic.class)) {
            version = Kafka.VERSIONS.get(0);
        } else if (cls.equals(KafkaUser.class)) {
            version = Kafka.VERSIONS.get(0);
        } else if (cls.equals(KafkaMirrorMaker.class)) {
            version = KafkaMirrorMaker.VERSIONS.get(0);
        } else if (cls.equals(KafkaBridge.class)) {
            version = KafkaBridge.VERSIONS.get(0);
        } else {
            throw new RuntimeException();
        }

        return crd(cls, version);
    }

    private static CustomResourceDefinition crd(Class<? extends CustomResource> cls, String version) {
        String scope;
        String crdApiVersion;
        String plural;
        String group;
        if (cls.equals(Kafka.class)) {
            scope = Kafka.SCOPE;
            crdApiVersion = Kafka.CRD_API_VERSION;
            plural = Kafka.RESOURCE_PLURAL;
            group = Kafka.RESOURCE_GROUP;
            if (!Kafka.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaConnect.class)) {
            scope = KafkaConnect.SCOPE;
            crdApiVersion = KafkaConnect.CRD_API_VERSION;
            plural = KafkaConnect.RESOURCE_PLURAL;
            group = KafkaConnect.RESOURCE_GROUP;
            if (!KafkaConnect.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaConnectS2I.class)) {
            scope = KafkaConnectS2I.SCOPE;
            crdApiVersion = KafkaConnectS2I.CRD_API_VERSION;
            plural = KafkaConnectS2I.RESOURCE_PLURAL;
            group = KafkaConnectS2I.RESOURCE_GROUP;
            if (!KafkaConnectS2I.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaTopic.class)) {
            scope = KafkaTopic.SCOPE;
            crdApiVersion = KafkaTopic.CRD_API_VERSION;
            plural = KafkaTopic.RESOURCE_PLURAL;
            group = KafkaTopic.RESOURCE_GROUP;
            if (!KafkaTopic.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaUser.class)) {
            scope = KafkaUser.SCOPE;
            crdApiVersion = KafkaUser.CRD_API_VERSION;
            plural = KafkaUser.RESOURCE_PLURAL;
            group = KafkaUser.RESOURCE_GROUP;
            if (!KafkaUser.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaMirrorMaker.class)) {
            scope = KafkaMirrorMaker.SCOPE;
            crdApiVersion = KafkaMirrorMaker.CRD_API_VERSION;
            plural = KafkaMirrorMaker.RESOURCE_PLURAL;
            group = KafkaMirrorMaker.RESOURCE_GROUP;
            if (!KafkaMirrorMaker.VERSIONS.contains(version)) {
                throw new RuntimeException();
            }
        } else if (cls.equals(KafkaBridge.class)) {
            scope = KafkaBridge.SCOPE;
            crdApiVersion = KafkaBridge.CRD_API_VERSION;
            plural = KafkaBridge.RESOURCE_PLURAL;
            group = KafkaBridge.RESOURCE_GROUP;
            if (!KafkaBridge.VERSIONS.contains(version)) {
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
                        .withPlural(plural)
                    .endNames()
                .endSpec()
                .build();
    }

    public static CustomResourceDefinition kafka() {
        return crd(Kafka.class);
    }

    public static MixedOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafkaOperation(KubernetesClient client) {
        return client.customResources(kafka(), Kafka.class, KafkaList.class, DoneableKafka.class);
    }

    public static MixedOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafkaV1Alpha1Operation(KubernetesClient client) {
        return client.customResources(crd(Kafka.class, "v1alpha1"), Kafka.class, KafkaList.class, DoneableKafka.class);
    }

    public static CustomResourceDefinition kafkaConnect() {
        return crd(KafkaConnect.class);
    }

    public static MixedOperation<KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> kafkaConnectOperation(KubernetesClient client) {
        return client.customResources(kafkaConnect(), KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class);
    }

    public static CustomResourceDefinition kafkaConnectS2I() {
        return crd(KafkaConnectS2I.class);
    }

    public static <D extends CustomResourceDoneable<T>, T extends CustomResource> MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> kafkaConnectS2iOperation(KubernetesClient client) {
        return client.customResources(Crds.kafkaConnectS2I(), KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class);
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

    public static CustomResourceDefinition kafkaBridge() {
        return crd(KafkaBridge.class);
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, DoneableKafkaBridge, Resource<KafkaBridge, DoneableKafkaBridge>> kafkaBridgeOperation(KubernetesClient client) {
        return client.customResources(kafkaBridge(), KafkaBridge.class, KafkaBridgeList.class, DoneableKafkaBridge.class);
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
}
