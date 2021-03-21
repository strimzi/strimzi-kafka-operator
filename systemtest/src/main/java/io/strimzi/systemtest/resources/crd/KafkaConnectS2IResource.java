/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

// Deprecation is suppressed because of KafkaConnectS2I
@SuppressWarnings("deprecation")
public class KafkaConnectS2IResource implements ResourceType<KafkaConnectS2I> {

    public KafkaConnectS2IResource() { }

    @Override
    public String getKind() {
        return KafkaConnectS2I.RESOURCE_KIND;
    }
    @Override
    public KafkaConnectS2I get(String namespace, String name) {
        return kafkaConnectS2IClient().inNamespace(namespace).withName(name).get();
    }
    @Override
    public void create(KafkaConnectS2I resource) {
        kafkaConnectS2IClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).createOrReplace(resource);
    }
    @Override
    public void delete(KafkaConnectS2I resource) throws Exception {
        kafkaConnectS2IClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public boolean waitForReadiness(KafkaConnectS2I resource) {
        return ResourceManager.waitForResourceStatus(kafkaConnectS2IClient(), resource, CustomResourceStatus.Ready);
    }

    public static MixedOperation<KafkaConnectS2I, KafkaConnectS2IList, Resource<KafkaConnectS2I>> kafkaConnectS2IClient() {
        return Crds.kafkaConnectS2iOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceConnectS2IResource(String resourceName, Consumer<KafkaConnectS2I> editor) {
        ResourceManager.replaceCrdResource(KafkaConnectS2I.class, KafkaConnectS2IList.class, resourceName, editor);
    }
}
