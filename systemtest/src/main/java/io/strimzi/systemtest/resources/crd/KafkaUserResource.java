/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;

public class KafkaUserResource implements ResourceType<KafkaUser> {

    public KafkaUserResource() {}

    @Override
    public String getKind() {
        return KafkaUser.RESOURCE_KIND;
    }
    @Override
    public KafkaUser get(String namespace, String name) {
        return kafkaUserClient().inNamespace(namespace).withName(name).get();
    }
    @Override
    public void create(KafkaUser resource) {
        kafkaUserClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaUser resource) {
        kafkaUserClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public boolean waitForReadiness(KafkaUser resource) {
        return ResourceManager.waitForResourceStatus(kafkaUserClient(), resource, Ready);
    }

    public static MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> kafkaUserClient() {
        return Crds.kafkaUserOperation(ResourceManager.kubeClient().getClient());
    }

    @Override
    public void update(KafkaUser kafkaUser) {
        kafkaUserClient().inNamespace(kafkaUser.getMetadata().getNamespace()).resource(kafkaUser).update();
    }

    public static void replaceUserResourceInSpecificNamespace(String resourceName, Consumer<KafkaUser> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(KafkaUser.class, KafkaUserList.class, resourceName, editor, namespaceName);
    }
}
