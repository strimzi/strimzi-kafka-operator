/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaNodePoolList;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;

import java.util.function.Consumer;

public class KafkaNodePoolResource implements ResourceType<KafkaNodePool> {

    @Override
    public String getKind() {
        return KafkaNodePool.RESOURCE_KIND;
    }

    @Override
    public KafkaNodePool get(String namespace, String name) {
        return kafkaNodePoolClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaNodePool resource) {
        kafkaNodePoolClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaNodePool resource) {
        kafkaNodePoolClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).delete();
    }

    @Override
    public boolean waitForReadiness(KafkaNodePool resource) {
        return resource != null;
    }

    public static MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> kafkaNodePoolClient() {
        return Crds.kafkaNodePoolOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceKafkaNodePoolResourceInSpecificNamespace(String resourceName, Consumer<KafkaNodePool> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(KafkaNodePool.class, KafkaNodePoolList.class, resourceName, editor, namespaceName);
    }
}
