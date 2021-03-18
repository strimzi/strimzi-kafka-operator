/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

public class KafkaConnectResource implements ResourceType<KafkaConnect> {

    public KafkaConnectResource() { }

    @Override
    public String getKind() {
        return KafkaConnect.RESOURCE_KIND;
    }
    @Override
    public KafkaConnect get(String namespace, String name) {
        return kafkaConnectClient().inNamespace(namespace).withName(name).get();
    }
    @Override
    public void create(KafkaConnect resource) {
        kafkaConnectClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).createOrReplace(resource);
    }
    @Override
    public void delete(KafkaConnect resource) throws Exception {
        kafkaConnectClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }
    @Override
    public boolean waitForReadiness(KafkaConnect resource) {
        return KafkaConnectUtils.waitForConnectReady(resource.getMetadata().getName());
    }

    public static MixedOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectClient() {
        return Crds.kafkaConnectOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceKafkaConnectResource(String resourceName, Consumer<KafkaConnect> editor) {
        ResourceManager.replaceCrdResource(KafkaConnect.class, KafkaConnectList.class, resourceName, editor);
    }
}
