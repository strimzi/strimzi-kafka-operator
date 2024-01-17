/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;

import java.util.function.Consumer;

public class KafkaConnectorResource implements ResourceType<KafkaConnector> {

    public KafkaConnectorResource() { }

    @Override
    public String getKind() {
        return KafkaConnector.RESOURCE_KIND;
    }
    @Override
    public KafkaConnector get(String namespace, String name) {
        return kafkaConnectorClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaConnector resource) {
        kafkaConnectorClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }
    @Override
    public void delete(KafkaConnector resource) {
        kafkaConnectorClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(KafkaConnector resource) {
        kafkaConnectorClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaConnector resource) {
        return KafkaConnectorUtils.waitForConnectorReady(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    public static MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorClient() {
        return Crds.kafkaConnectorOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceKafkaConnectorResourceInSpecificNamespace(String resourceName, Consumer<KafkaConnector> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(KafkaConnector.class, KafkaConnectorList.class, resourceName, editor, namespaceName);
    }
}
