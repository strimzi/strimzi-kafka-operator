/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.ResourceType;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;

public class KafkaResource implements ResourceType<Kafka> {

    public KafkaResource() {}

    @Override
    public String getKind() {
        return Kafka.RESOURCE_KIND;
    }
    @Override
    public Kafka get(String namespace, String name) {
        return kafkaClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(Kafka resource) {
        kafkaClient().inNamespace(resource.getMetadata().getNamespace()).createOrReplace(resource);
    }

    @Override
    public void delete(Kafka resource) throws Exception {
        kafkaClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public boolean waitForReadiness(Kafka resource) {
        long timeout = ResourceOperation.getTimeoutForResourceReadiness(resource.getKind());

        // Kafka Exporter is not setup every time
        if (resource.getSpec().getKafkaExporter() != null) {
            timeout += ResourceOperation.getTimeoutForResourceReadiness(Constants.KAFKA_EXPORTER_DEPLOYMENT);
        }
        // Cruise Control is not setup every time
        if (resource.getSpec().getCruiseControl() != null) {
            timeout += ResourceOperation.getTimeoutForResourceReadiness(Constants.KAFKA_CRUISE_CONTROL_DEPLOYMENT);
        }
        return ResourceManager.waitForResourceStatus(kafkaClient(), resource, Ready, timeout);
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaClient() {
        return Crds.kafkaOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceKafkaResource(String resourceName, Consumer<Kafka> editor) {
        ResourceManager.replaceCrdResource(Kafka.class, KafkaList.class, resourceName, editor);
    }

    public static KafkaStatus getKafkaStatus(String clusterName, String namespace) {
        return kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus();
    }
}
