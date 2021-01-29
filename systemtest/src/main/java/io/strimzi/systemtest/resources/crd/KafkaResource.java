/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

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
    public boolean isReady(Kafka resource) {
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

    @Override
    public void refreshResource(Kafka existing, Kafka newResource) {
        existing.setMetadata(newResource.getMetadata());
        existing.setSpec(newResource.getSpec());
        existing.setStatus(newResource.getStatus());
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaClient() {
        return Crds.kafkaV1Beta2Operation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceKafkaResource(String resourceName, Consumer<Kafka> editor) {
        ResourceManager.replaceCrdResource(Kafka.class, KafkaList.class, resourceName, editor);
    }

    public static KafkaStatus getKafkaStatus(String clusterName, String namespace) {
        return kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus();
    }
}
