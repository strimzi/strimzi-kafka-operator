/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;

public class KafkaTopicResource implements ResourceType<KafkaTopic> {

    public KafkaTopicResource() {}

    @Override
    public String getKind() {
        return KafkaTopic.RESOURCE_KIND;
    }
    @Override
    public KafkaTopic get(String namespace, String name) {
        return kafkaTopicClient().inNamespace(namespace).withName(name).get();
    }
    @Override
    public void create(KafkaTopic resource) {
        kafkaTopicClient().inNamespace(resource.getMetadata().getNamespace()).createOrReplace(resource);
    }
    @Override
    public void delete(KafkaTopic resource) throws Exception {
        kafkaTopicClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }
    @Override
    public boolean isReady(KafkaTopic resource) {
        return ResourceManager.waitForResourceStatus(kafkaTopicClient(), resource, CustomResourceStatus.Ready);
    }
    @Override
    public void refreshResource(KafkaTopic existing, KafkaTopic newResource) {
        existing.setMetadata(newResource.getMetadata());
        existing.setSpec(newResource.getSpec());
        existing.setStatus(newResource.getStatus());
    }
    public static MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> kafkaTopicClient() {
        return Crds.topicOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceTopicResource(String resourceName, Consumer<KafkaTopic> editor) {
        ResourceManager.replaceCrdResource(KafkaTopic.class, KafkaTopicList.class, resourceName, editor);
    }
}
