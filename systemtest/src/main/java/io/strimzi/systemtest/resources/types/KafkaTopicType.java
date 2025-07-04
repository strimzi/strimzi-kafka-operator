/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.types;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.ResourceType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicList;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.ResourceOperation;

import java.util.function.Consumer;

public class KafkaTopicType implements ResourceType<KafkaTopic> {
    private final MixedOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> client;

    public KafkaTopicType() {
        client = Crds.topicOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    @Override
    public Long getTimeoutForResourceReadiness() {
        return ResourceOperation.getTimeoutForResourceReadiness(KafkaTopic.RESOURCE_KIND);
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return KafkaTopic.RESOURCE_KIND;
    }

    @Override
    public void create(KafkaTopic kafkaTopic) {
        client.inNamespace(kafkaTopic.getMetadata().getNamespace()).resource(kafkaTopic).create();
    }

    @Override
    public void update(KafkaTopic kafkaTopic) {
        client.inNamespace(kafkaTopic.getMetadata().getNamespace()).resource(kafkaTopic).update();
    }

    @Override
    public void delete(KafkaTopic kafkaTopic) {
        client.inNamespace(kafkaTopic.getMetadata().getNamespace()).resource(kafkaTopic).delete();
    }

    @Override
    public void replace(KafkaTopic kafkaTopic, Consumer<KafkaTopic> consumer) {
        KafkaTopic toBeReplaced = client.inNamespace(kafkaTopic.getMetadata().getNamespace()).withName(kafkaTopic.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(KafkaTopic kafkaTopic) {
        return ResourceConditions.resourceIsReady().predicate().test(kafkaTopic);
    }

    @Override
    public boolean isDeleted(KafkaTopic kafkaTopic) {
        return client.inNamespace(kafkaTopic.getMetadata().getNamespace()).withName(kafkaTopic.getMetadata().getName()).get() == null;
    }
}