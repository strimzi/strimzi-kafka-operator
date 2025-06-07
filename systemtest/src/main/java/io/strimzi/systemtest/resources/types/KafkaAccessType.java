/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.types;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.ResourceType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.kafka.access.model.KafkaAccess;
import io.strimzi.kafka.access.model.KafkaAccessStatus;

import java.util.Optional;
import java.util.function.Consumer;

public class KafkaAccessType implements ResourceType<KafkaAccess> {
    private MixedOperation<KafkaAccess, KubernetesResourceList<KafkaAccess>, Resource<KafkaAccess>> client;

    public KafkaAccessType() {
        client = KubeResourceManager.get().kubeClient().getClient().resources(KafkaAccess.class);
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return KafkaAccess.KIND;
    }

    @Override
    public void create(KafkaAccess kafkaAccess) {
        client.inNamespace(kafkaAccess.getMetadata().getNamespace()).resource(kafkaAccess).create();
    }

    @Override
    public void update(KafkaAccess kafkaAccess) {
        client.inNamespace(kafkaAccess.getMetadata().getNamespace()).resource(kafkaAccess).update();
    }

    @Override
    public void delete(KafkaAccess kafkaAccess) {
        client.inNamespace(kafkaAccess.getMetadata().getNamespace()).resource(kafkaAccess).delete();
    }

    @Override
    public void replace(KafkaAccess kafkaAccess, Consumer<KafkaAccess> consumer) {
        KafkaAccess toBeReplaced = client.inNamespace(kafkaAccess.getMetadata().getNamespace()).withName(kafkaAccess.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(KafkaAccess kafkaAccess) {
        KafkaAccessStatus kafkaAccessStatus = kafkaAccess.getStatus();
        Optional<Condition> readyCondition = kafkaAccessStatus.getConditions().stream().filter(condition -> condition.getType().equals("Ready")).findFirst();

        return readyCondition.map(condition -> condition.getStatus().equals("True")).orElse(false);
    }

    @Override
    public boolean isDeleted(KafkaAccess kafkaAccess) {
        return kafkaAccess == null;
    }
}