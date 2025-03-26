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
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;

import java.util.Optional;
import java.util.function.Consumer;

public class KafkaUserType implements ResourceType<KafkaUser> {
    private final MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> client;

    public KafkaUserType() {
        client = Crds.kafkaUserOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return KafkaUser.RESOURCE_KIND;
    }

    @Override
    public void create(KafkaUser kafkaUser) {
        client.inNamespace(kafkaUser.getMetadata().getNamespace()).resource(kafkaUser).create();
    }

    @Override
    public void update(KafkaUser kafkaUser) {
        client.inNamespace(kafkaUser.getMetadata().getNamespace()).resource(kafkaUser).update();
    }

    @Override
    public void delete(KafkaUser kafkaUser) {
        client.inNamespace(kafkaUser.getMetadata().getNamespace()).resource(kafkaUser).delete();
    }

    @Override
    public void replace(KafkaUser kafkaUser, Consumer<KafkaUser> consumer) {
        KafkaUser toBeReplaced = client.inNamespace(kafkaUser.getMetadata().getNamespace()).withName(kafkaUser.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(KafkaUser kafkaUser) {
        KafkaUserStatus kafkaUserStatus = client.inNamespace(kafkaUser.getMetadata().getNamespace()).resource(kafkaUser).get().getStatus();
        Optional<Condition> readyCondition = kafkaUserStatus.getConditions().stream().filter(condition -> condition.getType().equals("Ready")).findFirst();

        return readyCondition.map(condition -> condition.getStatus().equals("True")).orElse(false);
    }

    @Override
    public boolean isDeleted(KafkaUser kafkaUser) {
        return kafkaUser == null;
    }
}