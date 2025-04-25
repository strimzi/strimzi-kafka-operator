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
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectList;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.systemtest.resources.ResourceOperation;

import java.util.Optional;
import java.util.function.Consumer;

public class KafkaConnectType implements ResourceType<KafkaConnect> {
    private final MixedOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> client;

    public KafkaConnectType() {
        client = Crds.kafkaConnectOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    @Override
    public Long getTimeoutForResourceReadiness() {
        return ResourceOperation.getTimeoutForResourceReadiness(KafkaConnect.RESOURCE_KIND);
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return KafkaConnect.RESOURCE_KIND;
    }

    @Override
    public void create(KafkaConnect kafkaConnect) {
        client.inNamespace(kafkaConnect.getMetadata().getNamespace()).resource(kafkaConnect).create();
    }

    @Override
    public void update(KafkaConnect kafkaConnect) {
        client.inNamespace(kafkaConnect.getMetadata().getNamespace()).resource(kafkaConnect).update();
    }

    @Override
    public void delete(KafkaConnect kafkaConnect) {
        client.inNamespace(kafkaConnect.getMetadata().getNamespace()).resource(kafkaConnect).delete();
    }

    @Override
    public void replace(KafkaConnect kafkaConnect, Consumer<KafkaConnect> consumer) {
        KafkaConnect toBeReplaced = client.inNamespace(kafkaConnect.getMetadata().getNamespace()).withName(kafkaConnect.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(KafkaConnect kafkaConnect) {
        KafkaConnectStatus kafkaConnectStatus = client.inNamespace(kafkaConnect.getMetadata().getNamespace()).resource(kafkaConnect).get().getStatus();
        Optional<Condition> readyCondition = kafkaConnectStatus.getConditions().stream().filter(condition -> condition.getType().equals("Ready")).findFirst();

        return readyCondition.map(condition -> condition.getStatus().equals("True")).orElse(false);
    }

    @Override
    public boolean isDeleted(KafkaConnect kafkaConnect) {
        return kafkaConnect == null;
    }
}