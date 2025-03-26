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
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.ResourceOperation;

import java.util.function.Consumer;

public class KafkaBridgeType implements ResourceType<KafkaBridge> {
    private final MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> client;

    public KafkaBridgeType() {
        client = Crds.kafkaBridgeOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    @Override
    public Long getTimeoutForResourceReadiness() {
        return ResourceOperation.getTimeoutForResourceReadiness(KafkaBridge.RESOURCE_KIND);
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return KafkaBridge.RESOURCE_KIND;
    }

    @Override
    public void create(KafkaBridge kafkaBridge) {
        client.inNamespace(kafkaBridge.getMetadata().getNamespace()).resource(kafkaBridge).create();
    }

    @Override
    public void update(KafkaBridge kafkaBridge) {
        client.inNamespace(kafkaBridge.getMetadata().getNamespace()).resource(kafkaBridge).update();
    }

    @Override
    public void delete(KafkaBridge kafkaBridge) {
        client.inNamespace(kafkaBridge.getMetadata().getNamespace()).resource(kafkaBridge).delete();
    }

    @Override
    public void replace(KafkaBridge kafkaBridge, Consumer<KafkaBridge> consumer) {
        KafkaBridge toBeReplaced = client.inNamespace(kafkaBridge.getMetadata().getNamespace()).withName(kafkaBridge.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(KafkaBridge kafkaBridge) {
        return ResourceConditions.resourceIsReady().predicate().test(kafkaBridge);
    }

    @Override
    public boolean isDeleted(KafkaBridge kafkaBridge) {
        return kafkaBridge == null;
    }
}