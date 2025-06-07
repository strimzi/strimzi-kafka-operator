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
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2List;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.ResourceOperation;

import java.util.function.Consumer;

public class KafkaMirrorMaker2Type implements ResourceType<KafkaMirrorMaker2> {
    private final MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> client;

    public KafkaMirrorMaker2Type() {
        client = Crds.kafkaMirrorMaker2Operation(KubeResourceManager.get().kubeClient().getClient());
    }

    @Override
    public Long getTimeoutForResourceReadiness() {
        return ResourceOperation.getTimeoutForResourceReadiness(KafkaMirrorMaker2.RESOURCE_KIND);
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return KafkaMirrorMaker2.RESOURCE_KIND;
    }

    @Override
    public void create(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        client.inNamespace(kafkaMirrorMaker2.getMetadata().getNamespace()).resource(kafkaMirrorMaker2).create();
    }

    @Override
    public void update(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        client.inNamespace(kafkaMirrorMaker2.getMetadata().getNamespace()).resource(kafkaMirrorMaker2).update();
    }

    @Override
    public void delete(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        client.inNamespace(kafkaMirrorMaker2.getMetadata().getNamespace()).resource(kafkaMirrorMaker2).delete();
    }

    @Override
    public void replace(KafkaMirrorMaker2 kafkaMirrorMaker2, Consumer<KafkaMirrorMaker2> consumer) {
        KafkaMirrorMaker2 toBeReplaced = client.inNamespace(kafkaMirrorMaker2.getMetadata().getNamespace()).withName(kafkaMirrorMaker2.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        return ResourceConditions.resourceIsReady().predicate().test(kafkaMirrorMaker2);
    }

    @Override
    public boolean isDeleted(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        return kafkaMirrorMaker2 == null;
    }
}