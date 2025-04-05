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
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetList;
import io.strimzi.api.kafka.model.podset.StrimziPodSetStatus;

import java.util.Optional;
import java.util.function.Consumer;

public class StrimziPodSetType implements ResourceType<StrimziPodSet> {

    private MixedOperation<StrimziPodSet, StrimziPodSetList, Resource<StrimziPodSet>> client;

    public StrimziPodSetType() {
        client = Crds.strimziPodSetOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return StrimziPodSet.RESOURCE_KIND;
    }

    @Override
    public void create(StrimziPodSet strimziPodSet) {
        client.inNamespace(strimziPodSet.getMetadata().getNamespace()).resource(strimziPodSet).create();
    }

    @Override
    public void update(StrimziPodSet strimziPodSet) {
        client.inNamespace(strimziPodSet.getMetadata().getNamespace()).resource(strimziPodSet).update();
    }

    @Override
    public void delete(StrimziPodSet strimziPodSet) {
        client.inNamespace(strimziPodSet.getMetadata().getNamespace()).resource(strimziPodSet).delete();
    }

    @Override
    public void replace(StrimziPodSet strimziPodSet, Consumer<StrimziPodSet> consumer) {
        StrimziPodSet toBeReplaced = client.inNamespace(strimziPodSet.getMetadata().getNamespace()).withName(strimziPodSet.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(StrimziPodSet strimziPodSet) {
        StrimziPodSetStatus strimziPodSetStatus = client.inNamespace(strimziPodSet.getMetadata().getNamespace()).resource(strimziPodSet).get().getStatus();
        Optional<Condition> readyCondition = strimziPodSetStatus.getConditions().stream().filter(condition -> condition.getType().equals("Ready")).findFirst();

        return readyCondition.map(condition -> condition.getStatus().equals("True")).orElse(false);
    }

    @Override
    public boolean isDeleted(StrimziPodSet strimziPodSet) {
        return strimziPodSet == null;
    }
}