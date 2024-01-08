/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.StrimziPodSetList;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;

public class StrimziPodSetResource implements ResourceType<StrimziPodSet> {
    @Override
    public String getKind() {
        return StrimziPodSet.RESOURCE_KIND;
    }

    @Override
    public StrimziPodSet get(String namespace, String name) {
        return strimziPodSetClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(StrimziPodSet resource) {
        strimziPodSetClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(StrimziPodSet resource) {
        strimziPodSetClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).delete();
    }

    @Override
    public void update(StrimziPodSet resource) {
        strimziPodSetClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(StrimziPodSet resource) {
        return ResourceManager.waitForResourceStatus(strimziPodSetClient(), resource, Ready);
    }

    public static void replaceStrimziPodSetInSpecificNamespace(String resourceName, Consumer<StrimziPodSet> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(StrimziPodSet.class, StrimziPodSetList.class, resourceName, editor, namespaceName);
    }

    public static MixedOperation<StrimziPodSet, StrimziPodSetList, Resource<StrimziPodSet>> strimziPodSetClient() {
        return Crds.strimziPodSetOperation(ResourceManager.kubeClient().getClient());
    }
}
