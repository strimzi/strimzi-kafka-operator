/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaBridgeList;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

public class KafkaBridgeResource implements ResourceType<KafkaBridge> {

    public KafkaBridgeResource() { }

    @Override
    public String getKind() {
        return KafkaBridge.RESOURCE_KIND;
    }
    @Override
    public KafkaBridge get(String namespace, String name) {
        return kafkaBridgeClient().inNamespace(namespace).withName(name).get();
    }
    @Override
    public void create(KafkaBridge resource) {
        kafkaBridgeClient().inNamespace(resource.getMetadata().getNamespace()).createOrReplace(resource);
    }
    @Override
    public void delete(KafkaBridge resource) throws Exception {
        kafkaBridgeClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }
    @Override
    public boolean waitForReadiness(KafkaBridge resource) {
        return ResourceManager.waitForResourceStatus(kafkaBridgeClient(), resource, CustomResourceStatus.Ready);
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeClient() {
        return Crds.kafkaBridgeOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceBridgeResource(String resourceName, Consumer<KafkaBridge> editor) {
        ResourceManager.replaceCrdResource(KafkaBridge.class, KafkaBridgeList.class, resourceName, editor);
    }
}
