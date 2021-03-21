/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

public class KafkaMirrorMaker2Resource implements ResourceType<KafkaMirrorMaker2> {

    @Override
    public String getKind() {
        return KafkaMirrorMaker2.RESOURCE_KIND;
    }
    @Override
    public KafkaMirrorMaker2 get(String namespace, String name) {
        return kafkaMirrorMaker2Client().inNamespace(namespace).withName(name).get();
    }
    @Override
    public void create(KafkaMirrorMaker2 resource) {
        kafkaMirrorMaker2Client().inNamespace(resource.getMetadata().getNamespace()).createOrReplace(resource);
    }
    @Override
    public void delete(KafkaMirrorMaker2 resource) throws Exception {
        kafkaMirrorMaker2Client().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }
    @Override
    public boolean waitForReadiness(KafkaMirrorMaker2 resource) {
        return KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(resource.getMetadata().getName());
    }

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> kafkaMirrorMaker2Client() {
        return Crds.kafkaMirrorMaker2Operation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceKafkaMirrorMaker2Resource(String resourceName, Consumer<KafkaMirrorMaker2> editor) {
        ResourceManager.replaceCrdResource(KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, resourceName, editor);
    }

}
