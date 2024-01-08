/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;

import java.util.HashMap;
import java.util.Map;
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
        kafkaBridgeClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }
    @Override
    public void delete(KafkaBridge resource) {
        kafkaBridgeClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(KafkaBridge resource) {
        kafkaBridgeClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaBridge resource) {
        return ResourceManager.waitForResourceStatus(kafkaBridgeClient(), resource, CustomResourceStatus.Ready);
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeClient() {
        return Crds.kafkaBridgeOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceBridgeResourceInSpecificNamespace(String resourceName, Consumer<KafkaBridge> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(KafkaBridge.class, KafkaBridgeList.class, resourceName, editor, namespaceName);
    }

    public static LabelSelector getLabelSelector(String clusterName, String componentName) {
        Map<String, String> matchLabels = new HashMap<>();
        matchLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        matchLabels.put(Labels.STRIMZI_KIND_LABEL, KafkaBridge.RESOURCE_KIND);
        matchLabels.put(Labels.STRIMZI_NAME_LABEL, componentName);

        return new LabelSelectorBuilder()
                .withMatchLabels(matchLabels)
                .build();
    }
}
