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
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerList;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;

import java.util.function.Consumer;

// Deprecation is suppressed because of KafkaMirrorMaker
@SuppressWarnings("deprecation")
public class KafkaMirrorMakerResource implements ResourceType<KafkaMirrorMaker> {

    @Override
    public String getKind() {
        return KafkaMirrorMaker.RESOURCE_KIND;
    }
    @Override
    public KafkaMirrorMaker get(String namespace, String name) {
        return kafkaMirrorMakerClient().inNamespace(namespace).withName(name).get();
    }
    @Override
    public void create(KafkaMirrorMaker resource) {
        kafkaMirrorMakerClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }
    @Override
    public void delete(KafkaMirrorMaker resource) {
        kafkaMirrorMakerClient().inNamespace(resource.getMetadata().getNamespace()).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(KafkaMirrorMaker resource) {
        kafkaMirrorMakerClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaMirrorMaker resource) {
        return ResourceManager.waitForResourceStatus(kafkaMirrorMakerClient(), resource, CustomResourceStatus.Ready);
    }

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, Resource<KafkaMirrorMaker>> kafkaMirrorMakerClient() {
        return Crds.mirrorMakerOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceMirrorMakerResourceInSpecificNamespace(String resourceName, Consumer<KafkaMirrorMaker> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(KafkaMirrorMaker.class, KafkaMirrorMakerList.class, resourceName, editor, namespaceName);
    }

    public static LabelSelector getLabelSelector(String clusterName, String componentName) {
        java.util.Map<String, String> matchLabels = new java.util.HashMap<>();
        matchLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        matchLabels.put(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker.RESOURCE_KIND);
        matchLabels.put(Labels.STRIMZI_NAME_LABEL, componentName);

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }
}
