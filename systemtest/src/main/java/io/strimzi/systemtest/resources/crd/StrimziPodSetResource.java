/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetList;
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

    public static void replaceStrimziPodSetInSpecificNamespace(String namespaceName, String resourceName, Consumer<StrimziPodSet> editor) {
        ResourceManager.replaceCrdResource(namespaceName, StrimziPodSet.class, StrimziPodSetList.class, resourceName, editor);
    }

    public static MixedOperation<StrimziPodSet, StrimziPodSetList, Resource<StrimziPodSet>> strimziPodSetClient() {
        return Crds.strimziPodSetOperation(ResourceManager.kubeClient().getClient());
    }

    /**
     * Returns the name of the controller's StrimziPodSet
     *
     * @param clusterName name of the Kafka cluster
     * @return component name of controller
     */
    public static String getControllerComponentName(String clusterName) {
        return KafkaResource.getStrimziPodSetName(clusterName, KafkaNodePoolResource.getControllerPoolName(clusterName));
    }

    /**
     * Returns the name of the broker's StrimziPodSet
     *
     * @param clusterName name of the Kafka cluster
     * @return component name of broker
     */
    public static String getBrokerComponentName(String clusterName) {
        return KafkaResource.getStrimziPodSetName(clusterName, KafkaNodePoolResource.getBrokerPoolName(clusterName));
    }
}
