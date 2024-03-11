/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetList;
import io.strimzi.systemtest.Environment;
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

    /**
     * Based on used mode - ZK, KRaft separate role, KRaft mixed role - returns the name of the controller's StrimziPodSet
     * In case of:
     *      - ZK mode, it returns {@link KafkaResources#zookeeperComponentName(String)}
     *      - KRaft mode - separate role, it returns {@link KafkaResource#getStrimziPodSetName(String, String)} with Pool name from
     *              {@link KafkaNodePoolResource#getControllerPoolName(String)}
     *      - KRaft mode - mixed role, it returns {@link KafkaResource#getStrimziPodSetName(String, String)} with Pool name from
     *              {@link KafkaNodePoolResource#getMixedPoolName(String)}
     * @param clusterName name of the Kafka cluster
     * @return component name of controller
     */
    public static String getControllerComponentName(String clusterName) {
        if (Environment.isKRaftModeEnabled()) {
            if (!Environment.isSeparateRolesMode()) {
                return KafkaResource.getStrimziPodSetName(clusterName, KafkaNodePoolResource.getMixedPoolName(clusterName));
            }
            return KafkaResource.getStrimziPodSetName(clusterName, KafkaNodePoolResource.getControllerPoolName(clusterName));
        }
        return KafkaResources.zookeeperComponentName(clusterName);
    }

    /**
     * Based on used mode - ZK, KRaft separate role, KRaft mixed role - returns the name of the broker's StrimziPodSet
     * In case of:
     *      - ZK mode, it returns {@link KafkaResources#kafkaComponentName(String)}
     *      - KRaft mode - separate role, it returns {@link KafkaResource#getStrimziPodSetName(String, String)} with Pool name from
     *              {@link KafkaNodePoolResource#getBrokerPoolName(String)}
     *      - KRaft mode - mixed role, it returns {@link KafkaResource#getStrimziPodSetName(String, String)} with Pool name from
     *              {@link KafkaNodePoolResource#getMixedPoolName(String)}
     * @param clusterName name of the Kafka cluster
     * @return component name of broker
     */
    public static String getBrokerComponentName(String clusterName) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            if (Environment.isKRaftModeEnabled() && !Environment.isSeparateRolesMode()) {
                return KafkaResource.getStrimziPodSetName(clusterName, KafkaNodePoolResource.getMixedPoolName(clusterName));
            }
            return KafkaResource.getStrimziPodSetName(clusterName, KafkaNodePoolResource.getBrokerPoolName(clusterName));
        }
        return KafkaResources.kafkaComponentName(clusterName);
    }
}
