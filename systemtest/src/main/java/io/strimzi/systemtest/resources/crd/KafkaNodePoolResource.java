/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaNodePoolList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.strimzi.operator.common.Util.hashStub;

public class KafkaNodePoolResource implements ResourceType<KafkaNodePool> {

    @Override
    public String getKind() {
        return KafkaNodePool.RESOURCE_KIND;
    }

    @Override
    public KafkaNodePool get(String namespace, String name) {
        return kafkaNodePoolClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(KafkaNodePool resource) {
        kafkaNodePoolClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaNodePool resource) {
        kafkaNodePoolClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).delete();
    }

    @Override
    public boolean waitForReadiness(KafkaNodePool resource) {
        return resource != null;
    }

    public static MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> kafkaNodePoolClient() {
        return Crds.kafkaNodePoolOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceKafkaNodePoolResourceInSpecificNamespace(String resourceName, Consumer<KafkaNodePool> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(KafkaNodePool.class, KafkaNodePoolList.class, resourceName, editor, namespaceName);
    }

    public static KafkaNodePool convertKafkaResourceToKafkaNodePool(Kafka resource) {
        List<ProcessRoles> nodeRoles = new ArrayList<>();
        nodeRoles.add(ProcessRoles.BROKER);

        if (Environment.isKRaftModeEnabled()) {
            nodeRoles.add(ProcessRoles.CONTROLLER);
        }

        String nodePoolName = Constants.KAFKA_NODE_POOL_PREFIX + hashStub(resource.getMetadata().getName());

        KafkaNodePoolBuilder builder = KafkaNodePoolTemplates.defaultKafkaNodePool(nodePoolName, resource.getMetadata().getName(), resource.getSpec().getKafka().getReplicas())
            .editOrNewMetadata()
                .withNamespace(resource.getMetadata().getNamespace())
                .addToLabels(resource.getMetadata().getLabels())
            .endMetadata()
            .editOrNewSpec()
                .withRoles(nodeRoles)
                .withStorage(resource.getSpec().getKafka().getStorage())
                .withJvmOptions(resource.getSpec().getKafka().getJvmOptions())
                .withResources(resource.getSpec().getKafka().getResources())
            .endSpec();

        if (resource.getSpec().getKafka().getTemplate() != null) {
            builder = builder
                .editOrNewSpec()
                    .editOrNewTemplate()
                        .withPersistentVolumeClaim(resource.getSpec().getKafka().getTemplate().getPersistentVolumeClaim())
                        .withPod(resource.getSpec().getKafka().getTemplate().getPod())
                    .endTemplate()
                .endSpec();
        }

        return builder.build();
    }
}
