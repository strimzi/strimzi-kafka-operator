/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaNodePoolList;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.strimzi.operator.common.Util.hashStub;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

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
        String namespaceName = resource.getMetadata().getNamespace();
        String clusterName = resource.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String podsetName = String.join("-", clusterName, resource.getMetadata().getName());

        kafkaNodePoolClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).delete();

        // once KafkaNodePool is deleted, we should delete PVCs and wait for the deletion to be completed
        // the Kafka pods will not be deleted during Kafka resource deletion -> so if we would delete PVCs during Kafka deletion
        // they will not be deleted too
        // additional deletion of pvcs with specification deleteClaim set to false which were not deleted prior this method
        PersistentVolumeClaimUtils.deletePvcsByPrefixWithWait(namespaceName, podsetName);
    }

    @Override
    public void update(KafkaNodePool resource) {
        kafkaNodePoolClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(KafkaNodePool resource) {
        return resource != null;
    }

    public static MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> kafkaNodePoolClient() {
        return Crds.kafkaNodePoolOperation(kubeClient().getClient());
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

        String nodePoolName = TestConstants.KAFKA_NODE_POOL_PREFIX + hashStub(resource.getMetadata().getName());

        KafkaNodePoolBuilder builder = KafkaNodePoolTemplates.defaultKafkaNodePool(resource.getMetadata().getNamespace(), nodePoolName, resource.getMetadata().getName(), resource.getSpec().getKafka().getReplicas())
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

    public static LabelSelector getLabelSelector(String clusterName, String poolName, ProcessRoles processRole) {
        Map<String, String> matchLabels = new HashMap<>();
        if (clusterName != null) {
            matchLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        }
        matchLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        matchLabels.put(Labels.STRIMZI_POOL_NAME_LABEL, poolName);

        switch (processRole) {
            case BROKER -> matchLabels.put(Labels.STRIMZI_BROKER_ROLE_LABEL, "true");
            case CONTROLLER -> matchLabels.put(Labels.STRIMZI_CONTROLLER_ROLE_LABEL, "true");
            default -> throw new RuntimeException("No role for KafkaNodePool specified");
        }

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }

    public static LabelSelector getLabelSelector(final String poolName, final ProcessRoles processRoles) {
        return getLabelSelector(null, poolName, processRoles);
    }

    public static String getStrimziPodSetName(String kafkaClusterName, String kafkaNodePoolName) {
        return kafkaClusterName + "-" + kafkaNodePoolName;
    }

}
