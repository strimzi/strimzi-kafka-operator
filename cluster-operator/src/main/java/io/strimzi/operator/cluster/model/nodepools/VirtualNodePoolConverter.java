/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaClusterTemplate;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolTemplate;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolTemplateBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Class which contains utility methods for converting the .spec.kafka section from the Kafka CR into a virtual KafkaNodePool resource
 */
public class VirtualNodePoolConverter {
    /**
     * Name of the default virtual node pool. This matches the suffix of the Kafka StrimziPodSet or StatefulSet created
     * when node pools are not used which is "kafka".
     */
    public static final String DEFAULT_NODE_POOL_NAME = "kafka";

    /**
     * Converts the Kafka CR into a virtual node pool by copying the corresponding fields.
     *
     * @param kafka             The Kafka custom resource
     * @param existingReplicas  Existing number of replicas which is used to generate the Node IDs
     *
     * @return  The newly generated node pool
     */
    public static KafkaNodePool convertKafkaToVirtualNodePool(Kafka kafka, Integer existingReplicas) {
        List<Integer> nodeIds = null;

        if (existingReplicas != null && existingReplicas > 0)   {
            // We have at least one existing replica => we prepare the existing node IDs accordingly
            nodeIds = IntStream.range(0, existingReplicas).boxed().toList();
        }

        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName(DEFAULT_NODE_POOL_NAME)
                    .withNamespace(kafka.getMetadata().getNamespace())
                    .withLabels(kafka.getMetadata().getLabels())
                .endMetadata()
                .withNewSpec()
                    .withReplicas(kafka.getSpec().getKafka().getReplicas())
                    .withStorage(kafka.getSpec().getKafka().getStorage())
                    .withRoles(List.of(ProcessRoles.BROKER)) // We do not need to care about the controller role here since this is only used with ZooKeeper based clusters
                    .withResources(kafka.getSpec().getKafka().getResources())
                    .withJvmOptions(kafka.getSpec().getKafka().getJvmOptions())
                    .withTemplate(convertTemplate(kafka.getSpec().getKafka().getTemplate()))
                .endSpec()
                .withNewStatus()
                    .withNodeIds(nodeIds)
                    .withRoles(ProcessRoles.BROKER)
                .endStatus()
                .build();
    }

    /**
     * Copied any existing template fields which are set in the Kafka CR and are supported in the KafkaNodePool CR to
     * the new virtual node pool.
     *
     * @param template  The Kafka template from the Kafka CR
     *
     * @return  New generated node pool template or null if the Kafka template is null in the Kafka CR.
     */
    /* test */ static KafkaNodePoolTemplate convertTemplate(KafkaClusterTemplate template) {
        if (template != null)   {
            return new KafkaNodePoolTemplateBuilder()
                    .withPodSet(template.getPodSet())
                    .withPod(template.getPod())
                    .withPerPodService(template.getPerPodService())
                    .withPerPodRoute(template.getPerPodRoute())
                    .withPerPodIngress(template.getPerPodIngress())
                    .withPersistentVolumeClaim(template.getPersistentVolumeClaim())
                    .withKafkaContainer(template.getKafkaContainer())
                    .withInitContainer(template.getInitContainer())
                    .build();
        } else {
            return null;
        }
    }
}
