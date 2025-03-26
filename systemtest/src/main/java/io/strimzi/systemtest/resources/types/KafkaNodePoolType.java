/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.types;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.ResourceType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;

import java.util.function.Consumer;

public class KafkaNodePoolType implements ResourceType<KafkaNodePool> {
    private final MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> client;

    public KafkaNodePoolType() {
        client = Crds.kafkaNodePoolOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return KafkaNodePool.RESOURCE_KIND;
    }

    @Override
    public void create(KafkaNodePool kafkaNodePool) {
        client.inNamespace(kafkaNodePool.getMetadata().getNamespace()).resource(kafkaNodePool).create();
    }

    @Override
    public void update(KafkaNodePool kafkaNodePool) {
        client.inNamespace(kafkaNodePool.getMetadata().getNamespace()).resource(kafkaNodePool).update();
    }

    @Override
    public void delete(KafkaNodePool kafkaNodePool) {
        String namespaceName = kafkaNodePool.getMetadata().getNamespace();
        String clusterName = kafkaNodePool.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        String podsetName = String.join("-", clusterName, kafkaNodePool.getMetadata().getName());

        client.inNamespace(kafkaNodePool.getMetadata().getNamespace()).resource(kafkaNodePool).delete();

        // once KafkaNodePool is deleted, we should delete PVCs and wait for the deletion to be completed
        // the Kafka pods will not be deleted during Kafka resource deletion -> so if we would delete PVCs during Kafka deletion
        // they will not be deleted too
        // additional deletion of pvcs with specification deleteClaim set to false which were not deleted prior this method
        PersistentVolumeClaimUtils.deletePvcsByPrefixWithWait(namespaceName, podsetName);
    }

    @Override
    public void replace(KafkaNodePool kafkaNodePool, Consumer<KafkaNodePool> consumer) {
        KafkaNodePool toBeReplaced = client.inNamespace(kafkaNodePool.getMetadata().getNamespace()).withName(kafkaNodePool.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(KafkaNodePool kafkaNodePool) {
        return kafkaNodePool != null;
    }

    @Override
    public boolean isDeleted(KafkaNodePool kafkaNodePool) {
        return kafkaNodePool == null;
    }
}