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
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.systemtest.resources.ResourceOperation;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class KafkaRebalanceType implements ResourceType<KafkaRebalance> {
    private final MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> client;

    public KafkaRebalanceType() {
        client = Crds.kafkaRebalanceOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    @Override
    public Long getTimeoutForResourceReadiness() {
        return ResourceOperation.getTimeoutForKafkaRebalanceState(KafkaRebalanceState.ProposalReady);
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return KafkaRebalance.RESOURCE_KIND;
    }

    @Override
    public void create(KafkaRebalance kafkaRebalance) {
        client.inNamespace(kafkaRebalance.getMetadata().getNamespace()).resource(kafkaRebalance).create();
    }

    @Override
    public void update(KafkaRebalance kafkaRebalance) {
        client.inNamespace(kafkaRebalance.getMetadata().getNamespace()).resource(kafkaRebalance).update();
    }

    @Override
    public void delete(KafkaRebalance kafkaRebalance) {
        client.inNamespace(kafkaRebalance.getMetadata().getNamespace()).resource(kafkaRebalance).delete();
    }

    @Override
    public void replace(KafkaRebalance kafkaRebalance, Consumer<KafkaRebalance> consumer) {
        KafkaRebalance toBeReplaced = client.inNamespace(kafkaRebalance.getMetadata().getNamespace()).withName(kafkaRebalance.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(KafkaRebalance kafkaRebalance) {
        List<String> readyConditions = Arrays.asList(
            KafkaRebalanceState.PendingProposal.toString(),
            KafkaRebalanceState.ProposalReady.toString(),
            KafkaRebalanceState.Ready.toString()
        );

        List<String> actualConditions = kafkaRebalance.getStatus().getConditions().stream().map(Condition::getType).toList();

        for (String condition: actualConditions) {
            if (readyConditions.contains(condition)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isDeleted(KafkaRebalance kafkaRebalance) {
        return kafkaRebalance == null;
    }
}