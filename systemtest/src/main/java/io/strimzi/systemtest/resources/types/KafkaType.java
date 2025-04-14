/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.types;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.ResourceType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.platform.commons.util.Preconditions;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class KafkaType implements ResourceType<Kafka> {

    private MixedOperation<Kafka, KafkaList, Resource<Kafka>> client;
    private static final Logger LOGGER = LogManager.getLogger(KafkaType.class);
    private static final Predicate<Kafka> HAS_CRUISE_CONTROL_SUPPORT = resource -> resource.getSpec().getCruiseControl() != null;

    public KafkaType() {
        client = Crds.kafkaOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaClient() {
        return Crds.kafkaOperation(KubeResourceManager.get().kubeClient().getClient());
    }

    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    @Override
    public String getKind() {
        return Kafka.RESOURCE_KIND;
    }

    @Override
    public void create(Kafka kafka) {
        client.inNamespace(kafka.getMetadata().getNamespace()).resource(kafka).create();
    }

    @Override
    public void update(Kafka kafka) {
        client.inNamespace(kafka.getMetadata().getNamespace()).resource(kafka).update();
    }

    @Override
    public void delete(Kafka kafka) {
        final String namespaceName = kafka.getMetadata().getNamespace();
        final String clusterName = kafka.getMetadata().getName();

        Preconditions.notNull(namespaceName, "Kafka Namespace name is null!");
        Preconditions.notNull(clusterName, "Kafka cluster name is null!");

        // important: contract that if one wants to delete Kafka cluster with CruiseControl it also trigger
        // deletion of all KafkaTopics
        if (HAS_CRUISE_CONTROL_SUPPORT.test(kafka)) {
            LOGGER.info("Explicit deletion of KafkaTopics in Namespace: {}, for CruiseControl Kafka cluster {}", namespaceName, clusterName);
            CrdClients.kafkaTopicClient().inNamespace(namespaceName).list()
                .getItems().stream()
                .parallel()
                .filter(kt -> kt.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL).equals(clusterName))
                .map(kt -> CrdClients.kafkaTopicClient().inNamespace(namespaceName).withName(kt.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete())
                // check that all topic was successfully deleted
                .allMatch(result -> true);
        }

        // get current Kafka
        Kafka currentKafka = client.inNamespace(namespaceName)
            .withName(kafka.getMetadata().getName()).get();

        // proceed only if kafka is still present as Kafka is purposefully deleted in some test cases
        if (currentKafka != null) {
            // load current Kafka's annotations to obtain information, if KafkaNodePools are used for this Kafka
            Map<String, String> annotations = currentKafka.getMetadata().getAnnotations();

            client.inNamespace(namespaceName).withName(
                kafka.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

            if (annotations.get(Annotations.ANNO_STRIMZI_IO_NODE_POOLS) == null
                || annotations.get(Annotations.ANNO_STRIMZI_IO_NODE_POOLS).equals("disabled")) {
                // additional deletion of pvcs with specification deleteClaim set to false which were not deleted prior this method
                PersistentVolumeClaimUtils.deletePvcsByPrefixWithWait(namespaceName, clusterName);
            }
        }
    }

    @Override
    public void replace(Kafka kafka, Consumer<Kafka> consumer) {
        Kafka toBeReplaced = client.inNamespace(kafka.getMetadata().getNamespace()).withName(kafka.getMetadata().getName()).get();
        consumer.accept(toBeReplaced);
        update(toBeReplaced);
    }

    @Override
    public boolean isReady(Kafka kafka) {
        KafkaStatus kafkaStatus = client.inNamespace(kafka.getMetadata().getNamespace()).resource(kafka).get().getStatus();
        Optional<Condition> readyCondition = kafkaStatus.getConditions().stream().filter(condition -> condition.getType().equals("Ready")).findFirst();

        return readyCondition.map(condition -> condition.getStatus().equals("True")).orElse(false);
    }

    @Override
    public boolean isDeleted(Kafka kafka) {
        return kafka == null;
    }
}