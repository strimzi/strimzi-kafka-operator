/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.platform.commons.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaResource implements ResourceType<Kafka> {

    private static final Logger LOGGER = LogManager.getLogger(KafkaResource.class);
    private static final Predicate<Kafka> HAS_CRUISE_CONTROL_SUPPORT = resource -> resource.getSpec().getCruiseControl() != null;

    public KafkaResource() {}

    @Override
    public String getKind() {
        return Kafka.RESOURCE_KIND;
    }
    @Override
    public Kafka get(String namespace, String name) {
        return kafkaClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(Kafka resource) {
        kafkaClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).createOrReplace();
    }

    @Override
    public void delete(Kafka resource) {
        final String namespaceName = resource.getMetadata().getNamespace();
        final String clusterName = resource.getMetadata().getName();

        Preconditions.notNull(namespaceName, "Kafka namespace name is null!");
        Preconditions.notNull(clusterName, "Kafka cluster name is null!");

        // imporant: contract that if one wants to delete Kafka cluster with `cruise control` it also trigger
        // deletion of all KafkaTopics
        if (HAS_CRUISE_CONTROL_SUPPORT.test(resource)) {
            LOGGER.info("Explicit deletion of KafkaTopics in namespace {}, for cruise control Kafka cluster {}", namespaceName, clusterName);
            KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).list()
                .getItems().stream()
                .parallel()
                .filter(kt -> kt.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL).equals(clusterName))
                .map(kt -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(kt.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete())
                .map(kt -> kt.equals(Boolean.TRUE))
                // check that all topic was successfully deleted
                .allMatch(result -> true);
        }

        kafkaClient().inNamespace(namespaceName).withName(
            resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

        // additional deletion of pvcs with specification deleteClaim set to false which were not deleted prior this method
        List<PersistentVolumeClaim> persistentVolumeClaimsList = kubeClient().listPersistentVolumeClaims(namespaceName, clusterName);

        for (PersistentVolumeClaim persistentVolumeClaim : persistentVolumeClaimsList) {
            kubeClient().deletePersistentVolumeClaim(namespaceName, persistentVolumeClaim.getMetadata().getName());
        }

        for (PersistentVolumeClaim persistentVolumeClaim : persistentVolumeClaimsList) {
            PersistentVolumeClaimUtils.waitForPersistentVolumeClaimDeletion(namespaceName, persistentVolumeClaim.getMetadata().getName());
        }
    }

    @Override
    public boolean waitForReadiness(Kafka resource) {
        long timeout = ResourceOperation.getTimeoutForResourceReadiness(resource.getKind());

        // Kafka Exporter is not setup every time
        if (resource.getSpec().getKafkaExporter() != null) {
            timeout += ResourceOperation.getTimeoutForResourceReadiness(Constants.KAFKA_EXPORTER_DEPLOYMENT);
        }
        // Cruise Control is not setup every time
        if (resource.getSpec().getCruiseControl() != null) {
            timeout += ResourceOperation.getTimeoutForResourceReadiness(Constants.KAFKA_CRUISE_CONTROL_DEPLOYMENT);
        }
        return ResourceManager.waitForResourceStatus(kafkaClient(), resource, Ready, timeout);
    }

    public static MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaClient() {
        return Crds.kafkaOperation(ResourceManager.kubeClient().getClient());
    }

    public static void replaceKafkaResourceInSpecificNamespace(String resourceName, Consumer<Kafka> editor, String namespaceName) {
        ResourceManager.replaceCrdResource(Kafka.class, KafkaList.class, resourceName, editor, namespaceName);
    }

    public static KafkaStatus getKafkaStatus(String clusterName, String namespace) {
        return kafkaClient().inNamespace(namespace).withName(clusterName).get().getStatus();
    }

    public static LabelSelector getLabelSelector(String clusterName, String componentName) {
        Map<String, String> matchLabels = new HashMap<>();
        matchLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        matchLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        matchLabels.put(Labels.STRIMZI_NAME_LABEL, componentName);

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }
}
