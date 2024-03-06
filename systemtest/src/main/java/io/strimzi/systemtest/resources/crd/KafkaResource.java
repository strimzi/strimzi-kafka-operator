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
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.platform.commons.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;

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
        kafkaClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(Kafka resource) {
        final String namespaceName = resource.getMetadata().getNamespace();
        final String clusterName = resource.getMetadata().getName();

        Preconditions.notNull(namespaceName, "Kafka Namespace name is null!");
        Preconditions.notNull(clusterName, "Kafka cluster name is null!");

        // important: contract that if one wants to delete Kafka cluster with CruiseControl it also trigger
        // deletion of all KafkaTopics
        if (HAS_CRUISE_CONTROL_SUPPORT.test(resource)) {
            LOGGER.info("Explicit deletion of KafkaTopics in Namespace: {}, for CruiseControl Kafka cluster {}", namespaceName, clusterName);
            KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).list()
                .getItems().stream()
                .parallel()
                .filter(kt -> kt.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL).equals(clusterName))
                .map(kt -> KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(kt.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete())
                // check that all topic was successfully deleted
                .allMatch(result -> true);
        }

        // get current Kafka
        Kafka kafka = kafkaClient().inNamespace(namespaceName)
            .withName(resource.getMetadata().getName()).get();

        // proceed only if kafka is still present as Kafka is purposefully deleted in some test cases
        if (kafka != null) {
            // load current Kafka's annotations to obtain information, if KafkaNodePools are used for this Kafka
            Map<String, String> annotations = kafka.getMetadata().getAnnotations();

            kafkaClient().inNamespace(namespaceName).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

            if (annotations.get(Annotations.ANNO_STRIMZI_IO_NODE_POOLS) == null
                || annotations.get(Annotations.ANNO_STRIMZI_IO_NODE_POOLS).equals("disabled")) {
                // additional deletion of pvcs with specification deleteClaim set to false which were not deleted prior this method
                PersistentVolumeClaimUtils.deletePvcsByPrefixWithWait(namespaceName, clusterName);
            }
        }


    }

    @Override
    public void update(Kafka resource) {
        kafkaClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(Kafka resource) {
        long timeout = ResourceOperation.getTimeoutForResourceReadiness(resource.getKind());

        // KafkaExporter is not setup every time
        if (resource.getSpec().getKafkaExporter() != null) {
            timeout += ResourceOperation.getTimeoutForResourceReadiness(TestConstants.KAFKA_EXPORTER_DEPLOYMENT);
        }
        // CruiseControl is not setup every time
        if (resource.getSpec().getCruiseControl() != null) {
            timeout += ResourceOperation.getTimeoutForResourceReadiness(TestConstants.KAFKA_CRUISE_CONTROL_DEPLOYMENT);
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
        Map<String, String> matchLabels = getCommonKafkaMatchLabels(clusterName);

        if (Environment.isKafkaNodePoolsEnabled()
            && !componentName.contains("zookeeper")
            && !componentName.contains("entity-operator")
        ) {
            matchLabels.put(Labels.STRIMZI_CONTROLLER_NAME_LABEL, componentName);
        } else {
            matchLabels.put(Labels.STRIMZI_NAME_LABEL, componentName);
        }

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }

    public static LabelSelector getEntityOperatorLabelSelector(final String clusterName) {
        final Map<String, String> matchLabels = getCommonKafkaMatchLabels(clusterName);

        matchLabels.put(Labels.STRIMZI_COMPONENT_TYPE_LABEL, "entity-operator");

        return new LabelSelectorBuilder()
                .withMatchLabels(matchLabels)
                .build();
    }

    public static LabelSelector getCruiseControlLabelSelector(final String clusterName) {
        final Map<String, String> matchLabels = getCommonKafkaMatchLabels(clusterName);

        matchLabels.put(Labels.STRIMZI_COMPONENT_TYPE_LABEL, "cruise-control");

        return new LabelSelectorBuilder()
                .withMatchLabels(matchLabels)
                .build();
    }

    public static LabelSelector getLabelSelectorForAllKafkaPods(String clusterName) {
        Map<String, String> matchLabels = getCommonKafkaMatchLabels(clusterName);

        matchLabels.put(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(clusterName));

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }

    private static Map<String, String> getCommonKafkaMatchLabels(String clusterName) {
        Map<String, String> labels = new HashMap<>();

        labels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        labels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);

        return labels;
    }

    public static String getStrimziPodSetName(String clusterName, String nodePoolName) {
        if (Environment.isKafkaNodePoolsEnabled() && nodePoolName == null) {
            return String.join("-", clusterName, KafkaNodePoolResource.getBrokerPoolName(clusterName));
        } else if (Environment.isKafkaNodePoolsEnabled()) {
            return String.join("-", clusterName, nodePoolName);
        } else {
            return KafkaResources.kafkaComponentName(clusterName);
        }
    }

    public static String getKafkaPodName(String clusterName, String nodePoolName, int podNum) {
        if (Environment.isKafkaNodePoolsEnabled()) {
            return String.join("-",
                clusterName,
                Objects.requireNonNullElseGet(nodePoolName, () -> KafkaNodePoolResource.getBrokerPoolName(clusterName)),
                String.valueOf(podNum)
            );
        }

        return KafkaResources.kafkaPodName(clusterName, podNum);
    }

    public static int getPodNumFromPodName(String componentName, String podName) {
        return Integer.parseInt(podName.replace(componentName + "-", ""));
    }
}
