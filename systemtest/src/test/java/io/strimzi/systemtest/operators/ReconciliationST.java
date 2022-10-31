/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.Map;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
@ParallelSuite
public class ReconciliationST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ReconciliationST.class);

    private static final Map<String, String> PAUSE_ANNO = Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true");
    private static final int SCALE_TO = 4;

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @KRaftNotSupported("Probably bug - https://github.com/strimzi/strimzi-kafka-operator/issues/6862")
    void testPauseReconciliationInKafkaAndKafkaConnectWithConnector(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaSsName = KafkaResources.kafkaStatefulSetName(clusterName);

        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, kafkaSsName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3).build());

        LOGGER.info("Adding pause annotation into Kafka resource and also scaling replicas to 4, new pod should not appear");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getMetadata().setAnnotations(PAUSE_ANNO);
            kafka.getSpec().getKafka().setReplicas(SCALE_TO);
        }, namespaceName);

        LOGGER.info("Kafka should contain status with {}", CustomResourceStatus.ReconciliationPaused.toString());
        KafkaUtils.waitForKafkaStatus(namespaceName, clusterName, CustomResourceStatus.ReconciliationPaused);
        PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, kafkaSsName, 3);

        LOGGER.info("Setting annotation to \"false\", Kafka should be scaled to {}", SCALE_TO);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> kafka.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"), namespaceName);
        RollingUpdateUtils.waitForComponentAndPodsReady(namespaceName, kafkaSelector, SCALE_TO);

        LOGGER.info("Deploying KafkaConnect with pause annotation from the start, no pods should appear");
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnectWithFilePlugin(clusterName, namespaceName, 1)
            .editOrNewMetadata()
                .addToAnnotations(PAUSE_ANNO)
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        String connectDepName = KafkaConnectResources.deploymentName(clusterName);

        KafkaConnectUtils.waitForConnectStatus(namespaceName, clusterName, CustomResourceStatus.ReconciliationPaused);
        PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, connectDepName, 0);

        LOGGER.info("Setting annotation to \"false\" and creating KafkaConnector");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(clusterName,
            kc -> kc.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"), namespaceName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, connectDepName, 1);

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName).build());

        String connectPodName = kubeClient(namespaceName).listPods(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String connectorSpec = KafkaConnectorUtils.getConnectorSpecFromConnectAPI(namespaceName, connectPodName, clusterName);

        LOGGER.info("Adding pause annotation into the KafkaConnector and scaling taskMax to 4");
        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(clusterName, connector -> {
            connector.getMetadata().setAnnotations(PAUSE_ANNO);
            connector.getSpec().setTasksMax(SCALE_TO);
        }, namespaceName);

        KafkaConnectorUtils.waitForConnectorStatus(namespaceName, clusterName, CustomResourceStatus.ReconciliationPaused);
        KafkaConnectorUtils.waitForConnectorSpecFromConnectAPIStability(namespaceName, connectPodName, clusterName, connectorSpec);

        LOGGER.info("Setting annotation to \"false\", taskMax should be increased to {}", SCALE_TO);
        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(clusterName, connector ->
            connector.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"), namespaceName);

        String oldConfig = new JsonObject(connectorSpec).getValue("config").toString();
        JsonObject newConfig = new JsonObject(KafkaConnectorUtils.waitForConnectorConfigUpdate(namespaceName, connectPodName, clusterName, oldConfig, "localhost"));

        KafkaConnectorUtils.waitForConnectorsTaskMaxChangeViaAPI(namespaceName, connectPodName, clusterName, SCALE_TO);
    }

    @ParallelNamespaceTest
    @Tag(CRUISE_CONTROL)
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testPauseReconciliationInKafkaRebalanceAndTopic(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String scraperName = mapWithScraperNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 1).build(),
            ScraperTemplates.scraperPod(namespaceName, scraperName).build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(namespaceName, scraperName).get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        // to prevent race condition when reconciliation is paused before KafkaTopic is actually created in Kafka
        KafkaTopicUtils.waitForTopicWillBePresentInKafka(namespaceName, topicName, KafkaResources.plainBootstrapAddress(clusterName), scraperPodName);

        LOGGER.info("Adding pause annotation into KafkaTopic resource and changing replication factor");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, topic -> {
            topic.getMetadata().setAnnotations(PAUSE_ANNO);
            topic.getSpec().setPartitions(SCALE_TO);
        }, namespaceName);

        KafkaTopicUtils.waitForKafkaTopicStatus(namespaceName, topicName, CustomResourceStatus.ReconciliationPaused);
        KafkaTopicUtils.waitForKafkaTopicSpecStability(namespaceName, topicName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName));

        LOGGER.info("Setting annotation to \"false\", partitions should be scaled to {}", SCALE_TO);
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName,
            topic -> topic.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"),
            namespaceName);
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(namespaceName, topicName, SCALE_TO);

        resourceManager.createResource(extensionContext, KafkaRebalanceTemplates.kafkaRebalance(clusterName).build());

        LOGGER.info("Waiting for {}, then add pause and rebalance annotation, rebalancing should not be triggered", KafkaRebalanceState.ProposalReady);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(namespaceName, clusterName, KafkaRebalanceState.ProposalReady);

        KafkaRebalanceResource.replaceKafkaRebalanceResourceInSpecificNamespace(clusterName, rebalance -> rebalance.getMetadata().setAnnotations(PAUSE_ANNO), namespaceName);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(namespaceName, clusterName, KafkaRebalanceState.ReconciliationPaused);

        KafkaRebalanceUtils.annotateKafkaRebalanceResource(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, namespaceName, clusterName), namespaceName, clusterName, KafkaRebalanceAnnotation.approve);

        // unfortunately we don't have any option to check, if something is changed when reconciliations are paused
        // so we will check stability of status
        KafkaRebalanceUtils.waitForRebalanceStatusStability(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, namespaceName, clusterName), namespaceName, clusterName);

        LOGGER.info("Setting annotation to \"false\" and waiting for KafkaRebalance to be in {} state", KafkaRebalanceState.Ready);
        KafkaRebalanceResource.replaceKafkaRebalanceResourceInSpecificNamespace(clusterName,
            rebalance -> rebalance.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"), namespaceName);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(namespaceName, clusterName, KafkaRebalanceState.ProposalReady);

        // because approve annotation wasn't reflected, approving again
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, namespaceName, clusterName), namespaceName, clusterName, KafkaRebalanceAnnotation.approve);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(namespaceName, clusterName, KafkaRebalanceState.Ready);
    }
}
