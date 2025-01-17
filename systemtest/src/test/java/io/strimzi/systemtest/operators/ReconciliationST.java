/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(REGRESSION)
public class ReconciliationST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ReconciliationST.class);

    private static final Map<String, String> PAUSE_ANNO = Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true");
    private static final int SCALE_TO = 4;

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings("deprecation") // Replicas in Kafka CR are deprecated, but some API methods are still called here
    void testPauseReconciliationInKafkaAndKafkaConnectWithConnector() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        LOGGER.info("Adding pause annotation into Kafka resource and also scaling replicas to 4, new Pod should not appear");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            Map<String, String> annotations = kafka.getMetadata().getAnnotations();
            annotations.putAll(PAUSE_ANNO);

            kafka.getMetadata().setAnnotations(annotations);
        });

        LOGGER.info("Kafka should contain status with {}", CustomResourceStatus.ReconciliationPaused.toString());
        KafkaUtils.waitForKafkaStatus(testStorage.getNamespaceName(), testStorage.getClusterName(), CustomResourceStatus.ReconciliationPaused);
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp -> knp.getSpec().setReplicas(SCALE_TO));

        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(), 3);

        LOGGER.info("Setting annotation to \"false\", Kafka should be scaled to {}", SCALE_TO);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> kafka.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"));
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), SCALE_TO);

        LOGGER.info("Deploying KafkaConnect with pause annotation from the start, no Pods should appear");
        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editOrNewMetadata()
                .addToAnnotations(PAUSE_ANNO)
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        final String connectDepName = KafkaConnectResources.componentName(testStorage.getClusterName());

        KafkaConnectUtils.waitForConnectStatus(testStorage.getNamespaceName(), testStorage.getClusterName(), CustomResourceStatus.ReconciliationPaused);
        PodUtils.waitUntilPodStabilityReplicasCount(testStorage.getNamespaceName(), connectDepName, 0);

        LOGGER.info("Setting annotation to \"false\" and creating KafkaConnector");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            kc -> kc.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"));
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 1);

        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), testStorage.getClusterName()).build());

        String connectPodName = kubeClient(testStorage.getNamespaceName()).listPods(testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String connectorSpec = KafkaConnectorUtils.getConnectorSpecFromConnectAPI(testStorage.getNamespaceName(), connectPodName, testStorage.getClusterName());

        LOGGER.info("Adding pause annotation into the KafkaConnector and scaling taskMax to 4");
        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), connector -> {
            connector.getMetadata().setAnnotations(PAUSE_ANNO);
            connector.getSpec().setTasksMax(SCALE_TO);
        });

        KafkaConnectorUtils.waitForConnectorStatus(testStorage.getNamespaceName(), testStorage.getClusterName(), CustomResourceStatus.ReconciliationPaused);
        KafkaConnectorUtils.waitForConnectorSpecFromConnectAPIStability(testStorage.getNamespaceName(), connectPodName, testStorage.getClusterName(), connectorSpec);

        LOGGER.info("Setting annotation to \"false\", taskMax should be increased to {}", SCALE_TO);
        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), connector ->
            connector.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"));

        String oldConfig = new JsonObject(connectorSpec).getValue("config").toString();
        KafkaConnectorUtils.waitForConnectorConfigUpdate(testStorage.getNamespaceName(), connectPodName, testStorage.getClusterName(), oldConfig, "localhost");
        KafkaConnectorUtils.waitForConnectorsTaskMaxChangeViaAPI(testStorage.getNamespaceName(), connectPodName, testStorage.getClusterName(), SCALE_TO);
    }

    @ParallelNamespaceTest
    @Tag(CRUISE_CONTROL)
    void testPauseReconciliationInKafkaRebalanceAndTopic() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build());

        // to prevent race condition when reconciliation is paused before KafkaTopic is actually created in Kafka
        KafkaTopicUtils.waitForTopicWillBePresentInKafka(testStorage.getNamespaceName(), testStorage.getTopicName(), KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), scraperPodName);

        LOGGER.info("Adding pause annotation into KafkaTopic resource and changing replication factor");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> {
            topic.getMetadata().setAnnotations(PAUSE_ANNO);
            topic.getSpec().setPartitions(SCALE_TO);
        });

        KafkaTopicUtils.waitForKafkaTopicStatus(testStorage.getNamespaceName(), testStorage.getTopicName(), CustomResourceStatus.ReconciliationPaused);
        KafkaTopicUtils.waitForKafkaTopicSpecStability(testStorage.getNamespaceName(), testStorage.getTopicName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));

        LOGGER.info("Setting annotation to \"false\", partitions should be scaled to {}", SCALE_TO);
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false")
        );
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(testStorage.getNamespaceName(), testStorage.getTopicName(), SCALE_TO);

        resourceManager.createResourceWithWait(KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build());

        LOGGER.info("Waiting for {}, then add pause and rebalance annotation, rebalancing should not be triggered", KafkaRebalanceState.ProposalReady);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        KafkaRebalanceResource.replaceKafkaRebalanceResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), rebalance -> rebalance.getMetadata().setAnnotations(PAUSE_ANNO));

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ReconciliationPaused);

        KafkaRebalanceUtils.annotateKafkaRebalanceResource(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.approve);

        // unfortunately we don't have any option to check, if something is changed when reconciliations are paused
        // so we will check stability of status
        KafkaRebalanceUtils.waitForRebalanceStatusStability(testStorage.getNamespaceName(), testStorage.getClusterName());

        LOGGER.info("Setting annotation to \"false\" and waiting for KafkaRebalance to be in {} state", KafkaRebalanceState.Ready);
        KafkaRebalanceResource.replaceKafkaRebalanceResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            rebalance -> rebalance.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"));

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);

        // because approve annotation wasn't reflected, approving again
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceAnnotation.approve);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.Ready);
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
