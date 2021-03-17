/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.Map;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
public class ReconciliationST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ReconciliationST.class);
    private static final String NAMESPACE = "reconciliation-cluster-test";

    private static final Map<String, String> PAUSE_ANNO = Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true");
    private static final int SCALE_TO = 4;

    @IsolatedTest
    void testPauseReconciliationInKafkaAndKafkaConnectWithConnector(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3).build());

        String kafkaSsName = KafkaResources.kafkaStatefulSetName(clusterName);

        LOGGER.info("Adding pause annotation into Kafka resource and also scaling replicas to 4, new pod should not appear");
        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            kafka.getMetadata().setAnnotations(PAUSE_ANNO);
            kafka.getSpec().getKafka().setReplicas(SCALE_TO);
        });

        LOGGER.info("Kafka should contain status with {}", CustomResourceStatus.ReconciliationPaused.toString());
        KafkaUtils.waitForKafkaStatus(clusterName, CustomResourceStatus.ReconciliationPaused);
        PodUtils.waitUntilPodStabilityReplicasCount(kafkaSsName, 3);

        LOGGER.info("Setting annotation to \"false\", Kafka should be scaled to {}", SCALE_TO);
        KafkaResource.replaceKafkaResource(clusterName, kafka -> kafka.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"));
        StatefulSetUtils.waitForAllStatefulSetPodsReady(kafkaSsName, SCALE_TO);

        LOGGER.info("Deploying KafkaConnect with pause annotation from the start, no pods should appear");
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editOrNewMetadata()
                .addToAnnotations(PAUSE_ANNO)
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        String connectDepName = KafkaConnectResources.deploymentName(clusterName);

        KafkaConnectUtils.waitForConnectStatus(clusterName, CustomResourceStatus.ReconciliationPaused);
        PodUtils.waitUntilPodStabilityReplicasCount(connectDepName, 0);

        LOGGER.info("Setting annotation to \"false\" and creating KafkaConnector");
        KafkaConnectResource.replaceKafkaConnectResource(clusterName,
            kc -> kc.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"));
        DeploymentUtils.waitForDeploymentAndPodsReady(connectDepName, 1);

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName).build());

        String connectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String connectorSpec = KafkaConnectorUtils.getConnectorSpecFromConnectAPI(connectPodName, clusterName);

        LOGGER.info("Adding pause annotation into the KafkaConnector and scaling taskMax to 4");
        KafkaConnectorResource.replaceKafkaConnectorResource(clusterName, connector -> {
            connector.getMetadata().setAnnotations(PAUSE_ANNO);
            connector.getSpec().setTasksMax(SCALE_TO);
        });

        KafkaConnectorUtils.waitForConnectorStatus(clusterName, CustomResourceStatus.ReconciliationPaused);
        KafkaConnectorUtils.waitForConnectorSpecFromConnectAPIStability(connectPodName, clusterName, connectorSpec);

        LOGGER.info("Setting annotation to \"false\", taskMax should be increased to {}", SCALE_TO);
        KafkaConnectorResource.replaceKafkaConnectorResource(clusterName, connector ->
            connector.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"));

        String oldConfig = new JsonObject(connectorSpec).getValue("config").toString();
        JsonObject newConfig = new JsonObject(KafkaConnectorUtils.waitForConnectorConfigUpdate(connectPodName, clusterName, oldConfig, "localhost"));

        assertThat(newConfig.getValue("tasks.max"), is(Integer.toString(SCALE_TO)));
    }

    @IsolatedTest
    void testPauseReconciliationInKafkaRebalanceAndTopic(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, TOPIC_NAME).build());

        /* TODO: after issue with TO and pausing reconciliations in KafkaTopic will be fixed, uncomment these lines
        LOGGER.info("Adding pause annotation into KafkaTopic resource and changing replication factor");
        KafkaTopicResource.replaceTopicResource(TOPIC_NAME, topic -> {
            topic.getMetadata().setAnnotations(PAUSE_ANNO);
            topic.getSpec().setPartitions(SCALE_TO);
        });

        KafkaTopicUtils.waitForKafkaTopicStatus(TOPIC_NAME, CustomResourceStatus.ReconciliationPaused);
        KafkaTopicUtils.waitForKafkaTopicSpecStability(TOPIC_NAME, KafkaResources.kafkaPodName(clusterName, 0), KafkaResources.plainBootstrapAddress(clusterName));

        LOGGER.info("Setting annotation to \"false\", partitions should be scaled to {}", SCALE_TO);
        KafkaTopicResource.replaceTopicResource(TOPIC_NAME,
            topic -> topic.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"));
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(TOPIC_NAME, SCALE_TO);
        */

        resourceManager.createResource(extensionContext, KafkaRebalanceTemplates.kafkaRebalance(clusterName).build());

        LOGGER.info("Waiting for {}, then add pause and rebalance annotation, rebalancing should not be triggered", KafkaRebalanceState.ProposalReady);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(clusterName, KafkaRebalanceState.ProposalReady);

        KafkaRebalanceResource.replaceKafkaRebalanceResource(clusterName, rebalance -> rebalance.getMetadata().setAnnotations(PAUSE_ANNO));

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(clusterName, KafkaRebalanceState.ReconciliationPaused);

        KafkaRebalanceUtils.annotateKafkaRebalanceResource(clusterName, KafkaRebalanceAnnotation.approve);

        // unfortunately we don't have any option to check, if something is changed when reconciliations are paused
        // so we will check stability of status
        KafkaRebalanceUtils.waitForRebalanceStatusStability(clusterName);

        LOGGER.info("Setting annotation to \"false\" and waiting for KafkaRebalance to be in {} state", KafkaRebalanceState.Ready);
        KafkaRebalanceResource.replaceKafkaRebalanceResource(clusterName,
            rebalance -> rebalance.getMetadata().getAnnotations().replace(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true", "false"));

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(clusterName, KafkaRebalanceState.ProposalReady);

        // because approve annotation wasn't reflected, approving again
        KafkaRebalanceUtils.annotateKafkaRebalanceResource(clusterName, KafkaRebalanceAnnotation.approve);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(clusterName, KafkaRebalanceState.Ready);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);
    }
}
