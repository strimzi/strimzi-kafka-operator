/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.KafkaTopicSpec;
import io.strimzi.api.kafka.model.status.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.model.balancing.KafkaRebalanceState;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
public class CruiseControlIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlIsolatedST.class);
    private static final String NAMESPACE = "cruise-control-isolated-test";

    private static final String CRUISE_CONTROL_METRICS_TOPIC = "strimzi.cruisecontrol.metrics"; // partitions 1 , rf - 1
    private static final String CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC = "strimzi.cruisecontrol.modeltrainingsamples"; // partitions 32 , rf - 2
    private static final String CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC = "strimzi.cruisecontrol.partitionmetricsamples"; // partitions 32 , rf - 2

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testAutoCreationOfCruiseControlTopics(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3)
            .editOrNewSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec()
            .build());

        KafkaTopicUtils.waitForKafkaTopicReady(CRUISE_CONTROL_METRICS_TOPIC);
        KafkaTopicSpec metricsTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE)
            .withName(CRUISE_CONTROL_METRICS_TOPIC).get().getSpec();

        KafkaTopicUtils.waitForKafkaTopicReady(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        KafkaTopicSpec modelTrainingTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE)
            .withName(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC).get().getSpec();

        KafkaTopicUtils.waitForKafkaTopicReady(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        KafkaTopicSpec partitionMetricsTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE)
            .withName(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC).get().getSpec();

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_METRICS_TOPIC);
        assertThat(metricsTopic.getPartitions(), is(1));
        assertThat(metricsTopic.getReplicas(), is(1));

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        assertThat(modelTrainingTopic.getPartitions(), is(32));
        assertThat(modelTrainingTopic.getReplicas(), is(2));

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        assertThat(partitionMetricsTopic.getPartitions(), is(32));
        assertThat(partitionMetricsTopic.getReplicas(), is(2));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(ACCEPTANCE)
    void testCruiseControlWithRebalanceResource(ExtensionContext extensionContext) {
        String clusterName = "what-about-this";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());
        resourceManager.createResource(extensionContext, KafkaRebalanceTemplates.kafkaRebalance(clusterName).build());

        KafkaRebalanceUtils.doRebalancingProcess(clusterName);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCruiseControlWithSingleNodeKafka(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        String errMessage =  "Kafka " + NAMESPACE + "/" + clusterName + " has invalid configuration." +
            " Cruise Control cannot be deployed with a single-node Kafka cluster. It requires " +
            "at least two Kafka nodes.";

        LOGGER.info("Deploying single node Kafka with CruiseControl");
        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaWithCruiseControl(clusterName, 1, 1).build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, NAMESPACE, errMessage, Duration.ofMinutes(6).toMillis());

        KafkaStatus kafkaStatus = KafkaTemplates.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus();

        assertThat(kafkaStatus.getConditions().get(0).getReason(), is("InvalidResourceException"));

        LOGGER.info("Increasing Kafka nodes to 3");
        KafkaResource.replaceKafkaResource(clusterName, kafka -> kafka.getSpec().getKafka().setReplicas(3));
        KafkaUtils.waitForKafkaReady(clusterName);

        kafkaStatus = KafkaTemplates.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus();
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is(not(errMessage)));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCruiseControlTopicExclusion(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        String excludedTopic1 = "excluded-topic-1";
        String excludedTopic2 = "excluded-topic-2";
        String includedTopic = "included-topic";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, excludedTopic1).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, excludedTopic2).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, includedTopic).build());

        resourceManager.createResource(extensionContext,  KafkaRebalanceTemplates.kafkaRebalance(clusterName)
            .editOrNewSpec()
                .withExcludedTopics("excluded-.*")
            .endSpec()
            .build());

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(clusterName, KafkaRebalanceState.ProposalReady);

        LOGGER.info("Checking status of KafkaRebalance");
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus();
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic1));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic2));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), not(containsString(includedTopic)));

        KafkaRebalanceUtils.annotateKafkaRebalanceResource(clusterName, KafkaRebalanceAnnotation.approve);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(clusterName, KafkaRebalanceState.Ready);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCruiseControlReplicaMovementStrategy(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        final String replicaMovementStrategies = "default.replica.movement.strategies";
        String newReplicaMovementStrategies = "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy," +
                "com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy," +
                "com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3).build());
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(kafkaClientsName).build());

        String ccPodName = kubeClient().listPodsByPrefixInName(CruiseControlResources.deploymentName(clusterName)).get(0).getMetadata().getName();

        LOGGER.info("Check for default CruiseControl replicaMovementStrategy in pod configuration file.");
        Map<String, Object> actualStrategies = KafkaTemplates.kafkaClient().inNamespace(NAMESPACE)
                .withName(clusterName).get().getSpec().getCruiseControl().getConfig();
        assertThat(actualStrategies, anEmptyMap());

        String ccConfFileContent = cmdKubeClient().execInPodContainer(ccPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME, "cat", Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, not(containsString(replicaMovementStrategies)));

        Map<String, String> kafkaRebalanceSnapshot = DeploymentUtils.depSnapshot(CruiseControlResources.deploymentName(clusterName));

        Map<String, Object> ccConfigMap = new HashMap<>();
        ccConfigMap.put(replicaMovementStrategies, newReplicaMovementStrategies);

        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            LOGGER.info("Set non-default CruiseControl replicaMovementStrategies to KafkaRebalance resource.");
            kafka.getSpec().getCruiseControl().setConfig(ccConfigMap);
        });

        LOGGER.info("Verifying that CC pod is rolling, because of change size of disk");
        DeploymentUtils.waitTillDepHasRolled(CruiseControlResources.deploymentName(clusterName), 1, kafkaRebalanceSnapshot);

        ccPodName = kubeClient().listPodsByPrefixInName(CruiseControlResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        ccConfFileContent = cmdKubeClient().execInPodContainer(ccPodName, Constants.CRUISE_CONTROL_CONTAINER_NAME, "cat", Constants.CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();
        assertThat(ccConfFileContent, containsString(newReplicaMovementStrategies));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testHostAliases(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        HostAlias hostAlias = new HostAliasBuilder()
            .withIp(aliasIp)
            .withHostnames(aliasHostname)
            .build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(clusterName, 3, 3)
            .editSpec()
                .editCruiseControl()
                    .withNewTemplate()
                        .withNewPod()
                            .withHostAliases(hostAlias)
                        .endPod()
                    .endTemplate()
                .endCruiseControl()
            .endSpec()
            .build());

        String ccPodName = kubeClient().listPodsByPrefixInName(CruiseControlResources.deploymentName(clusterName)).get(0).getMetadata().getName();

        LOGGER.info("Checking the /etc/hosts file");
        String output = cmdKubeClient().execInPod(ccPodName, "cat", "/etc/hosts").out();
        assertThat(output, containsString(etcHostsData));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);
    }
}
