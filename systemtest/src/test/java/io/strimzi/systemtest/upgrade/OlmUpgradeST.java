/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.operator.OlmResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class OlmUpgradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(OlmUpgradeST.class);

    private final String namespace = "olm-upgrade-namespace";
    private final String producerName = "producer";
    private final String consumerName = "consumer";
    private final String topicUpgradeName = "topic-upgrade";
    private final int messageUpgradeCount =  10_000;
    private final Map<String, List<String>> mapOfKafkaVersionsWithSupportedClusterOperators = getMapKafkaVersionsWithSupportedClusterOperatorVersions();

    @Test
    void testUpgrade() {
        Map<String, String> kafkaSnapshot = null;
        boolean isUpgradeAble = true;

        while (isUpgradeAble) {
            // 1. Create subscription (+ operator group) with version latest - 1 (manual approval strategy) already done...!
            // 2. Approve installation
            //   a) get name of install-plan
            //   b) approve installation
            OlmResource.upgradeAbleClusterOperator(namespace, OlmInstallationStrategy.Manual, false);

            String currentVersionOfCo = OlmResource.getClusterOperatorVersion();

            LOGGER.info("====================================================================================");
            LOGGER.info("============== Verification version of CO:" + currentVersionOfCo);
            LOGGER.info("====================================================================================");

            // wait until RU is finished (first run skipping)
            if (kafkaSnapshot != null) {
                StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
            }

            // 3. perform verification of specific version
            performUpgradeVerification();
            kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

            OlmResource.getClosedMapInstallPlan().put(OlmResource.getNonUsedInstallPlan(), Boolean.TRUE);
            OlmResource.obtainInstallPlanName();
            isUpgradeAble = OlmResource.isUpgradeable();
        }
    }

    private void performUpgradeVerification() {
        // fetch the tag from imageName: docker.io/strimzi/operator:'[latest|0.19.0|0.18.0]'
        String containerImageTag = kubeClient().getDeployment(kubeClient().getDeploymentNameByPrefix(Constants.STRIMZI_DEPLOYMENT_NAME))
            .getSpec()
            .getTemplate()
            .getMetadata()
            .getAnnotations()
            .get("containerImage").split(":")[1];

        LOGGER.info("Image tag of strimzi operator is {}", containerImageTag);

        // NOT (latest image or default substring(1)) for skipping 'v'0.19.0 on the start...
        // '6.6.6' is the latest version of cluster operator
        if (!containerImageTag.equals("6.6.6") && (!(containerImageTag.equals("latest") || containerImageTag.equals(Environment.OLM_OPERATOR_VERSION_DEFAULT.substring(1))))) {
            try {
                File dir = FileUtils.downloadAndUnzip("https://github.com/strimzi/strimzi-kafka-operator/releases/download/" + containerImageTag + "/strimzi-" + containerImageTag + ".zip");

                deployKafkaFromFile(dir, containerImageTag);
                waitForReadinessOfKafkaCluster();

                KafkaTopicResource.topic(CLUSTER_NAME, topicUpgradeName).done();
            } catch (IOException e) {
                e.printStackTrace();
            }
        //  this is round only last version (so kafka is not present)
        } else if (KafkaResource.kafkaClient().inNamespace(namespace).withName(CLUSTER_NAME).get() == null) {
            KafkaResource.kafkaPersistent(CLUSTER_NAME, 3).done();
        }

        String currentKafkaVersion = KafkaResource.kafkaClient().inNamespace(namespace).withName(CLUSTER_NAME).get().getSpec().getKafka().getVersion();

        LOGGER.info("Current Kafka message version is {}", currentKafkaVersion);

        if (mapOfKafkaVersionsWithSupportedClusterOperators.containsKey(currentKafkaVersion)) {
            // supported co version for specific kafka version
            List<String> supportedClusterOperatorVersion = mapOfKafkaVersionsWithSupportedClusterOperators.get(currentKafkaVersion);

            // exist version of cluster operator in list of supported
            if (supportedClusterOperatorVersion.contains(containerImageTag)) {
                LOGGER.info("Current Kafka Version {} supports Cluster operator version {}. So we are not gonna upgrade Kafka", currentKafkaVersion, containerImageTag);
            } else {
                LOGGER.warn("Current Kafka Version {} does not supports Cluster operator version {}. So we are gonna upgrade Kafka", currentKafkaVersion, containerImageTag);

                // sort keys and pick 'next version'
                SortedSet<String> sortedKeys = new TreeSet<>(mapOfKafkaVersionsWithSupportedClusterOperators.keySet());
                Iterator<String> kafkaVersions = sortedKeys.iterator();
                String[] newKafkaVersion = new String[1];

                while (kafkaVersions.hasNext()) {
                    String kafkaVersion = kafkaVersions.next();
                    if (kafkaVersion.equals(currentKafkaVersion)) {
                        LOGGER.info("This is current version {} but we need next one!", kafkaVersion);
                        if (kafkaVersions.hasNext()) {
                            newKafkaVersion[0] = kafkaVersions.next();
                            LOGGER.info("New Kafka version is {} and we are gonna update Kafka custom resource.", newKafkaVersion[0]);
                        }
                    }
                }

                if (newKafkaVersion[0] == null || newKafkaVersion[0].isEmpty()) {
                    throw new RuntimeException("There is not new Kafka version! Latest is:" + currentKafkaVersion);
                }

                Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

                // we are gonna use latest Kafka
                if (containerImageTag.equals("6.6.6")) {
                    newKafkaVersion[0] = sortedKeys.last();
                }

                KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
                    //  2.2.1 -> 2.2 (gonna trim from kafka version)
                    String logMessageFormatVersion = newKafkaVersion[0].substring(0, 2);
                    LOGGER.info("We are gonna update Kafka CR with following versions:\n" +
                        "Kafka version: {}\n" +
                        "Log message format version: {}", newKafkaVersion[0], logMessageFormatVersion);
                    kafka.getSpec().getKafka().getConfig().put("log.message.format.version", logMessageFormatVersion);
                    kafka.getSpec().getKafka().setVersion(newKafkaVersion[0]);
                });

                // wait until RU
                StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
            }
        }

        KafkaBasicExampleClients kafkaBasicClientJob = new KafkaBridgeExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
            .withTopicName(topicUpgradeName)
            .withMessageCount(messageUpgradeCount)
            .withDelayMs(1)
            .build();

        kafkaBasicClientJob.producerStrimzi().done();
        kafkaBasicClientJob.consumerStrimzi().done();

        ClientUtils.waitForClientSuccess(producerName, namespace, messageUpgradeCount);
        ClientUtils.waitForClientSuccess(consumerName, namespace, messageUpgradeCount);

        // Delete jobs to make same names available for next upgrade
        kubeClient().deleteJob(producerName);
        kubeClient().deleteJob(consumerName);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }


    private void deployKafkaFromFile(File dir, String imageTag) {
        File kafkaYaml = new File(dir, "strimzi-" + imageTag + "/examples/kafka/kafka-persistent.yaml");
        LOGGER.info("Going to deploy Kafka from: {}", kafkaYaml.getPath());
        cmdKubeClient().create(kafkaYaml);
    }

    private void waitForReadinessOfKafkaCluster() {
        LOGGER.info("Waiting for Zookeeper StatefulSet");
        StatefulSetUtils.waitForAllStatefulSetPodsReady(CLUSTER_NAME + "-zookeeper", 3);
        LOGGER.info("Waiting for Kafka StatefulSet");
        StatefulSetUtils.waitForAllStatefulSetPodsReady(CLUSTER_NAME + "-kafka", 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(CLUSTER_NAME + "-entity-operator", 1);
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();

        cluster.setNamespace(namespace);
        cluster.createNamespace(namespace);
    }
}
