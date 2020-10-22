/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.resources.ResourceManager;
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
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class OlmUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(OlmUpgradeST.class);

    private final String namespace = "olm-upgrade-namespace";
    private final String producerName = "producer";
    private final String consumerName = "consumer";
    private final String topicUpgradeName = "topic-upgrade";
    private final int messageUpgradeCount =  10_000;

    @Test
    void testUpgrade() {
        // 1. Create subscription (+ operator group) with version latest - 1 (manual approval strategy) already done...!
        // 2. Approve installation
        //   a) get name of install-plan
        //   b) approve installation

        OlmResource.clusterOperator(namespace, OlmInstallationStrategy.Manual, false);

        // we need firstly invoke `obtainInstallPlanName` to use `isUpgradeable`
        OlmResource.obtainInstallPlanName();

        while (OlmResource.isUpgradeable()) {
            LOGGER.info("====================================================================================");
            LOGGER.info("============== Verification version of CO:" + OlmResource.getClusterOperatorVersion());
            LOGGER.info("====================================================================================");

            // 3.perform verification of specific version
            performUpgradeVerification();

            Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

            // 4. Upgrade CO
            OlmResource.upgradeClusterOperator();

            // 5. wait until RU is finished
            StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        }
    }

    private void performUpgradeVerification() {
        // fetch the tag from imageName: docker.io/strimzi/operator:'[latest|0.19.0|0.18.0]'
        String imageTag = kubeClient().getDeployment(kubeClient().getDeploymentNameByPrefix(Constants.STRIMZI_DEPLOYMENT_NAME))
            .getSpec()
            .getTemplate()
            .getMetadata()
            .getAnnotations()
            .get("containerImage").split(":")[1];

        // NOT (latest image or default substring(1)) for skipping 'v'0.19.0 on the start...
        if (!(imageTag.equals("latest") || imageTag.equals(Environment.OLM_OPERATOR_VERSION_DEFAULT.substring(1)))) {
            try {
                File dir = FileUtils.downloadAndUnzip("https://github.com/strimzi/strimzi-kafka-operator/releases/download/" + imageTag + "/strimzi-" + imageTag + ".zip");

                deployKafkaFromFile(dir, imageTag);
                waitForReadinessOfKafkaCluster();

                KafkaTopicResource.topic(CLUSTER_NAME, topicUpgradeName).done();
            } catch (IOException e) {
                e.printStackTrace();
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
