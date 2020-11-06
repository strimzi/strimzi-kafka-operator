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
import io.strimzi.systemtest.utils.specific.OlmUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.systemtest.Environment.OLM_LATEST_CONTAINER_IMAGE_TAG;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class OlmUpgradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(OlmUpgradeST.class);

    private final String namespace = "olm-upgrade-namespace";
    private final String producerName = "producer";
    private final String consumerName = "consumer";
    private final String topicUpgradeName = "topic-upgrade";
    private final int messageUpgradeCount =  10_000;
    private final Map<String, List<String>> mapOfKafkaVersionsWithSupportedClusterOperators = getMapKafkaVersionsWithSupportedClusterOperatorVersions();

    @ParameterizedTest(name = "testUpgradeStrimziVersion-{0}-{1}")
    @MethodSource("loadJsonUpgradeData")
    void testChainUpgrade(String fromVersion, String toVersion, JsonObject parameters) {

        int clusterOperatorVersion = Integer.parseInt(fromVersion.split("\\.")[1]);
        // only 0.|18|.0 and more is supported
        assumeTrue(clusterOperatorVersion >= 18);

        // 5. make snapshots
        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        // 6. wait until non-used install plan is present (sometimes install-plan did not append immediately and we need to wait for at least 10m)
        OlmUtils.waitUntilNonUsedInstallPlanIsPresent(fromVersion);

        // 7. upgrade cluster operator
        OlmResource.upgradeClusterOperator();

        // 8. wait until RU is finished (first run skipping)
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);

        // 9. verification that cluster operator has correct version (install-plan) - strimzi-cluster-operator.v[version]
        String afterUpgradeVersionOfCo = OlmResource.getClusterOperatorVersion();
        assertThat(afterUpgradeVersionOfCo, is(Environment.OLM_APP_BUNDLE_PREFIX + ".v" + toVersion));

        // 10. perform verification of to version
        performUpgradeVerification(afterUpgradeVersionOfCo);

        // 11. save install-plan to closed-map
        OlmResource.getClosedMapInstallPlan().put(OlmResource.getNonUsedInstallPlan(), Boolean.TRUE);
    }

    private void performUpgradeVerification(String version) {
        LOGGER.info("====================================================================================");
        LOGGER.info("============== Verification version of CO:" + version);
        LOGGER.info("====================================================================================");

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
        if (!containerImageTag.equals(OLM_LATEST_CONTAINER_IMAGE_TAG) && (!(containerImageTag.equals("latest") || containerImageTag.equals(Environment.OLM_OPERATOR_VERSION_DEFAULT.substring(1))))) {
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
                if (containerImageTag.equals(OLM_LATEST_CONTAINER_IMAGE_TAG)) {
                    newKafkaVersion[0] = sortedKeys.last();
                }

                KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
                    //  2.2.1 -> 2.2 (gonna trim from kafka version)
                    String logMessageFormatVersion = newKafkaVersion[0].substring(0, 3);
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

        ClientUtils.waitForClientSuccess(producerName, namespace, messageUpgradeCount);
        ClientUtils.waitForClientSuccess(consumerName, namespace, messageUpgradeCount);

        // Delete jobs to make same names available for next upgrade
        kubeClient().deleteJob(producerName);
        kubeClient().deleteJob(consumerName);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    private String getFirstSupportedFromVersion() {

        Stream<Arguments> argumentStream = loadJsonUpgradeData();

        List<Arguments> supportedVersions = argumentStream.filter(arguments -> {
            String fromVersion = (String) arguments.get()[0];
            int middleFromVersion = Integer.parseInt(fromVersion.split("\\.")[1]);
            return middleFromVersion >= 18;
        }).collect(Collectors.toList());

        String firstSupportedFromVersion = (String) supportedVersions.get(0).get()[0];

        LOGGER.info("We are gonna use first supported version for OLM upgrade: {}", firstSupportedFromVersion);

        return firstSupportedFromVersion;
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

        // 1. Create subscription (+ operator group) with manual approval strategy
        // 2. Approve installation
        //   a) get name of install-plan
        //   b) approve installation
        // strimzi-cluster-operator-v0.19.0 <-- need concatenate version with starting 'v' before version
        OlmResource.clusterOperator(namespace, OlmInstallationStrategy.Manual, "v" + getFirstSupportedFromVersion());

        String beforeUpgradeVersionOfCo = OlmResource.getClusterOperatorVersion();

        // 3. perform verification of from version
        performUpgradeVerification(beforeUpgradeVersionOfCo);

        // 4. save install-plan to closed-map
        OlmResource.getClosedMapInstallPlan().put(OlmResource.getNonUsedInstallPlan(), Boolean.TRUE);
    }
}
