/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.resources.operator.OlmResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.specific.OlmUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.systemtest.Constants.OLM_UPGRADE;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(OLM_UPGRADE)
public class OlmUpgradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(OlmUpgradeST.class);

    private final String namespace = "olm-upgrade-namespace";
    private final String producerName = "producer";
    private final String consumerName = "consumer";
    private final String topicUpgradeName = "topic-upgrade";
    private final int messageUpgradeCount =  50_000; // 10k ~= 23s, 50k ~= 115s
    private final KafkaBasicExampleClients kafkaBasicClientJob = new KafkaBasicExampleClients.Builder()
        .withProducerName(producerName)
        .withConsumerName(consumerName)
        .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
        .withTopicName(topicUpgradeName)
        .withMessageCount(messageUpgradeCount)
        .withDelayMs(1)
        .build();

    @ParameterizedTest(name = "testUpgradeStrimziVersion-{0}-{1}")
    @MethodSource("loadJsonUpgradeData")
    void testChainUpgrade(String fromVersion, String toVersion, JsonObject testParameters) throws IOException {

        // only 0.|18|.0 and more is supported
        assumeTrue(testParameters.getBoolean("olmUpgrade"));

        // perform verification of to version
        performUpgradeVerification(fromVersion, toVersion, testParameters);
    }

    private void performUpgradeVerification(String fromVersion, String toVersion, JsonObject testParameters) throws IOException {
        LOGGER.info("====================================================================================");
        LOGGER.info("============== Verification version of CO:" + fromVersion + " => " + toVersion);
        LOGGER.info("====================================================================================");

        // In chainUpgrade we want to setup CO only at the start
        if (kubeClient().listPodsByPrefixInName(ResourceManager.getCoDeploymentName()).size() == 0) {

            // we need to push CO class stack because of subscription (if CO is in method stack after upgrade subscription will be deleted)
            ResourceManager.setClassResources();

            // 1. Create subscription (+ operator group) with manual approval strategy
            // 2. Approve installation
            //   a) get name of install-plan
            //   b) approve installation
            // strimzi-cluster-operator-v0.19.0 <-- need concatenate version with starting 'v' before version
            OlmResource.clusterOperator(namespace, OlmInstallationStrategy.Manual, "v" + getFirstSupportedItemFromUpgradeJson().getString("fromVersion"));
        }

        // In chainUpgrade we want to setup Kafka only at the start and then upgrade it via CO
        if (KafkaResource.kafkaClient().inNamespace(namespace).withName(CLUSTER_NAME).get() == null) {
            JsonObject firstSupportedItemFromUpgradeJsonArray = getFirstSupportedItemFromUpgradeJson();
            String url = firstSupportedItemFromUpgradeJsonArray.getString("urlFrom");
            File dir = FileUtils.downloadAndUnzip(url);

            // In chainUpgrade we want to setup Kafka only at the begging and then upgrade it via CO
            kafkaYaml = new File(dir, firstSupportedItemFromUpgradeJsonArray.getString("fromExamples") + "/examples/kafka/kafka-persistent.yaml");
            LOGGER.info("Going to deploy Kafka from: {}", kafkaYaml.getPath());
            KubeClusterResource.cmdKubeClient().create(kafkaYaml);
            // Wait for readiness
            waitForReadinessOfKafkaCluster();

            OlmResource.getClosedMapInstallPlan().put(OlmResource.getNonUsedInstallPlan(), Boolean.TRUE);

            ResourceManager.setMethodResources();
        }

        kafkaBasicClientJob.producerStrimzi().done();
        kafkaBasicClientJob.consumerStrimzi().done();

        String clusterOperatorDeploymentName = ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME);
        LOGGER.info("Old deployment name of cluster operator is {}", clusterOperatorDeploymentName);

        // ======== Cluster Operator upgrade starts ========
        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        // wait until non-used install plan is present (sometimes install-plan did not append immediately and we need to wait for at least 10m)
        OlmUtils.waitUntilNonUsedInstallPlanIsPresent(toVersion);

        // Cluster Operator
        OlmResource.upgradeClusterOperator();

        // wait until RU is finished (first run skipping)
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        // ======== Cluster Operator upgrade ends ========

        clusterOperatorDeploymentName = ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME);
        LOGGER.info("New deployment name of cluster operator is {}", clusterOperatorDeploymentName);
        ResourceManager.setCoDeploymentName(clusterOperatorDeploymentName);

        // verification that cluster operator has correct version (install-plan) - strimzi-cluster-operator.v[version]
        String afterUpgradeVersionOfCo = OlmResource.getClusterOperatorVersion();

        // if HEAD -> 6.6.6 version
        toVersion = toVersion.equals("HEAD") ? Environment.OLM_LATEST_CONTAINER_IMAGE_TAG : toVersion;
        assertThat(afterUpgradeVersionOfCo, is(Environment.OLM_APP_BUNDLE_PREFIX + ".v" + toVersion));

        // ======== Kafka upgrade starts ========
        // Make snapshots of all pods
        makeSnapshots();
        logPodImages();
        changeKafkaAndLogFormatVersion(testParameters.getJsonObject("proceduresAfter"));
        logPodImages();
        // ======== Kafka upgrade ends ========

        ClientUtils.waitForClientSuccess(producerName, namespace, messageUpgradeCount);
        ClientUtils.waitForClientSuccess(consumerName, namespace, messageUpgradeCount);

        // Delete jobs to make same names available for next upgrade
        kubeClient().deleteJob(producerName);
        kubeClient().deleteJob(consumerName);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);

        // Save install-plan to closed-map
        OlmResource.getClosedMapInstallPlan().put(OlmResource.getNonUsedInstallPlan(), Boolean.TRUE);
    }

    /**
     * Loads auxiliary information from StrimziUpgradeST.json
     * @return json object with upgrade information
     */
    private JsonObject getFirstSupportedItemFromUpgradeJson() {
        Stream<Arguments> argumentStream = loadJsonUpgradeData();

        List<Arguments> olmUpgradeSupported = argumentStream.filter(arguments -> ((JsonObject) arguments.get()[2]).getBoolean("olmUpgrade")).collect(Collectors.toList());

        JsonObject upgradeInformation = (JsonObject) olmUpgradeSupported.get(0).get()[2];
        LOGGER.info("We are gonna use first supported upgrade information provided by json file for OLM upgrade: {}", upgradeInformation);
        return upgradeInformation;
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();

        cluster.setNamespace(namespace);
        cluster.createNamespace(namespace);
    }
}
