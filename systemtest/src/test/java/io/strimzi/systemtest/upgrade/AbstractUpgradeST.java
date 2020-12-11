/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.params.provider.Arguments;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class AbstractUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractUpgradeST.class);

    protected Map<String, String> zkPods;
    protected Map<String, String> kafkaPods;
    protected Map<String, String> eoPods;
    protected Map<String, String> coPods;

    protected File kafkaYaml;

    protected static JsonArray readUpgradeJson() {
        try (InputStream fis = new FileInputStream(TestUtils.USER_PATH + "/src/main/resources/StrimziUpgradeST.json")) {
            JsonReader reader = Json.createReader(fis);
            return reader.readArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(TestUtils.USER_PATH + "/src/main/resources/StrimziUpgradeST.json" + " file was not found.");
        }
    }

    protected static Stream<Arguments> loadJsonUpgradeData() {
        JsonArray upgradeData = readUpgradeJson();
        List<Arguments> parameters = new LinkedList<>();

        upgradeData.forEach(jsonData -> {
            JsonObject data = (JsonObject) jsonData;
            parameters.add(Arguments.of(data.getString("fromVersion"), data.getString("toVersion"), data));
        });

        return parameters.stream();
    }

    protected void makeSnapshots(String clusterName) {
        coPods = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(clusterName));
        kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));
        eoPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));
    }

    protected void changeKafkaAndLogFormatVersion(JsonObject procedures, String clusterName) {
        if (!procedures.isEmpty()) {
            String kafkaVersion = procedures.getString("kafkaVersion");
            if (!kafkaVersion.isEmpty()) {
                LOGGER.info("Going to set Kafka version to " + kafkaVersion);
                KafkaResource.replaceKafkaResource(clusterName, k -> k.getSpec().getKafka().setVersion(kafkaVersion));
                LOGGER.info("Wait until kafka rolling update is finished");
                if (!kafkaVersion.equals("2.0.0")) {
                    StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
                }
                makeSnapshots(clusterName);
            }

            String logMessageVersion = procedures.getString("logMessageVersion");
            String interBrokerProtocolVersion = procedures.getString("interBrokerProtocolVersion");
            if (!logMessageVersion.isEmpty() || !interBrokerProtocolVersion.isEmpty()) {
                KafkaResource.replaceKafkaResource(clusterName, k -> {
                    if (!logMessageVersion.isEmpty()) {
                        LOGGER.info("Going to set log message format version to " + logMessageVersion);
                        k.getSpec().getKafka().getConfig().put("log.message.format.version", logMessageVersion);
                    }

                    if (!interBrokerProtocolVersion.isEmpty()) {
                        LOGGER.info("Going to set inter-broker protocol version to " + interBrokerProtocolVersion);
                        k.getSpec().getKafka().getConfig().put("inter.broker.protocol.version", interBrokerProtocolVersion);
                    }
                });

                LOGGER.info("Wait until kafka rolling update is finished");
                StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
                makeSnapshots(clusterName);
            }
        }
    }

    protected void logPodImages(String clusterName) {
        List<Pod> pods = kubeClient().listPods(kubeClient().getStatefulSetSelectors(KafkaResources.zookeeperStatefulSetName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(kubeClient().getStatefulSetSelectors(KafkaResources.kafkaStatefulSetName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(kubeClient().getDeploymentSelectors(KafkaResources.entityOperatorDeploymentName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(1).getImage());
        }
    }

    protected void waitForReadinessOfKafkaCluster(String clusterName) {
        LOGGER.info("Waiting for Zookeeper StatefulSet");
        StatefulSetUtils.waitForAllStatefulSetPodsReady(clusterName + "-zookeeper", 3);
        LOGGER.info("Waiting for Kafka StatefulSet");
        StatefulSetUtils.waitForAllStatefulSetPodsReady(clusterName + "-kafka", 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(clusterName + "-entity-operator", 1);
    }
}
