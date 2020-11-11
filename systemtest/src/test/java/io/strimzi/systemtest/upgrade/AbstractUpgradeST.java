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

    protected String zkStsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
    protected String kafkaStsName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
    protected String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);

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

    protected void makeSnapshots() {
        coPods = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        zkPods = StatefulSetUtils.ssSnapshot(zkStsName);
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStsName);
        eoPods = DeploymentUtils.depSnapshot(eoDepName);
    }

    protected void changeKafkaAndLogFormatVersion(JsonObject procedures) {
        if (!procedures.isEmpty()) {
            String kafkaVersion = procedures.getString("kafkaVersion");
            if (!kafkaVersion.isEmpty()) {
                LOGGER.info("Going to set Kafka version to " + kafkaVersion);
                KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion(kafkaVersion));
                LOGGER.info("Wait until kafka rolling update is finished");
                if (!kafkaVersion.equals("2.0.0")) {
                    StatefulSetUtils.waitTillSsHasRolled(kafkaStsName, 3, kafkaPods);
                }
                makeSnapshots();
            }

            String logMessageVersion = procedures.getString("logMessageVersion");
            if (!logMessageVersion.isEmpty()) {
                LOGGER.info("Going to set log message format version to " + logMessageVersion);
                KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().getConfig().put("log.message.format.version", logMessageVersion));
                LOGGER.info("Wait until kafka rolling update is finished");
                StatefulSetUtils.waitTillSsHasRolled(kafkaStsName, 3, kafkaPods);
                makeSnapshots();
            }
        }
    }

    protected void logPodImages() {
        List<Pod> pods = kubeClient().listPods(kubeClient().getStatefulSetSelectors(zkStsName));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(kubeClient().getStatefulSetSelectors(kafkaStsName));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(kubeClient().getDeploymentSelectors(eoDepName));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(1).getImage());
        }
    }

    protected void waitForReadinessOfKafkaCluster() {
        LOGGER.info("Waiting for Zookeeper StatefulSet");
        StatefulSetUtils.waitForAllStatefulSetPodsReady(CLUSTER_NAME + "-zookeeper", 3);
        LOGGER.info("Waiting for Kafka StatefulSet");
        StatefulSetUtils.waitForAllStatefulSetPodsReady(CLUSTER_NAME + "-kafka", 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(CLUSTER_NAME + "-entity-operator", 1);
    }
}
