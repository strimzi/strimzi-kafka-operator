/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.extensions.StrimziExtension;
import io.strimzi.test.k8s.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;

@ExtendWith(StrimziExtension.class)
@Tag(REGRESSION)
public class StrimziUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);

    public static final String NAMESPACE = "strimzi-upgrade-test";

    private void copyModifyApply(File root) {
        Arrays.stream(root.listFiles()).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                KUBE_CLIENT.applyContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else if (f.getName().matches("050-Deployment.*")) {
                KUBE_CLIENT.applyContent(TestUtils.changeDeploymentNamespaceUpgrade(f, NAMESPACE));
            } else {
                KUBE_CLIENT.apply(f);
            }
        });
    }

    private void deleteInstalledYamls(File root) {
        Arrays.stream(root.listFiles()).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                KUBE_CLIENT.deleteContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else {
                KUBE_CLIENT.delete(f);
            }
        });
    }

    @Test
    void upgrade_0_10_0_to_HEAD() throws IOException {
        KUBE_CLIENT.namespace(NAMESPACE);
        File coDir = null;
        File kafkaEphemeralYaml = null;
        File kafkaTopicYaml = null;
        File kafkaUserYaml = null;
        // This is a default Kafka version in Strimzi release 0.10.0
        String kafkaVersionForOldStrimziVersion = "2.1.0";
        try {
            // Deploy a 0.10.0 cluster operator
            // https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.10.0/strimzi-0.10.0.zip
            String url = "https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.10.0/strimzi-0.10.0.zip";
            File dir = StUtils.downloadAndUnzip(url);

            coDir = new File(dir, "strimzi-0.10.0/install/cluster-operator/");
            // Modify + apply installation files
            copyModifyApply(coDir);

            LOGGER.info("Waiting for CO deployment");
            KUBE_CLIENT.waitForDeployment("strimzi-cluster-operator", 1);

            // Deploy a 0.10.0. Kafka cluster
            kafkaEphemeralYaml = new File(dir, "strimzi-0.10.0/examples/kafka/kafka-ephemeral.yaml");
            KUBE_CLIENT.create(kafkaEphemeralYaml);
            // Wait for readiness
            LOGGER.info("Waiting for Zookeeper StatefulSet");
            KUBE_CLIENT.waitForStatefulSet("my-cluster-zookeeper", 3);
            LOGGER.info("Waiting for Kafka StatefulSet");
            KUBE_CLIENT.waitForStatefulSet("my-cluster-kafka", 3);
            LOGGER.info("Waiting for EO Deployment");
            KUBE_CLIENT.waitForDeployment("my-cluster-entity-operator", 1);

            // And a topic and a user
            kafkaTopicYaml = new File(dir, "strimzi-0.10.0/examples/topic/kafka-topic.yaml");
            KUBE_CLIENT.create(kafkaTopicYaml);
            kafkaUserYaml = new File(dir, "strimzi-0.10.0/examples/user/kafka-user.yaml");
            KUBE_CLIENT.create(kafkaUserYaml);

            String zkSsName = KafkaResources.zookeeperStatefulSetName("my-cluster");
            String kafkaSsName = KafkaResources.kafkaStatefulSetName("my-cluster");
            String eoDepName = KafkaResources.entityOperatorDeploymentName("my-cluster");
            Map<String, String> zkPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, zkSsName);
            Map<String, String> kafkaPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, kafkaSsName);
            Map<String, String> eoPods = StUtils.depSnapshot(CLIENT, NAMESPACE, eoDepName);

            List<Pod> pods = CLIENT.pods().inNamespace(NAMESPACE).withLabels(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zkSsName).get().getSpec().getSelector().getMatchLabels()).list().getItems();
            for (Pod pod : pods) {
                LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
            }

            // Upgrade the CO, to current HEAD,
            LOGGER.info("Updating");
            copyModifyApply(new File("../install/cluster-operator"));
            LOGGER.info("Waiting for CO redeployment");
            KUBE_CLIENT.waitForDeployment("strimzi-cluster-operator", 1);

            LOGGER.info("Waiting for ZK SS roll");
            StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, zkSsName, zkPods);
            LOGGER.info("Checking ZK pods using new image");
            waitTillAllPodsUseImage(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zkSsName).get().getSpec().getSelector().getMatchLabels(),
                    "strimzi/kafka:latest-kafka-" + kafkaVersionForOldStrimziVersion);
            LOGGER.info("Waiting for Kafka SS roll");
            StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods);
            LOGGER.info("Checking Kafka pods using new image");
            waitTillAllPodsUseImage(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaSsName).get().getSpec().getSelector().getMatchLabels(),
                    "strimzi/kafka:latest-kafka-" + kafkaVersionForOldStrimziVersion);
            LOGGER.info("Waiting for EO Dep roll");
            // Check the TO and UO also got upgraded
            StUtils.waitTillDepHasRolled(CLIENT, NAMESPACE, eoDepName, eoPods);
            LOGGER.info("Checking EO pod using new image");
            waitTillAllContainersUseImage(
                    CLIENT.apps().deployments().inNamespace(NAMESPACE).withName(eoDepName).get().getSpec().getSelector().getMatchLabels(),
                    0,
                    "strimzi/topic-operator:latest");
            waitTillAllContainersUseImage(
                    CLIENT.apps().deployments().inNamespace(NAMESPACE).withName(eoDepName).get().getSpec().getSelector().getMatchLabels(),
                    1,
                    "strimzi/user-operator:latest");

            LOGGER.info("Updating snapshots before upgrading Kafka version");
            zkPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, zkSsName);
            kafkaPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, kafkaSsName);

            LOGGER.info("Upgrading Kafka version to {}", Resources.ST_KAFKA_VERSION);
            replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion(Resources.ST_KAFKA_VERSION));

            LOGGER.info("Waiting for ZK SS roll");
            StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, zkSsName, zkPods);
            LOGGER.info("Checking ZK pods using new Kafka version");
            waitTillAllPodsUseImage(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zkSsName).get().getSpec().getSelector().getMatchLabels(),
                    "strimzi/kafka:latest-kafka-" + Resources.ST_KAFKA_VERSION);
            LOGGER.info("Waiting for Kafka SS roll");
            StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods);
            LOGGER.info("Checking Kafka pods using Kafka version");
            waitTillAllPodsUseImage(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaSsName).get().getSpec().getSelector().getMatchLabels(),
                    "strimzi/kafka:latest-kafka-" + Resources.ST_KAFKA_VERSION);

            // Tidy up
        } catch (KubeClusterException e) {
            if (kafkaEphemeralYaml != null) {
                KUBE_CLIENT.delete(kafkaEphemeralYaml);
            }
            if (coDir != null) {
                KUBE_CLIENT.delete(coDir);
            }
            throw e;
        } finally {
            deleteInstalledYamls(new File("../install/cluster-operator"));
        }

    }

    private void waitTillAllPodsUseImage(Map<String, String> matchLabels, String image) {
        waitTillAllContainersUseImage(matchLabels, 0, image);
    }

    private void waitTillAllContainersUseImage(Map<String, String> matchLabels, int container, String image) {
        TestUtils.waitFor("All pods matching " + matchLabels + " to have image " + image, GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT, () -> {
            List<Pod> pods1 = CLIENT.pods().inNamespace(NAMESPACE).withLabels(matchLabels).list().getItems();
            for (Pod pod : pods1) {
                if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                    return false;
                }
            }
            return true;
        });
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        createNamespace(NAMESPACE);
    }
    @AfterAll
    void teardownEnvironment() {
        deleteNamespaces();
    }


    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        deleteNamespaces();
        createNamespace(NAMESPACE);
    }
}
