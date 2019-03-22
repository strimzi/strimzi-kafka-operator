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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;

@ExtendWith(StrimziExtension.class)
@Tag(REGRESSION)
public class StrimziUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);

    public static final String NAMESPACE = "strimzi-upgrade-test";
    private String zkSsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
    private String kafkaSsName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
    private String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);

    private Map<String, String> zkPods;
    private Map<String, String> kafkaPods;
    private Map<String, String> eoPods;

    private void copyModifyApply(File root) {
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
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
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                KUBE_CLIENT.deleteContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else {
                KUBE_CLIENT.delete(f);
            }
        });
    }

//    enum StrimziVersions {
//        FROM_0_8_2_TO_0_11_1 ("0.8.2", "0.11.1", new String[]{"set log message format version to 2.0", "set Kafka version to 2.0.0"}),
//        FROM_0_11_1_TO_MASTER ("0.11.0", "master", new String[]{""});
//
//        final String fromVersion;
//        final String toVersion;
//        final String[] preconditions;
//
//        StrimziVersions(String fromVersion, String toVersion, String[] preconditions) {
//            this.fromVersion = fromVersion;
//            this.toVersion = toVersion;
//            this.preconditions = preconditions;
//        }
//    }

    enum StrimziVersions {
        FROM_0_8_2_TO_0_11_1 ("0.8.2", "0.11.1", "2.0.0"),
        FROM_0_11_1_TO_MASTER ("0.11.1", "master", "2.1.1");

        final String fromVersion;
        final String toVersion;
        final String kafkaVersion;

        StrimziVersions(String fromVersion, String toVersion, String kafkaVersion) {
            this.fromVersion = fromVersion;
            this.toVersion = toVersion;
            this.kafkaVersion = kafkaVersion;
        }
    }

    private void waitForClusterReadiness() {
        // Wait for readiness
        LOGGER.info("Waiting for Zookeeper StatefulSet");
        KUBE_CLIENT.waitForStatefulSet("my-cluster-zookeeper", 3);
        LOGGER.info("Waiting for Kafka StatefulSet");
        KUBE_CLIENT.waitForStatefulSet("my-cluster-kafka", 3);
        LOGGER.info("Waiting for EO Deployment");
        KUBE_CLIENT.waitForDeployment("my-cluster-entity-operator", 1);
    }

    private void waitForRollingUpdate(String strimziVersion, String kafkaVersion) {
        String dockerTagWithKafkaVersion = strimziVersion + "-kafka-" + kafkaVersion;

        LOGGER.info("Waiting for ZK SS roll");
        StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, zkSsName, zkPods);
        LOGGER.info("Checking ZK pods using new image");
        waitTillAllPodsUseImage(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zkSsName).get().getSpec().getSelector().getMatchLabels(),
                "strimzi/kafka:" + dockerTagWithKafkaVersion);

        LOGGER.info("Waiting for Kafka SS roll");
        StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods);
        LOGGER.info("Checking Kafka pods using new image");
        waitTillAllPodsUseImage(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaSsName).get().getSpec().getSelector().getMatchLabels(),
                "strimzi/kafka:" + dockerTagWithKafkaVersion);
        LOGGER.info("Waiting for EO Dep roll");
        // Check the TO and UO also got upgraded
        StUtils.waitTillDepHasRolled(CLIENT, NAMESPACE, eoDepName, eoPods);
        LOGGER.info("Checking EO pod using new image");
        waitTillAllContainersUseImage(
                CLIENT.apps().deployments().inNamespace(NAMESPACE).withName(eoDepName).get().getSpec().getSelector().getMatchLabels(),
                0,
                "strimzi/topic-operator:" + strimziVersion);
        waitTillAllContainersUseImage(
                CLIENT.apps().deployments().inNamespace(NAMESPACE).withName(eoDepName).get().getSpec().getSelector().getMatchLabels(),
                1,
                "strimzi/user-operator:" + strimziVersion);
    }

    private void makeSnapshots() {
        zkPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, zkSsName);
        kafkaPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, kafkaSsName);
        eoPods = StUtils.depSnapshot(CLIENT, NAMESPACE, eoDepName);
    }


    @ParameterizedTest
    @EnumSource(value = StrimziVersions.class, names = {"FROM_0_8_2_TO_0_11_1", "FROM_0_11_1_TO_MASTER"})
    void upgradeStrimziVersion(StrimziVersions versions) throws IOException {
        KUBE_CLIENT.namespace(NAMESPACE);
        File coDir = null;
        File kafkaEphemeralYaml = null;
        File kafkaTopicYaml = null;
        File kafkaUserYaml = null;
        try {
            String url = "https://github.com/strimzi/strimzi-kafka-operator/releases/download/" + versions.fromVersion + "/strimzi-" + versions.fromVersion + ".zip";
            File dir = StUtils.downloadAndUnzip(url);

            coDir = new File(dir, "strimzi-" + versions.fromVersion + "/install/cluster-operator/");
            // Modify + apply installation files
            copyModifyApply(coDir);

            LOGGER.info("Waiting for CO deployment");
            KUBE_CLIENT.waitForDeployment("strimzi-cluster-operator", 1);

            // Deploy a Kafka cluster
            kafkaEphemeralYaml = new File(dir, "strimzi-" + versions.fromVersion + "/examples/kafka/kafka-ephemeral.yaml");
            KUBE_CLIENT.create(kafkaEphemeralYaml);
            // Wait for readiness
            waitForClusterReadiness();

            // And a topic and a user
            kafkaTopicYaml = new File(dir, "strimzi-" + versions.fromVersion + "/examples/topic/kafka-topic.yaml");
            KUBE_CLIENT.create(kafkaTopicYaml);
            kafkaUserYaml = new File(dir, "strimzi-" + versions.fromVersion + "/examples/user/kafka-user.yaml");
            KUBE_CLIENT.create(kafkaUserYaml);

            makeSnapshots();

            List<Pod> pods = CLIENT.pods().inNamespace(NAMESPACE).withLabels(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zkSsName).get().getSpec().getSelector().getMatchLabels()).list().getItems();
            for (Pod pod : pods) {
                LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
            }

            // Update Kafka version
            if(!versions.kafkaVersion.isEmpty()) {
                replaceKafkaResource(CLUSTER_NAME, k -> {
                    if(k.getSpec().getKafka().getVersion() == null || !k.getSpec().getKafka().getVersion().equals(versions.kafkaVersion)) {
                        switch (versions.kafkaVersion) {
                            case "2.0.0": {
                                replaceKafkaResource(CLUSTER_NAME, ka -> ka.getSpec().getKafka().getConfig().put("log.message.format.version", "2.0"));
                                StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods);
                                makeSnapshots();
                                replaceKafkaResource(CLUSTER_NAME, ka -> ka.getSpec().getKafka().setVersion(versions.kafkaVersion));
                                StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods);
                                makeSnapshots();
                                break;
                            }
                            case "2.1.0": {
                                replaceKafkaResource(CLUSTER_NAME, ka -> ka.getSpec().getKafka().getConfig().put("log.message.format.version", "2.1"));
                                StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods);
                                makeSnapshots();
                                replaceKafkaResource(CLUSTER_NAME, ka -> ka.getSpec().getKafka().setVersion(versions.kafkaVersion));
                                StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods);
                                makeSnapshots();
                                break;
                            }
                            case "2.1.1": {
                                replaceKafkaResource(CLUSTER_NAME, ka -> ka.getSpec().getKafka().getConfig().put("log.message.format.version", "2.1"));
                                StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods);
                                makeSnapshots();
                                replaceKafkaResource(CLUSTER_NAME, ka -> ka.getSpec().getKafka().setVersion(versions.kafkaVersion));
                                StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods);
                                makeSnapshots();
                                break;
                            }
                        }
                    }
                });
                //TODO wait for rolling update after updating version
            }

//            //apply preconditions
//            Arrays.asList(versions.preconditions).forEach(condition -> {
//                switch (condition) {
//                    case "set log message format version to 2.0": {
//                        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().getConfig().put("log.message.format.version", "2.0"));
//                        break;
//                    }
//                    case "set log message format version to 2.1": {
//                        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().getConfig().put("log.message.format.version", "2.1"));
//                        break;
//                    }
//                    case "set Kafka version to 2.0.0": {
//                        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion("2.0.0"));
//                        break;
//                    }
//                    case "set Kafka version to 2.1.0": {
//                        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion("2.1.0"));
//                        break;
//                    }
//                    case "set Kafka version to 2.1.1": {
//                        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion("2.1.1"));
//                        break;
//                    }
//                }
//            });

            switch (versions.toVersion) {
                case "master": {
                    // Upgrade the CO, to current HEAD,
                    LOGGER.info("Updating");
                    copyModifyApply(new File("../install/cluster-operator"));
                    LOGGER.info("Waiting for CO redeployment");
                    KUBE_CLIENT.waitForDeployment("strimzi-cluster-operator", 1);
                    break;
                }
                default: {
                    url = "https://github.com/strimzi/strimzi-kafka-operator/releases/download/" + versions.toVersion + "/strimzi-" + versions.toVersion + ".zip";
                    dir = StUtils.downloadAndUnzip(url);

                    coDir = new File(dir, "strimzi-" + versions.toVersion + "/install/cluster-operator/");
                    // Modify + apply installation files
                    copyModifyApply(coDir);

                    LOGGER.info("Waiting for CO deployment");
                    KUBE_CLIENT.waitForDeployment("strimzi-cluster-operator", 1);
                }
            }

            waitForRollingUpdate(versions.toVersion, versions.kafkaVersion);
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
