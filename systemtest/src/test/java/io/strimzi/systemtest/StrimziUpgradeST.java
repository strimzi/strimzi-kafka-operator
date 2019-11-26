/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.LogCollector;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import net.joshka.junit.json.params.JsonFileSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import javax.json.JsonObject;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static io.strimzi.systemtest.Constants.UPGRADE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(UPGRADE)
public class StrimziUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);

    public static final String NAMESPACE = "strimzi-upgrade-test";
    private String zkStsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
    private String kafkaStsName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
    private String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);

    private Map<String, String> zkPods;
    private Map<String, String> kafkaPods;
    private Map<String, String> eoPods;

    @ParameterizedTest()
    @JsonFileSource(resources = "/StrimziUpgradeST.json")
    void upgradeStrimziVersion(JsonObject parameters) throws Exception {

        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(parameters.getJsonObject("supportedK8sVersion").getString("version")));

        LOGGER.info("Going to test upgrade of Cluster Operator from version {} to version {}", parameters.getString("fromVersion"), parameters.getString("toVersion"));
        setNamespace(NAMESPACE);
        File coDir = null;
        File kafkaEphemeralYaml = null;
        File kafkaTopicYaml = null;
        File kafkaUserYaml = null;

        try {
            String url = parameters.getString("urlFrom");
            File dir = StUtils.downloadAndUnzip(url);

            coDir = new File(dir, parameters.getString("fromExamples") + "/install/cluster-operator/");

            // Modify + apply installation files
            copyModifyApply(coDir);

            LOGGER.info("Waiting for CO deployment");
            StUtils.waitForDeploymentReady("strimzi-cluster-operator", 1);

            // Deploy a Kafka cluster
            kafkaEphemeralYaml = new File(dir, parameters.getString("fromExamples") + "/examples/kafka/kafka-persistent.yaml");
            cmdKubeClient().create(kafkaEphemeralYaml);
            // Wait for readiness
            waitForClusterReadiness();

            // And a topic and a user
            kafkaTopicYaml = new File(dir, parameters.getString("fromExamples") + "/examples/topic/kafka-topic.yaml");
            cmdKubeClient().create(kafkaTopicYaml);
            kafkaUserYaml = new File(dir, parameters.getString("fromExamples") + "/examples/user/kafka-user.yaml");
            cmdKubeClient().create(kafkaUserYaml);

            makeSnapshots();
            logPodImages();
            // Execution of required procedures before upgrading CO
            changeKafkaAndLogFormatVersion(parameters.getJsonObject("proceduresBefore"));

            // Upgrade the CO
            // Modify + apply installation files
            if ("HEAD" .equals(parameters.getString("toVersion"))) {
                LOGGER.info("Updating");
                coDir = new File("../install/cluster-operator");
                upgradeClusterOperator(coDir, parameters.getJsonObject("imagesBeforeKafkaUpdate"));
            } else {
                url = parameters.getString("urlTo");
                dir = StUtils.downloadAndUnzip(url);
                coDir = new File(dir, parameters.getString("toExamples") + "/install/cluster-operator/");
                upgradeClusterOperator(coDir, parameters.getJsonObject("imagesBeforeKafkaUpdate"));
            }

            // Make snapshots of all pods
            makeSnapshots();
            logPodImages();
            //  Upgrade kafka
            changeKafkaAndLogFormatVersion(parameters.getJsonObject("proceduresAfter"));
            logPodImages();
            checkAllImages(parameters.getJsonObject("imagesAfterKafkaUpdate"));

            // Check errors in CO log
            assertNoCoErrorsLogged(0);

            // Tidy up
        } catch (KubeClusterException e) {
            if (kafkaEphemeralYaml != null) {
                cmdKubeClient().delete(kafkaEphemeralYaml);
            }
            if (coDir != null) {
                cmdKubeClient().delete(coDir);
            }
            e.printStackTrace();
            throw e;
        } finally {
            // Get current date to create a unique folder
            String currentDate = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
            String logDir = TEST_LOG_DIR + testClass + currentDate;

            LogCollector logCollector = new LogCollector(kubeClient(), new File(logDir));
            logCollector.collectEvents();
            logCollector.collectConfigMaps();
            logCollector.collectLogsFromPods();

            deleteInstalledYamls(coDir);
        }
    }

    private void upgradeClusterOperator(File coInstallDir, JsonObject images) {
        copyModifyApply(coInstallDir);
        LOGGER.info("Waiting for CO deployment");
        StUtils.waitForDeploymentReady("strimzi-cluster-operator", 1);
        waitForRollingUpdate();
        checkAllImages(images);
    }

    private void copyModifyApply(File root) {
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                cmdKubeClient().applyContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else if (f.getName().matches("050-Deployment.*")) {
                cmdKubeClient().applyContent(TestUtils.changeDeploymentNamespaceUpgrade(f, NAMESPACE));
            } else {
                cmdKubeClient().apply(f);
            }
        });
    }

    private void deleteInstalledYamls(File root) {
        if (root != null) {
            Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
                if (f.getName().matches(".*RoleBinding.*")) {
                    cmdKubeClient().deleteContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
                } else {
                    cmdKubeClient().delete(f);
                }
            });
        }
    }

    private void waitForClusterReadiness() {
        // Wait for readiness
        LOGGER.info("Waiting for Zookeeper StatefulSet");
        StUtils.waitForAllStatefulSetPodsReady("my-cluster-zookeeper", 3);
        LOGGER.info("Waiting for Kafka StatefulSet");
        StUtils.waitForAllStatefulSetPodsReady("my-cluster-kafka", 3);
        LOGGER.info("Waiting for EO Deployment");
        StUtils.waitForDeploymentReady("my-cluster-entity-operator", 1);
    }

    private void waitForRollingUpdate() {
        LOGGER.info("Waiting for ZK StatefulSet roll");
        StUtils.waitTillSsHasRolled(zkStsName, 3, zkPods);
        LOGGER.info("Waiting for Kafka StatefulSet roll");
        StUtils.waitTillSsHasRolled(kafkaStsName, 3, kafkaPods);
        LOGGER.info("Waiting for EO Deployment roll");
        // Check the TO and UO also got upgraded
        StUtils.waitTillDepHasRolled(eoDepName, 1, eoPods);
    }

    private void makeSnapshots() {
        zkPods = StUtils.ssSnapshot(zkStsName);
        kafkaPods = StUtils.ssSnapshot(kafkaStsName);
        eoPods = StUtils.depSnapshot(eoDepName);
    }

    private Kafka getKafka(String resourceName) {
        Resource<Kafka, DoneableKafka> namedResource = Crds.kafkaV1Alpha1Operation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(resourceName);
        return namedResource.get();
    }

    private void replaceKafka(String resourceName, Consumer<Kafka> editor) {
        Resource<Kafka, DoneableKafka> namedResource = Crds.kafkaV1Alpha1Operation(kubeClient().getClient()).inNamespace(kubeClient().getNamespace()).withName(resourceName);
        Kafka kafka = getKafka(resourceName);
        editor.accept(kafka);
        namedResource.replace(kafka);
    }

    private void checkAllImages(JsonObject images) {
        if (images.isEmpty()) {
            fail("There are no expected images");
        }
        String zkImage = images.getString("zookeeper");
        String kafkaImage = images.getString("kafka");
        String tOImage = images.getString("topicOperator");
        String uOImage = images.getString("userOperator");

        checkContainerImages(kubeClient().getStatefulSet(zkStsName).getSpec().getSelector().getMatchLabels(), zkImage);
        checkContainerImages(kubeClient().getStatefulSet(kafkaStsName).getSpec().getSelector().getMatchLabels(), kafkaImage);
        checkContainerImages(kubeClient().getDeployment(eoDepName).getSpec().getSelector().getMatchLabels(), 0, tOImage);
        checkContainerImages(kubeClient().getDeployment(eoDepName).getSpec().getSelector().getMatchLabels(), 1, uOImage);
    }

    private void checkContainerImages(Map<String, String> matchLabels, String image) {
        checkContainerImages(matchLabels, 0, image);
    }

    private void checkContainerImages(Map<String, String> matchLabels, int container, String image) {
        List<Pod> pods1 = kubeClient().listPods(matchLabels);
        for (Pod pod : pods1) {
            if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                LOGGER.debug("Expected image: {} \nCurrent image: {}", image, pod.getSpec().getContainers().get(container).getImage());
                assertThat("Used image is not valid!", pod.getSpec().getContainers().get(container).getImage(), is(image));
            }
        }
    }

    private void changeKafkaAndLogFormatVersion(JsonObject procedures) {
        if (!procedures.isEmpty()) {
            String kafkaVersion = procedures.getString("kafkaVersion");
            if (!kafkaVersion.isEmpty()) {
                LOGGER.info("Going to set Kafka version to " + kafkaVersion);
                replaceKafka(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion(kafkaVersion));
                LOGGER.info("Wait until kafka rolling update is finished");
                if (!kafkaVersion.equals("2.0.0")) {
                    StUtils.waitTillSsHasRolled(kafkaStsName, 3, kafkaPods);
                }
                makeSnapshots();
            }

            String logMessageVersion = procedures.getString("logMessageVersion");
            if (!logMessageVersion.isEmpty()) {
                LOGGER.info("Going to set log message format version to " + logMessageVersion);
                replaceKafka(CLUSTER_NAME, k -> k.getSpec().getKafka().getConfig().put("log.message.format.version", logMessageVersion));
                LOGGER.info("Wait until kafka rolling update is finished");
                StUtils.waitTillSsHasRolled(kafkaStsName, 3, kafkaPods);
                makeSnapshots();
            }
        }
    }

    void logPodImages() {
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

    @BeforeEach
    void setupEnvironment() {
        createNamespace(NAMESPACE);
    }

    @Override
    protected void tearDownEnvironmentAfterEach() {
        deleteNamespaces();
    }

    // There is no value of having teardown logic for class resources due to the fact that
    // CO was deployed by method StrimziUpgradeST.copyModifyApply() and removed by method StrimziUpgradeST.deleteInstalledYamls()
    @Override
    protected void tearDownEnvironmentAfterAll() {
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        deleteNamespaces();
        createNamespace(NAMESPACE);
    }
}
