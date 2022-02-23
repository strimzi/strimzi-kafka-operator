/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.IsolatedSuite;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.executor.ExecResult;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.IOUtils;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static io.strimzi.systemtest.Constants.GLOBAL_POLL_INTERVAL;
import static io.strimzi.systemtest.Constants.GLOBAL_TIMEOUT;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.PATH_TO_KAFKA_TOPIC_CONFIG;
import static io.strimzi.systemtest.Constants.PATH_TO_PACKAGING_EXAMPLES;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@IsolatedSuite
public class AbstractUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractUpgradeST.class);

    protected File coDir = null;
    protected File kafkaTopicYaml = null;
    protected File kafkaUserYaml = null;

    protected final String clusterName = "my-cluster";

    protected Map<String, String> zkPods;
    protected Map<String, String> kafkaPods;
    protected Map<String, String> eoPods;
    protected Map<String, String> coPods;

    protected LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
    protected LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));


    protected final String topicName = "my-topic";
    protected final String userName = "my-user";
    protected final int upgradeTopicCount = 40;
    // ExpectedTopicCount contains additionally consumer-offset topic, my-topic and continuous-topic
    protected final int expectedTopicCount = upgradeTopicCount + 3;

    public static final String UPGRADE_JSON_FILE = "StrimziUpgradeST.json";
    public static final String DOWNGRADE_JSON_FILE = "StrimziDowngradeST.json";

    protected File kafkaYaml;

    protected static JsonArray readUpgradeJson(String fileName) {
        try {
            String jsonStr = IOUtils.toString(new FileReader(TestUtils.USER_PATH + "/src/test/resources/upgrade/" + fileName));

            return new JsonArray(jsonStr);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(TestUtils.USER_PATH + "/src/test/resources/upgrade/" + fileName + " file was not found.");
        }
    }

    protected static Map<String, JsonObject> buildMidStepUpgradeData(JsonObject jsonData) {
        List<TestKafkaVersion> testKafkaVersions = TestKafkaVersion.getSupportedKafkaVersions();
        TestKafkaVersion testKafkaVersion = testKafkaVersions.get(testKafkaVersions.size() - 1);

        Map<String, JsonObject> steps = new HashMap<>();

        String midStepUrl = jsonData.getString("urlFrom");
        String midStepVersion = jsonData.getString("fromVersion");
        String midStepExamples = jsonData.getString("fromExamples");

        JsonObject conversionTool = jsonData.getJsonObject("conversionTool");

        // X -> 0.22.0 data
        JsonObject midStep = JsonObject.mapFrom(jsonData);
        JsonObject afterMidStep = JsonObject.mapFrom(jsonData);
        if (jsonData.getString("prevVersion").isEmpty()) {
            midStep.put("urlFrom", jsonData.getString("urlFrom"));
            midStep.put("fromVersion", jsonData.getString("fromVersion"));
            midStep.put("fromExamples", jsonData.getString("fromExamples"));
            afterMidStep.put("urlFrom", "HEAD");
            afterMidStep.put("fromVersion", "HEAD");
            afterMidStep.put("fromExamples", "HEAD");
        } else {
            midStep.put("urlFrom", jsonData.getString("urlPrevVersion"));
            midStep.put("fromVersion", jsonData.getString("prevVersion"));
            midStep.put("fromExamples", jsonData.getString("prevVersionExamples"));
            afterMidStep.put("urlFrom", midStepUrl);
            afterMidStep.put("fromVersion", midStepVersion);
            afterMidStep.put("fromExamples", midStepExamples);
        }

        midStep.put("urlTo", midStepUrl);
        midStep.put("toVersion", midStepVersion);
        midStep.put("toExamples", midStepExamples);
        midStep.put("urlToConversionTool", conversionTool.getString("urlToConversionTool"));
        midStep.put("toConversionTool", conversionTool.getString("toConversionTool"));

        JsonObject midStepProcedures = new JsonObject();
        midStepProcedures.put("kafkaVersion", testKafkaVersion.version());
        midStepProcedures.put("logMessageVersion", testKafkaVersion.messageVersion());
        midStepProcedures.put("interBrokerProtocolVersion", testKafkaVersion.protocolVersion());
        midStep.put("proceduresAfterOperatorUpgrade", midStepProcedures);

        steps.put("midStep", midStep);
        steps.put("toHEAD", afterMidStep);

        return steps;
    }

    protected static Stream<Arguments> loadJsonUpgradeData() {
        JsonArray upgradeData = readUpgradeJson(UPGRADE_JSON_FILE);
        List<Arguments> parameters = new LinkedList<>();

        List<TestKafkaVersion> testKafkaVersions = TestKafkaVersion.getSupportedKafkaVersions();
        TestKafkaVersion testKafkaVersion = testKafkaVersions.get(testKafkaVersions.size() - 1);

        upgradeData.forEach(jsonData -> {
            JsonObject data = (JsonObject) jsonData;

            data.put("urlTo", "HEAD");
            data.put("toVersion", "HEAD");
            data.put("toExamples", "HEAD");

            // Generate procedures for upgrade
            JsonObject procedures = new JsonObject();
            procedures.put("kafkaVersion", testKafkaVersion.version());
            procedures.put("logMessageVersion", testKafkaVersion.messageVersion());
            procedures.put("interBrokerProtocolVersion", testKafkaVersion.protocolVersion());
            data.put("proceduresAfterOperatorUpgrade", procedures);

            parameters.add(Arguments.of(
                data.getString("fromVersion"),
                "HEAD",
                data.getString("strimziFeatureGatesFlagsBefore").isEmpty() ? "None" : data.getString("strimziFeatureGatesFlagsBefore"),
                data.getString("strimziFeatureGatesFlagsAfter").isEmpty() ? "None" : data.getString("strimziFeatureGatesFlagsAfter"),
                data));
        });

        return parameters.stream();
    }

    protected void makeSnapshots() {
        coPods = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        zkPods = PodUtils.podSnapshot(INFRA_NAMESPACE, zkSelector);
        kafkaPods = PodUtils.podSnapshot(INFRA_NAMESPACE, kafkaSelector);
        eoPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));
    }

    @SuppressWarnings("CyclomaticComplexity")
    protected void changeKafkaAndLogFormatVersion(JsonObject procedures, JsonObject testParameters, ExtensionContext extensionContext) throws IOException {
        // Get Kafka configurations
        String operatorVersion = testParameters.getString("toVersion");
        String currentLogMessageFormat = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, ".spec.kafka.config.log\\.message\\.format\\.version");
        String currentInterBrokerProtocol = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, ".spec.kafka.config.inter\\.broker\\.protocol\\.version");
        // Get Kafka version
        String kafkaVersionFromCR = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, ".spec.kafka.version");
        kafkaVersionFromCR = kafkaVersionFromCR.equals("") ? null : kafkaVersionFromCR;
        String kafkaVersionFromProcedure = procedures.getString("kafkaVersion");

        // #######################################################################
        // #################    Update CRs to latest version   ###################
        // #######################################################################
        String toUrl = testParameters.getString("urlTo");
        String examplesPath = "";
        if (toUrl.equals("HEAD")) {
            examplesPath = PATH_TO_PACKAGING_EXAMPLES + "";
        } else {
            File dir = FileUtils.downloadAndUnzip(toUrl);
            examplesPath = dir.getAbsolutePath() + "/" + testParameters.getString("toExamples") + "/examples";
        }

        kafkaYaml = new File(examplesPath + "/kafka/kafka-persistent.yaml");
        LOGGER.info("Deploying Kafka from: {}", kafkaYaml.getPath());
        // Change kafka version of it's empty (null is for remove the version)
        String defaultValueForVersions = kafkaVersionFromCR == null ? null : TestKafkaVersion.getSpecificVersion(kafkaVersionFromCR).messageVersion();
        cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaConfiguration(kafkaYaml, kafkaVersionFromCR, defaultValueForVersions, defaultValueForVersions));

        kafkaUserYaml = new File(examplesPath + "/user/kafka-user.yaml");
        LOGGER.info("Deploying KafkaUser from: {}", kafkaUserYaml.getPath());
        cmdKubeClient().applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));

        kafkaTopicYaml = new File(examplesPath + "/topic/kafka-topic.yaml");
        LOGGER.info("Deploying KafkaTopic from: {}", kafkaTopicYaml.getPath());
        cmdKubeClient().applyContent(TestUtils.readFile(kafkaTopicYaml));
        // #######################################################################


        if (!procedures.isEmpty() && (!currentLogMessageFormat.isEmpty() || !currentInterBrokerProtocol.isEmpty())) {
            if (!kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure) && extensionContext.getTestClass().get().getSimpleName().toLowerCase(Locale.ROOT).contains("upgrade")) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);
                LOGGER.info("Wait until Kafka rolling update is finished");
                kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(INFRA_NAMESPACE, kafkaSelector, 3, kafkaPods);
            }

            String logMessageVersion = procedures.getString("logMessageVersion");
            String interBrokerProtocolVersion = procedures.getString("interBrokerProtocolVersion");

            if (!logMessageVersion.isEmpty() || !interBrokerProtocolVersion.isEmpty()) {
                if (!logMessageVersion.isEmpty()) {
                    LOGGER.info("Set log message format version to {} (current version is {})", logMessageVersion, currentLogMessageFormat);
                    cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, "/spec/kafka/config/log.message.format.version", logMessageVersion);
                }

                if (!interBrokerProtocolVersion.isEmpty()) {
                    LOGGER.info("Set inter-broker protocol version to {} (current version is {})", interBrokerProtocolVersion, currentInterBrokerProtocol);
                    LOGGER.info("Set inter-broker protocol version to " + interBrokerProtocolVersion);
                    cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, "/spec/kafka/config/inter.broker.protocol.version", interBrokerProtocolVersion);
                }

                if ((currentInterBrokerProtocol != null && !currentInterBrokerProtocol.equals(interBrokerProtocolVersion)) ||
                        (currentLogMessageFormat != null && !currentLogMessageFormat.isEmpty() && !currentLogMessageFormat.equals(logMessageVersion))) {
                    LOGGER.info("Wait until Kafka rolling update is finished");
                    kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(INFRA_NAMESPACE, kafkaSelector, 3, kafkaPods);
                }
                makeSnapshots();
            }

            if (!kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure) && extensionContext.getTestClass().get().getSimpleName().toLowerCase(Locale.ROOT).contains("downgrade")) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);
                LOGGER.info("Wait until Kafka rolling update is finished");
                kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(INFRA_NAMESPACE, kafkaSelector, kafkaPods);
            }
        }
    }

    protected static Stream<Arguments> loadJsonDowngradeData() {
        JsonArray upgradeData = readUpgradeJson(DOWNGRADE_JSON_FILE);
        List<Arguments> parameters = new LinkedList<>();

        upgradeData.forEach(jsonData -> {
            JsonObject data = (JsonObject) jsonData;
            parameters.add(Arguments.of(data.getString("fromVersion"), data.getString("toVersion"), data));
        });

        return parameters.stream();
    }

    protected void logPodImages(String clusterName) {
        List<Pod> pods = kubeClient().listPods(KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(kubeClient().getDeploymentSelectors(KafkaResources.entityOperatorDeploymentName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(1).getImage());
        }
    }

    protected void waitForKafkaClusterRollingUpdate() {
        LOGGER.info("Waiting for ZK StatefulSet roll");
        zkPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(INFRA_NAMESPACE, zkSelector, 3, zkPods);
        LOGGER.info("Waiting for Kafka StatefulSet roll");
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(INFRA_NAMESPACE, kafkaSelector, 3, kafkaPods);
        LOGGER.info("Waiting for EO Deployment roll");
        // Check the TO and UO also got upgraded
        eoPods = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);
    }

    protected void waitForReadinessOfKafkaCluster() {
        LOGGER.info("Waiting for Zookeeper StatefulSet");
        RollingUpdateUtils.waitForComponentAndPodsReady(zkSelector, 3);
        LOGGER.info("Waiting for Kafka StatefulSet");
        RollingUpdateUtils.waitForComponentAndPodsReady(kafkaSelector, 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaResources.entityOperatorDeploymentName(clusterName), 1);
    }

    protected void changeClusterOperator(JsonObject testParameters, String namespace, ExtensionContext extensionContext) throws IOException {
        File coDir;
        // Modify + apply installation files
        LOGGER.info("Update CO from {} to {}", testParameters.getString("fromVersion"), testParameters.getString("toVersion"));
        if ("HEAD".equals(testParameters.getString("toVersion"))) {
            coDir = new File(TestUtils.USER_PATH + "/../packaging/install/cluster-operator");
        } else {
            String url = testParameters.getString("urlTo");
            File dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, testParameters.getString("toExamples") + "/install/cluster-operator/");
        }

        copyModifyApply(coDir, namespace, extensionContext, testParameters.getString("strimziFeatureGatesFlagsAfter"));

        LOGGER.info("Waiting for CO upgrade");
        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coPods);
    }

    protected void copyModifyApply(File root, String namespace, ExtensionContext extensionContext, final String strimziFeatureGatesValue) {
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                cmdKubeClient().replaceContent(TestUtils.changeRoleBindingSubject(f, namespace));
            } else if (f.getName().matches(".*Deployment.*")) {
                cmdKubeClient().replaceContent(StUtils.changeDeploymentConfiguration(f, namespace, strimziFeatureGatesValue));
            } else {
                cmdKubeClient().replaceContent(TestUtils.getContent(f, TestUtils::toYamlString));
            }
        });
        // Set info that CO is already installed
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(io.strimzi.systemtest.Constants.PREPARE_OPERATOR_ENV_KEY + namespace, false);
    }

    protected void deleteInstalledYamls(File root, String namespace) {
        if (kafkaUserYaml != null) {
            LOGGER.info("Deleting KafkaUser configuration files");
            cmdKubeClient().delete(kafkaUserYaml);
        }
        if (kafkaTopicYaml != null) {
            LOGGER.info("Deleting KafkaTopic configuration files");
            cmdKubeClient().delete(kafkaTopicYaml);
        }
        if (kafkaYaml != null) {
            LOGGER.info("Deleting Kafka configuration files");
            cmdKubeClient().delete(kafkaYaml);
        }
        if (root != null) {
            Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
                try {
                    if (f.getName().matches(".*RoleBinding.*")) {
                        cmdKubeClient().deleteContent(TestUtils.changeRoleBindingSubject(f, namespace));
                    } else {
                        cmdKubeClient().delete(f);
                    }
                } catch (Exception ex) {
                    LOGGER.warn("Failed to delete resources: {}", f.getName());
                }
            });
        }
    }

    protected void checkAllImages(JsonObject images) {
        if (images.isEmpty()) {
            fail("There are no expected images");
        }
        String zkImage = images.getString("zookeeper");
        String kafkaImage = images.getString("kafka");
        String tOImage = images.getString("topicOperator");
        String uOImage = images.getString("userOperator");

        List<EnvVar> envVars = kubeClient().getDeployment(io.strimzi.systemtest.Constants.STRIMZI_DEPLOYMENT_NAME)
            .getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();

        boolean spsEnabled = envVars.stream()
            .anyMatch(envVar -> envVar.getName().equals(Environment.STRIMZI_FEATURE_GATES_ENV) && envVar.getValue() != null && envVar.getValue().contains("+UseStrimziPodSets"));

        checkContainerImages(StUtils.getStrimziPodSetOrStatefulSetMatchLabels(KafkaResources.zookeeperStatefulSetName(clusterName), spsEnabled), zkImage);
        checkContainerImages(StUtils.getStrimziPodSetOrStatefulSetMatchLabels(KafkaResources.kafkaStatefulSetName(clusterName), spsEnabled), kafkaImage);
        checkContainerImages(kubeClient().getDeployment(KafkaResources.entityOperatorDeploymentName(clusterName)).getSpec().getSelector().getMatchLabels(), tOImage);
        checkContainerImages(kubeClient().getDeployment(KafkaResources.entityOperatorDeploymentName(clusterName)).getSpec().getSelector().getMatchLabels(), 1, uOImage);
    }

    protected void checkContainerImages(Map<String, String> matchLabels, String image) {
        checkContainerImages(matchLabels, 0, image);
    }

    protected void checkContainerImages(Map<String, String> matchLabels, int container, String image) {
        List<Pod> pods1 = kubeClient().listPods(matchLabels);
        for (Pod pod : pods1) {
            if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                LOGGER.debug("Expected image for pod {}: {} \nCurrent image: {}", pod.getMetadata().getName(), image, pod.getSpec().getContainers().get(container).getImage());
                assertThat("Used image for pod " + pod.getMetadata().getName() + " is not valid!", pod.getSpec().getContainers().get(container).getImage(), containsString(image));
            }
        }
    }

    protected void setupEnvAndUpgradeClusterOperator(ExtensionContext extensionContext, JsonObject testParameters, String producerName, String consumerName,
                                                   String continuousTopicName, String continuousConsumerGroup,
                                                   String kafkaVersion, String namespace) throws IOException {

        int continuousClientsMessageCount = testParameters.getJsonObject("client").getInteger("continuousClientsMessages");

        LOGGER.info("Test upgrade of ClusterOperator from version {} to version {}", testParameters.getString("fromVersion"), testParameters.getString("toVersion"));
        cluster.setNamespace(namespace);

        String operatorVersion = testParameters.getString("fromVersion");
        String url = null;
        File dir = null;

        if ("HEAD".equals(testParameters.getString("fromVersion"))) {
            coDir = new File(TestUtils.USER_PATH + "/../packaging/install/cluster-operator");
        } else {
            url = testParameters.getString("urlFrom");
            dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, testParameters.getString("fromExamples") + "/install/cluster-operator/");
        }

        // Modify + apply installation files
        copyModifyApply(coDir, namespace, extensionContext, testParameters.getString("strimziFeatureGatesFlagsBefore"));

        LOGGER.info("Waiting for {} deployment", ResourceManager.getCoDeploymentName());
        DeploymentUtils.waitForDeploymentAndPodsReady(ResourceManager.getCoDeploymentName(), 1);
        LOGGER.info("{} is ready", ResourceManager.getCoDeploymentName());

        if (!cmdKubeClient().getResources(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion)).contains(clusterName)) {
            // Deploy a Kafka cluster
            if ("HEAD".equals(testParameters.getString("fromVersion"))) {
                resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
                    .editSpec()
                        .editKafka()
                            .withVersion(kafkaVersion)
                            .addToConfig("log.message.format.version", TestKafkaVersion.getSpecificVersion(kafkaVersion).messageVersion())
                            .addToConfig("inter.broker.protocol.version", TestKafkaVersion.getSpecificVersion(kafkaVersion).protocolVersion())
                        .endKafka()
                    .endSpec()
                    .build());
            } else {
                kafkaYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/kafka/kafka-persistent.yaml");
                LOGGER.info("Deploy Kafka from: {}", kafkaYaml.getPath());
                // Change kafka version of it's empty (null is for remove the version)
                cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaVersion(kafkaYaml, kafkaVersion));
                // Wait for readiness
                waitForReadinessOfKafkaCluster();
            }
        }
        if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaUser.RESOURCE_PLURAL, operatorVersion)).contains(userName)) {
            if ("HEAD".equals(testParameters.getString("fromVersion"))) {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, userName).build());
            } else {
                kafkaUserYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/user/kafka-user.yaml");
                LOGGER.info("Deploy KafkaUser from: {}", kafkaUserYaml.getPath());
                cmdKubeClient().applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));
                ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaUser.RESOURCE_PLURAL, operatorVersion), userName);
            }
        }
        if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL, operatorVersion)).contains(topicName)) {
            if ("HEAD".equals(testParameters.getString("fromVersion"))) {
                resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
            } else {
                kafkaTopicYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/topic/kafka-topic.yaml");
                LOGGER.info("Deploy KafkaTopic from: {}", kafkaTopicYaml.getPath());
                cmdKubeClient().create(kafkaTopicYaml);
                ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL, operatorVersion), topicName);
            }
        }
        // Create bunch of topics for upgrade if it's specified in configuration
        if (testParameters.getBoolean("generateTopics")) {
            for (int x = 0; x < upgradeTopicCount; x++) {
                if ("HEAD".equals(testParameters.getString("fromVersion"))) {
                    resourceManager.createResource(extensionContext, false, KafkaTopicTemplates.topic(clusterName, topicName + "-" + x, 1, 1, 1)
                        .editSpec()
                            .withTopicName(topicName + "-" + x)
                        .endSpec()
                        .build());
                } else {
                    kafkaTopicYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/topic/kafka-topic.yaml");
                    cmdKubeClient().applyContent(TestUtils.getContent(kafkaTopicYaml, TestUtils::toYamlString).replace("name: \"my-topic\"", "name: \"" + topicName + "-" + x + "\""));
                }
            }
        }

        if (continuousClientsMessageCount != 0) {
            // ##############################
            // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
            // ##############################
            // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
            if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL, operatorVersion)).contains(continuousTopicName)) {
                String pathToTopicExamples = testParameters.getString("fromExamples").equals("HEAD") ? PATH_TO_KAFKA_TOPIC_CONFIG : testParameters.getString("fromExamples") + "/examples/topic/kafka-topic.yaml";

                kafkaTopicYaml = new File(dir, pathToTopicExamples);
                cmdKubeClient().applyContent(TestUtils.getContent(kafkaTopicYaml, TestUtils::toYamlString)
                        .replace("name: \"my-topic\"", "name: \"" + continuousTopicName + "\"")
                        .replace("partitions: 1", "partitions: 3")
                        .replace("replicas: 1", "replicas: 3") +
                        "    min.insync.replicas: 2");

                ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL, operatorVersion), continuousTopicName);
            }

            String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

            KafkaClients kafkaBasicClientJob = new KafkaClientsBuilder()
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withTopicName(continuousTopicName)
                .withMessageCount(continuousClientsMessageCount)
                .withAdditionalConfig(producerAdditionConfiguration)
                .withConsumerGroup(continuousConsumerGroup)
                .withDelayMs(1000)
                .build();

            resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerStrimzi());
            resourceManager.createResource(extensionContext, kafkaBasicClientJob.consumerStrimzi());
            // ##############################
        }

        makeSnapshots();
        logPodImages(clusterName);
    }

    protected void verifyProcedure(JsonObject testParameters, String producerName, String consumerName, String namespace) {
        int continuousClientsMessageCount = testParameters.getJsonObject("client").getInteger("continuousClientsMessages");

        if (testParameters.getBoolean("generateTopics")) {
            // Check that topics weren't deleted/duplicated during upgrade procedures
            String listedTopics = cmdKubeClient().getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL));
            int additionalTopics = testParameters.getInteger("additionalTopics", 0);
            assertThat("KafkaTopic list doesn't have expected size", Long.valueOf(listedTopics.lines().count() - 1).intValue(), is(expectedTopicCount + additionalTopics));
            assertThat("KafkaTopic " + topicName + " is not in expected topic list",
                    listedTopics.contains(topicName), is(true));
            for (int x = 0; x < upgradeTopicCount; x++) {
                assertThat("KafkaTopic " + topicName + "-" + x + " is not in expected topic list", listedTopics.contains(topicName + "-" + x), is(true));
            }
        }

        if (continuousClientsMessageCount != 0) {
            // ##############################
            // Validate that continuous clients finished successfully
            // ##############################
            ClientUtils.waitForClientsSuccess(producerName, consumerName, namespace, continuousClientsMessageCount);
            // ##############################
        }
    }

    protected String getResourceApiVersion(String resourcePlural) {
        return getResourceApiVersion(resourcePlural, "HEAD");
    }

    protected String getResourceApiVersion(String resourcePlural, String coVersion) {
        if (coVersion.equals("HEAD") || TestKafkaVersion.compareDottedVersions(coVersion, "0.22.0") >= 0) {
            return resourcePlural + "." + Constants.V1BETA2 + "." + Constants.RESOURCE_GROUP_NAME;
        } else {
            return resourcePlural + "." + Constants.V1BETA1 + "." + Constants.RESOURCE_GROUP_NAME;
        }
    }

    protected JsonObject getConversionToolDataFromUpgradeJSON() {
        JsonArray upgradeFile = readUpgradeJson(UPGRADE_JSON_FILE);
        return upgradeFile.getJsonObject(0).getJsonObject("conversionTool");
    }

    protected void convertCRDs(JsonObject conversionTool, String namespace) throws IOException {
        String url = conversionTool.getString("urlToConversionTool");
        File dir = FileUtils.downloadAndUnzip(url);
        String convertorPath = dir.getAbsolutePath() + "/" + conversionTool.getString("toConversionTool") + "/bin/api-conversion.sh";

        Exec.exec("chmod", "+x", convertorPath);

        LOGGER.info("Converting CRs ...");
        // run conversion of crs
        // CRs conversion may fail, because for 0.23 for example it's already done
        // CRS conversion needs old versions of Strimzi CRDs, which are not available after 0.22
        if (cmdKubeClient().exec(true, Level.DEBUG, "get", "crd", "kafkas.kafka.strimzi.io", "-o", "jsonpath={.spec.versions}").out().trim().contains(Constants.V1ALPHA1)) {
            ExecResult execResult = Exec.exec(convertorPath, "cr", "-n=" + namespace);
            LOGGER.debug("CRs conversion STDOUT:");
            LOGGER.debug(execResult.out());
            LOGGER.debug("CRs conversion STDERR:");
            LOGGER.debug(execResult.err());
            LOGGER.info("CRs conversion done!");

            // run crd-upgrade
            LOGGER.info("Converting CRDs");
            execResult = Exec.exec(convertorPath, "crd");
            LOGGER.debug("CRDs conversion STDOUT:");
            LOGGER.debug(execResult.out());
            LOGGER.debug("CRDs conversion STDERR:");
            LOGGER.debug(execResult.err());
            LOGGER.info("CRDs conversion done!");

            waitForKafkaCRDChange();
        } else {
            LOGGER.info("CRs and CRDs already have v1beta2 versions");
        }
    }

    protected void waitForKafkaCRDChange() {
        TestUtils.waitFor("Kafka CRD kafkas.kafka.strimzi.io will change it's api version", GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT,
            () -> cmdKubeClient().exec(true, Level.TRACE, "get", "crd", "kafkas.kafka.strimzi.io", "-o", "jsonpath={.status.storedVersions}").out().trim().contains(Constants.V1BETA2));
    }
}
