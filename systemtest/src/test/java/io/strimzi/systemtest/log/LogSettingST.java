/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.log;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.JvmOptionsBuilder;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectS2ITemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentConfigUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CONNECT_S2I;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(MIRROR_MAKER)
@Tag(MIRROR_MAKER2)
@Tag(BRIDGE)
@Tag(CONNECT_S2I)
@Tag(CONNECT_COMPONENTS)
@TestMethodOrder(OrderAnnotation.class)
class LogSettingST extends AbstractST {
    static final String NAMESPACE = "log-setting-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LogSettingST.class);

    private static final String INFO = "INFO";
    private static final String ERROR = "ERROR";
    private static final String WARN = "WARN";
    private static final String TRACE = "TRACE";
    private static final String DEBUG = "DEBUG";
    private static final String FATAL = "FATAL";
    private static final String OFF = "OFF";

    private static final String LOG_SETTING_CLUSTER_NAME = "log-setting-cluster-name";
    private static final String GC_LOGGING_SET_NAME = "gc-set-logging";
    private static final String BRIDGE_NAME = "my-bridge";
    private static final String MM_NAME = "my-mirror-maker";
    private static final String MM2_NAME = "my-mirror-maker-2";
    private static final String CONNECT_NAME = "my-connect";
    private static final String CONNECTS2I_NAME = "my-connect-s2i";

    private static final JvmOptions JVM_OPTIONS = new JvmOptionsBuilder()
        .withGcLoggingEnabled(false)
        .build();

    private static final Map<String, String> KAFKA_LOGGERS = new HashMap<String, String>() {
        {
            put("kafka.root.logger.level", INFO);
            put("test.kafka.logger.level", INFO);
            put("log4j.logger.org.I0Itec.zkclient.ZkClient", ERROR);
            put("log4j.logger.org.apache.zookeeper", WARN);
            put("log4j.logger.kafka", TRACE);
            put("log4j.logger.org.apache.kafka", DEBUG);
            put("log4j.logger.kafka.request.logger", FATAL);
            put("log4j.logger.kafka.network.Processor", OFF);

            put("log4j.logger.kafka.server.KafkaApis", INFO);
            put("log4j.logger.kafka.network.RequestChannel$", ERROR);
            put("log4j.logger.kafka.controller", WARN);
            put("log4j.logger.kafka.log.LogCleaner", TRACE);
            put("log4j.logger.state.change.logger", DEBUG);
            put("log4j.logger.kafka.authorizer.logger", FATAL);
        }
    };

    private static final Map<String, String> ZOOKEEPER_LOGGERS = new HashMap<String, String>() {
        {
            put("zookeeper.root.logger", OFF);
            put("test.zookeeper.logger.level", DEBUG);
        }
    };

    private static final Map<String, String> CONNECT_LOGGERS = new HashMap<String, String>() {
        {
            put("connect.root.logger.level", INFO);
            put("test.connect.logger.level", DEBUG);
            put("log4j.logger.org.I0Itec.zkclient", ERROR);
            put("log4j.logger.org.reflections", WARN);
        }
    };

    private static final Map<String, String> OPERATORS_LOGGERS = new HashMap<String, String>() {
        {
            put("rootLogger.level", DEBUG);
            put("test.operator.logger.level", DEBUG);
        }
    };

    private static final Map<String, String> MIRROR_MAKER_LOGGERS = new HashMap<String, String>() {
        {
            put("mirrormaker.root.logger", TRACE);
            put("test.mirrormaker.logger.level", TRACE);
        }
    };

    private static final Map<String, String> BRIDGE_LOGGERS = new HashMap<String, String>() {
        {
            put("logger.createConsumer.name", "http.openapi.operation.createConsumer");
            put("logger.createConsumer.level", INFO);
            put("logger.deleteConsumer.name", "http.openapi.operation.deleteConsumer");
            put("logger.deleteConsumer.level", DEBUG);
            put("logger.subscribe.name", "http.openapi.operation.subscribe");
            put("logger.subscribe.level", TRACE);
            put("logger.unsubscribe.name", "http.openapi.operation.unsubscribe");
            put("logger.unsubscribe.level", DEBUG);
            put("logger.poll.name", "http.openapi.operation.poll");
            put("logger.poll.level", INFO);
            put("logger.assign.name", "http.openapi.operation.assign");
            put("logger.assign.level", TRACE);
            put("logger.commit.name", "http.openapi.operation.commit");
            put("logger.commit.level", DEBUG);
            put("logger.send.name", "http.openapi.operation.send");
            put("logger.send.level", ERROR);
            put("logger.sendToPartition.name", "http.openapi.operation.sendToPartition");
            put("logger.sendToPartition.level", TRACE);
            put("logger.seekToBeginning.name", "http.openapi.operation.seekToBeginning");
            put("logger.seekToBeginning.level", DEBUG);
            put("logger.seekToEnd.name", "http.openapi.operation.seekToEnd");
            put("logger.seekToEnd.level", WARN);
            put("logger.seek.name", "http.openapi.operation.seek");
            put("logger.seek.level", INFO);
            put("logger.healthy.name", "http.openapi.operation.healthy");
            put("logger.healthy.level", ERROR);
            put("logger.ready.name", "http.openapi.operation.ready");
            put("logger.ready.level", WARN);
            put("logger.openapi.name", "http.openapi.operation.openapi");
            put("logger.openapi.level", TRACE);
            put("test.logger.bridge.level", ERROR);
        }
    };

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testKafkaLogSetting(ExtensionContext extensionContext) {
        String kafkaMap = KafkaResources.kafkaMetricsAndLogConfigMapName(LOG_SETTING_CLUSTER_NAME);
        String zookeeperMap = KafkaResources.zookeeperMetricsAndLogConfigMapName(LOG_SETTING_CLUSTER_NAME);
        String topicOperatorMap = String.format("%s-%s", LOG_SETTING_CLUSTER_NAME, "entity-topic-operator-config");
        String userOperatorMap = String.format("%s-%s", LOG_SETTING_CLUSTER_NAME, "entity-user-operator-config");

        String eoDepName = KafkaResources.entityOperatorDeploymentName(LOG_SETTING_CLUSTER_NAME);
        String kafkaSsName = KafkaResources.kafkaStatefulSetName(LOG_SETTING_CLUSTER_NAME);
        String zkSsName = KafkaResources.zookeeperStatefulSetName(LOG_SETTING_CLUSTER_NAME);

        Map<String, String> eoPods = DeploymentUtils.depSnapshot(eoDepName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaSsName);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zkSsName);

        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(LOG_SETTING_CLUSTER_NAME, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(LOG_SETTING_CLUSTER_NAME, userName).build());

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has log level set properly", LOG_SETTING_CLUSTER_NAME);
        assertThat("Kafka's log level is set properly", checkLoggersLevel(KAFKA_LOGGERS, kafkaMap), is(true));
        assertThat("Zookeeper's log level is set properly", checkLoggersLevel(ZOOKEEPER_LOGGERS, zookeeperMap), is(true));
        assertThat("Topic operator's log level is set properly", checkLoggersLevel(OPERATORS_LOGGERS, topicOperatorMap), is(true));
        assertThat("User operator's log level is set properly", checkLoggersLevel(OPERATORS_LOGGERS, userOperatorMap), is(true));

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has GC logging enabled in stateful sets/deployments", LOG_SETTING_CLUSTER_NAME);
        assertThat("Kafka GC logging is not enabled", checkGcLoggingStatefulSets(kafkaSsName), is(true));
        assertThat("Zookeeper GC logging is enabled", checkGcLoggingStatefulSets(zkSsName), is(true));
        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(eoDepName, "topic-operator"), is(true));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(eoDepName, "user-operator"), is(true));

        LOGGER.info("Changing JVM options - setting GC logging to false");
        KafkaResource.replaceKafkaResource(LOG_SETTING_CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getZookeeper().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getEntityOperator().getTopicOperator().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getEntityOperator().getUserOperator().setJvmOptions(JVM_OPTIONS);
        });

        StatefulSetUtils.waitTillSsHasRolled(zkSsName, 1, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(kafkaSsName, 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(eoDepName, 1, eoPods);

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has GC logging disabled in stateful sets/deployments", LOG_SETTING_CLUSTER_NAME);
        assertThat("Kafka GC logging is disabled", checkGcLoggingStatefulSets(kafkaSsName), is(false));
        assertThat("Zookeeper GC logging is disabled", checkGcLoggingStatefulSets(zkSsName), is(false));
        assertThat("TO GC logging is disabled", checkGcLoggingDeployments(eoDepName, "topic-operator"), is(false));
        assertThat("UO GC logging is disabled", checkGcLoggingDeployments(eoDepName, "user-operator"), is(false));

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has GC logging disabled in stateful sets/deployments", GC_LOGGING_SET_NAME);
        assertThat("Kafka GC logging is enabled", checkGcLoggingStatefulSets(kafkaSsName), is(false));
        assertThat("Zookeeper GC logging is enabled", checkGcLoggingStatefulSets(zkSsName), is(false));
        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(eoDepName, "topic-operator"), is(false));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(eoDepName, "user-operator"), is(false));

        kubectlGetStrimzi(LOG_SETTING_CLUSTER_NAME);
        kubectlGetStrimzi(GC_LOGGING_SET_NAME);

        checkContainersHaveProcessOneAsTini(LOG_SETTING_CLUSTER_NAME);
        checkContainersHaveProcessOneAsTini(GC_LOGGING_SET_NAME);
    }

    @ParallelTest
    void testConnectLogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectClusterName = clusterName + "-connect";

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, connectClusterName, LOG_SETTING_CLUSTER_NAME, 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String connectDepName = KafkaConnectResources.deploymentName(connectClusterName);
        Map<String, String> connectPods = DeploymentUtils.depSnapshot(connectDepName);
        String connectMap = KafkaConnectResources.metricsAndLogConfigMapName(connectClusterName);

        LOGGER.info("Checking if Connect has log level set properly");
        assertThat("KafkaConnect's log level is set properly", checkLoggersLevel(CONNECT_LOGGERS, connectMap), is(true));
        assertThat("Connect GC logging is enabled", checkGcLoggingDeployments(connectDepName), is(true));

        KafkaConnectResource.replaceKafkaConnectResource(connectClusterName, kc -> kc.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentUtils.waitTillDepHasRolled(connectDepName, 1, connectPods);
        assertThat("Connect GC logging is disabled", checkGcLoggingDeployments(connectDepName), is(false));

        kubectlGetStrimzi(connectClusterName);
        checkContainersHaveProcessOneAsTini(connectClusterName);
    }

    @ParallelTest
    @OpenShiftOnly
    void testConnectS2ILogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";

        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, LOG_SETTING_CLUSTER_NAME, 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String connectS2IDepName = KafkaConnectS2IResources.deploymentName(connectS2IClusterName);
        Map<String, String> connectS2IPods = DeploymentConfigUtils.depConfigSnapshot(connectS2IDepName);
        String connectS2IMap = KafkaConnectS2IResources.metricsAndLogConfigMapName(connectS2IClusterName);

        LOGGER.info("Checking if ConnectS2I has log level set properly");
        assertThat("KafkaConnectS2I's log level is set properly", checkLoggersLevel(CONNECT_LOGGERS, connectS2IMap), is(true));
        assertThat("ConnectS2I GC logging is enabled", checkGcLoggingDeploymentConfig(connectS2IDepName), is(true));

        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, cs2i -> cs2i.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentConfigUtils.waitTillDepConfigHasRolled(connectS2IDepName, connectS2IPods);
        assertThat("ConnectS2I GC logging is disabled", checkGcLoggingDeploymentConfig(connectS2IDepName), is(false));

        kubectlGetStrimzi(connectS2IClusterName);
        checkContainersHaveProcessOneAsTini(connectS2IClusterName);
    }

    @ParallelTest
    void testMirrorMakerLogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMakerName = clusterName + "-mirror-maker";

        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(mirrorMakerName, LOG_SETTING_CLUSTER_NAME, GC_LOGGING_SET_NAME, "my-group", 1, false)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String mmDepName = KafkaMirrorMakerResources.deploymentName(mirrorMakerName);
        Map<String, String> mmPods = DeploymentUtils.depSnapshot(mmDepName);
        String mirrorMakerMap = KafkaMirrorMakerResources.metricsAndLogConfigMapName(mirrorMakerName);

        LOGGER.info("Checking if MirrorMaker has log level set properly");
        assertThat("KafkaMirrorMaker's log level is set properly", checkLoggersLevel(MIRROR_MAKER_LOGGERS, mirrorMakerMap), is(true));
        assertThat("Mirror-maker GC logging is enabled", checkGcLoggingDeployments(mmDepName), is(true));

        KafkaMirrorMakerResource.replaceMirrorMakerResource(mirrorMakerName, mm -> mm.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentUtils.waitTillDepHasRolled(mmDepName, 1, mmPods);
        assertThat("Mirror-maker GC logging is disabled", checkGcLoggingDeployments(mmDepName), is(false));

        kubectlGetStrimzi(mirrorMakerName);
        checkContainersHaveProcessOneAsTini(mirrorMakerName);
    }

    @ParallelTest
    void testMirrorMaker2LogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMaker2Name = clusterName + "-mirror-maker-2";

        resourceManager.createResource(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(mirrorMaker2Name, LOG_SETTING_CLUSTER_NAME, GC_LOGGING_SET_NAME, 1, false)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String mm2DepName = KafkaMirrorMaker2Resources.deploymentName(mirrorMaker2Name);
        Map<String, String> mm2Pods = DeploymentUtils.depSnapshot(mm2DepName);
        String mirrorMakerMap = KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(mirrorMaker2Name);

        LOGGER.info("Checking if MirrorMaker2 has log level set properly");
        assertThat("KafkaMirrorMaker2's log level is set properly", checkLoggersLevel(MIRROR_MAKER_LOGGERS, mirrorMakerMap), is(true));
        assertThat("Mirror-maker-2 GC logging is enabled", checkGcLoggingDeployments(mm2DepName), is(true));

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(mirrorMaker2Name, mm2 -> mm2.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentUtils.waitTillDepHasRolled(mm2DepName, 1, mm2Pods);
        assertThat("Mirror-maker2 GC logging is disabled", checkGcLoggingDeployments(mm2DepName), is(false));

        kubectlGetStrimzi(mirrorMaker2Name);
        checkContainersHaveProcessOneAsTini(mirrorMaker2Name);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testBridgeLogSetting(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String bridgeName = clusterName + "-bridge";

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(bridgeName, LOG_SETTING_CLUSTER_NAME, KafkaResources.plainBootstrapAddress(LOG_SETTING_CLUSTER_NAME), 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(BRIDGE_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .build());

        String bridgeDepName = KafkaBridgeResources.deploymentName(bridgeName);
        Map<String, String> bridgePods = DeploymentUtils.depSnapshot(bridgeDepName);
        String bridgeMap = KafkaBridgeResources.metricsAndLogConfigMapName(bridgeName);

        LOGGER.info("Checking if Bridge has log level set properly");
        assertThat("Bridge's log level is set properly", checkLoggersLevel(BRIDGE_LOGGERS, bridgeMap), is(true));
        assertThat("Bridge's GC logging is enabled", checkGcLoggingDeployments(bridgeDepName), is(true));

        KafkaBridgeResource.replaceBridgeResource(bridgeName, bridge -> bridge.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentUtils.waitTillDepHasRolled(bridgeDepName, 1, bridgePods);
        assertThat("Bridge GC logging is disabled", checkGcLoggingDeployments(bridgeDepName), is(false));

        kubectlGetStrimzi(bridgeName);
        checkContainersHaveProcessOneAsTini(bridgeName);
    }

    // only one thread can access (eliminate data-race)
    private synchronized void kubectlGetStrimzi(String resourceName) {
        LOGGER.info("Checking if kubectl get strimzi contains {}", resourceName);
        String strimziCRs = cmdKubeClient().execInCurrentNamespace("get", "strimzi").out();
        assertThat(strimziCRs, containsString(resourceName));
    }

    // only one thread can access (eliminate data-race)
    private synchronized void checkContainersHaveProcessOneAsTini(String resourceClusterName) {
        //Used [/] in the grep command so that grep process does not return itself
        String command = "ps -ef | grep '[/]usr/bin/tini' | awk '{ print $2}'";

        for (Pod pod : kubeClient().listPods(Labels.STRIMZI_CLUSTER_LABEL, resourceClusterName)) {
            String podName = pod.getMetadata().getName();
            if (!podName.contains("build") && !podName.contains("deploy") && !podName.contains("kafka-clients")) {
                for (Container container : pod.getSpec().getContainers()) {
                    String containerName = container.getName();

                    PodUtils.waitForPodContainerReady(podName, containerName);
                    LOGGER.info("Checking tini process for pod {} with container {}", podName, containerName);
                    boolean isPresent = cmdKubeClient().execInPodContainer(false, podName, containerName, "/bin/bash", "-c", command).out().trim().equals("1");
                    assertThat(isPresent, is(true));
                }
            }
        }
    }

    private synchronized String configMap(String configMapName) {
        Map<String, String> configMapData = kubeClient().getConfigMap(configMapName).getData();
        // tries to get a log4j2 configuration file first (operator, bridge, ...) otherwise log4j one (kafka, zookeeper, ...)
        String configMapKey = configMapData.keySet()
                .stream()
                .filter(key -> key.equals("log4j2.properties") || key.equals("log4j.properties"))
                .findAny()
                .get();
        return configMapData.get(configMapKey);
    }

    private synchronized boolean checkLoggersLevel(Map<String, String> loggers, String configMapName) {
        boolean result = false;
        String configMap = configMap(configMapName);
        for (Map.Entry<String, String> entry : loggers.entrySet()) {
            LOGGER.info("Check log level setting for logger: {} Expected: {}", entry.getKey(), entry.getValue());
            String loggerConfig = String.format("%s=%s", entry.getKey(), entry.getValue());
            result = configMap.contains(loggerConfig);

            // Validation failed
            if (!result) {
                break;
            }
        }

        return result;
    }

    private synchronized Boolean checkGcLoggingDeployments(String deploymentName, String containerName) {
        LOGGER.info("Checking deployment: {}", deploymentName);
        List<Container> containers = kubeClient().getDeployment(deploymentName).getSpec().getTemplate().getSpec().getContainers();
        Container container = getContainerByName(containerName, containers);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private synchronized Boolean checkGcLoggingDeployments(String deploymentName) {
        LOGGER.info("Checking deployment: {}", deploymentName);
        Container container = kubeClient().getDeployment(deploymentName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private synchronized Boolean checkGcLoggingDeploymentConfig(String depConfName) {
        LOGGER.info("Checking deployment config: {}", depConfName);
        Container container = kubeClient().getDeploymentConfig(depConfName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private synchronized Boolean checkGcLoggingStatefulSets(String statefulSetName) {
        LOGGER.info("Checking stateful set: {}", statefulSetName);
        Container container = kubeClient().getStatefulSet(statefulSetName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private synchronized Container getContainerByName(String containerName, List<Container> containers) {
        return containers.stream().filter(c -> c.getName().equals(containerName)).findFirst().orElse(null);
    }

    private synchronized Boolean checkEnvVarValue(Container container) {
        assertThat("Container is null!", container, is(notNullValue()));

        List<EnvVar> loggingEnvVar = container.getEnv().stream().filter(envVar -> envVar.getName().contains("GC_LOG_ENABLED")).collect(Collectors.toList());
        LOGGER.info("{}={}", loggingEnvVar.get(0).getName(), loggingEnvVar.get(0).getValue());
        return loggingEnvVar.get(0).getValue().contains("true");
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);

        String operationId = timeMeasuringSystem.startOperation(Operation.CLASS_EXECUTION, extensionContext.getRequiredTestClass().getName(), extensionContext.getRequiredTestClass().getName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(LOG_SETTING_CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewInlineLogging()
                        .withLoggers(KAFKA_LOGGERS)
                    .endInlineLogging()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withNewInlineLogging()
                        .withLoggers(ZOOKEEPER_LOGGERS)
                    .endInlineLogging()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(true)
                    .endJvmOptions()
                .endZookeeper()
                .editEntityOperator()
                    .editOrNewUserOperator()
                        .withNewInlineLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLogging()
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
                    .endUserOperator()
                    .editOrNewTopicOperator()
                        .withNewInlineLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLogging()
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(true)
                        .endJvmOptions()
                    .endTopicOperator()
                .endEntityOperator()
                .withNewCruiseControl()
                .endCruiseControl()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build());

//         deploying second Kafka here because of MM and MM2 tests
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(GC_LOGGING_SET_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewJvmOptions()
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withNewJvmOptions()
                    .endJvmOptions()
                .endZookeeper()
                .editEntityOperator()
                    .editTopicOperator()
                        .withNewJvmOptions()
                        .endJvmOptions()
                    .endTopicOperator()
                    .editUserOperator()
                        .withNewJvmOptions()
                        .endJvmOptions()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        String kafkaClientsName = "shared" + "-" + Constants.KAFKA_CLIENTS;
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
    }

    @AfterAll
    protected void tearDownEnvironmentAfterAll() {
        teardownEnvForOperator();
    }
}
