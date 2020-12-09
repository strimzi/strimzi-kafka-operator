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
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentConfigUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

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

    private static final String GC_LOGGING_SET_NAME = "gc-set-logging";
    private static final String BRIDGE_NAME = "my-bridge";
    private static final String MM_NAME = "my-mirror-maker";
    private static final String MM2_NAME = "my-mirror-maker-2";
    private static final String CONNECT_NAME = "my-connect";
    private static final String CONNECTS2I_NAME = "my-connect-s2i";

    private static final String KAFKA_MAP = KafkaResources.kafkaMetricsAndLogConfigMapName(CLUSTER_NAME);
    private static final String ZOOKEEPER_MAP = KafkaResources.zookeeperMetricsAndLogConfigMapName(CLUSTER_NAME);
    private static final String TO_MAP = String.format("%s-%s", CLUSTER_NAME, "entity-topic-operator-config");
    private static final String UO_MAP = String.format("%s-%s", CLUSTER_NAME, "entity-user-operator-config");
    private static final String CONNECT_MAP = KafkaConnectResources.metricsAndLogConfigMapName(CONNECT_NAME);
    private static final String CONNECTS2I_MAP = KafkaConnectS2IResources.metricsAndLogConfigMapName(CONNECTS2I_NAME);
    private static final String MM_MAP = KafkaMirrorMakerResources.metricsAndLogConfigMapName(MM_NAME);
    private static final String MM2_MAP = KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(MM2_NAME);
    private static final String BRIDGE_MAP = KafkaBridgeResources.metricsAndLogConfigMapName(BRIDGE_NAME);

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

    @Test
    void testKafkaLogSetting() {
        String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);
        String kafkaSsName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        String zkSsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);

        Map<String, String> eoPods = DeploymentUtils.depSnapshot(eoDepName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaSsName);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zkSsName);

        String userName = "test-user";
        String topicName = "test-topic";

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has log level set properly", CLUSTER_NAME);
        assertThat("Kafka's log level is set properly", checkLoggersLevel(KAFKA_LOGGERS, KAFKA_MAP), is(true));
        assertThat("Zookeeper's log level is set properly", checkLoggersLevel(ZOOKEEPER_LOGGERS, ZOOKEEPER_MAP), is(true));
        assertThat("Topic operator's log level is set properly", checkLoggersLevel(OPERATORS_LOGGERS, TO_MAP), is(true));
        assertThat("User operator's log level is set properly", checkLoggersLevel(OPERATORS_LOGGERS, UO_MAP), is(true));

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has GC logging enabled in stateful sets/deployments", CLUSTER_NAME);
        assertThat("Kafka GC logging is not enabled", checkGcLoggingStatefulSets(kafkaSsName), is(true));
        assertThat("Zookeeper GC logging is enabled", checkGcLoggingStatefulSets(zkSsName), is(true));
        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(eoDepName, "topic-operator"), is(true));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(eoDepName, "user-operator"), is(true));

        LOGGER.info("Changing JVM options - setting GC logging to false");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getZookeeper().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getEntityOperator().getTopicOperator().setJvmOptions(JVM_OPTIONS);
            kafka.getSpec().getEntityOperator().getUserOperator().setJvmOptions(JVM_OPTIONS);
        });

        StatefulSetUtils.waitTillSsHasRolled(zkSsName, 1, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(kafkaSsName, 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(eoDepName, 1, eoPods);

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has GC logging disabled in stateful sets/deployments", CLUSTER_NAME);
        assertThat("Kafka GC logging is disabled", checkGcLoggingStatefulSets(kafkaSsName), is(false));
        assertThat("Zookeeper GC logging is disabled", checkGcLoggingStatefulSets(zkSsName), is(false));
        assertThat("TO GC logging is disabled", checkGcLoggingDeployments(eoDepName, "topic-operator"), is(false));
        assertThat("UO GC logging is disabled", checkGcLoggingDeployments(eoDepName, "user-operator"), is(false));

        LOGGER.info("Checking if Kafka, Zookeeper, TO and UO of cluster:{} has GC logging disabled in stateful sets/deployments", GC_LOGGING_SET_NAME);
        assertThat("Kafka GC logging is enabled", checkGcLoggingStatefulSets(kafkaSsName), is(false));
        assertThat("Zookeeper GC logging is enabled", checkGcLoggingStatefulSets(zkSsName), is(false));
        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(eoDepName, "topic-operator"), is(false));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(eoDepName, "user-operator"), is(false));

        kubectlGetStrimzi(CLUSTER_NAME);
        kubectlGetStrimzi(GC_LOGGING_SET_NAME);

        checkContainersHaveProcessOneAsTini(CLUSTER_NAME);
        checkContainersHaveProcessOneAsTini(GC_LOGGING_SET_NAME);
    }

    @Test
    void testConnectLogSetting() {
        KafkaConnectResource.kafkaConnect(CONNECT_NAME, CLUSTER_NAME, 1, true)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .done();

        String connectDepName = KafkaConnectResources.deploymentName(CONNECT_NAME);
        Map<String, String> connectPods = DeploymentUtils.depSnapshot(connectDepName);

        LOGGER.info("Checking if Connect has log level set properly");
        assertThat("KafkaConnect's log level is set properly", checkLoggersLevel(CONNECT_LOGGERS, CONNECT_MAP), is(true));
        assertThat("Connect GC logging is enabled", checkGcLoggingDeployments(connectDepName), is(true));

        KafkaConnectResource.replaceKafkaConnectResource(CONNECT_NAME, kc -> kc.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentUtils.waitTillDepHasRolled(connectDepName, 1, connectPods);
        assertThat("Connect GC logging is disabled", checkGcLoggingDeployments(connectDepName), is(false));

        kubectlGetStrimzi(CONNECT_NAME);
        checkContainersHaveProcessOneAsTini(CONNECT_NAME);
    }

    @Test
    @OpenShiftOnly
    void testConnectS2ILogSetting() {
        KafkaConnectS2IResource.kafkaConnectS2I(CONNECTS2I_NAME, CLUSTER_NAME, 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .done();

        String connectS2IDepName = KafkaConnectS2IResources.deploymentName(CONNECTS2I_NAME);
        Map<String, String> connectS2IPods = DeploymentConfigUtils.depConfigSnapshot(connectS2IDepName);

        LOGGER.info("Checking if ConnectS2I has log level set properly");
        assertThat("KafkaConnectS2I's log level is set properly", checkLoggersLevel(CONNECT_LOGGERS, CONNECTS2I_MAP), is(true));
        assertThat("ConnectS2I GC logging is enabled", checkGcLoggingDeploymentConfig(connectS2IDepName), is(true));

        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECTS2I_NAME, cs2i -> cs2i.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentConfigUtils.waitTillDepConfigHasRolled(connectS2IDepName, connectS2IPods);
        assertThat("ConnectS2I GC logging is disabled", checkGcLoggingDeploymentConfig(connectS2IDepName), is(false));

        kubectlGetStrimzi(CONNECTS2I_NAME);
        checkContainersHaveProcessOneAsTini(CONNECTS2I_NAME);
    }

    @Test
    void testMirrorMakerLogSetting() {
        KafkaMirrorMakerResource.kafkaMirrorMaker(MM_NAME, CLUSTER_NAME, GC_LOGGING_SET_NAME, "my-group", 1, false)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .done();

        String mmDepName = KafkaMirrorMakerResources.deploymentName(MM_NAME);
        Map<String, String> mmPods = DeploymentUtils.depSnapshot(mmDepName);

        LOGGER.info("Checking if MirrorMaker has log level set properly");
        assertThat("KafkaMirrorMaker's log level is set properly", checkLoggersLevel(MIRROR_MAKER_LOGGERS, MM_MAP), is(true));
        assertThat("Mirror-maker GC logging is enabled", checkGcLoggingDeployments(mmDepName), is(true));

        KafkaMirrorMakerResource.replaceMirrorMakerResource(MM_NAME, mm -> mm.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentUtils.waitTillDepHasRolled(mmDepName, 1, mmPods);
        assertThat("Mirror-maker GC logging is disabled", checkGcLoggingDeployments(mmDepName), is(false));

        kubectlGetStrimzi(MM_NAME);
        checkContainersHaveProcessOneAsTini(MM_NAME);
    }

    @Test
    void testMirrorMaker2LogSetting() {
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(MM2_NAME, CLUSTER_NAME, GC_LOGGING_SET_NAME, 1, false)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .done();

        String mm2DepName = KafkaMirrorMaker2Resources.deploymentName(MM2_NAME);
        Map<String, String> mm2Pods = DeploymentUtils.depSnapshot(mm2DepName);

        LOGGER.info("Checking if MirrorMaker2 has log level set properly");
        assertThat("KafkaMirrorMaker2's log level is set properly", checkLoggersLevel(MIRROR_MAKER_LOGGERS, MM2_MAP), is(true));
        assertThat("Mirror-maker-2 GC logging is enabled", checkGcLoggingDeployments(mm2DepName), is(true));

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(MM2_NAME, mm2 -> mm2.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentUtils.waitTillDepHasRolled(mm2DepName, 1, mm2Pods);
        assertThat("Mirror-maker2 GC logging is disabled", checkGcLoggingDeployments(mm2DepName), is(false));

        kubectlGetStrimzi(MM2_NAME);
        checkContainersHaveProcessOneAsTini(MM2_NAME);
    }

    @Test
    void testBridgeLogSetting() {
        KafkaBridgeResource.kafkaBridge(BRIDGE_NAME, CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(BRIDGE_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec()
            .done();

        String bridgeDepName = KafkaBridgeResources.deploymentName(BRIDGE_NAME);
        Map<String, String> bridgePods = DeploymentUtils.depSnapshot(bridgeDepName);

        LOGGER.info("Checking if Bridge has log level set properly");
        assertThat("Bridge's log level is set properly", checkLoggersLevel(BRIDGE_LOGGERS, BRIDGE_MAP), is(true));
        assertThat("Bridge's GC logging is enabled", checkGcLoggingDeployments(bridgeDepName), is(true));

        KafkaBridgeResource.replaceBridgeResource(BRIDGE_NAME, bridge -> bridge.getSpec().setJvmOptions(JVM_OPTIONS));
        DeploymentUtils.waitTillDepHasRolled(bridgeDepName, 1, bridgePods);
        assertThat("Bridge GC logging is disabled", checkGcLoggingDeployments(bridgeDepName), is(false));

        kubectlGetStrimzi(BRIDGE_NAME);
        checkContainersHaveProcessOneAsTini(BRIDGE_NAME);
    }

    void kubectlGetStrimzi(String resourceName) {
        LOGGER.info("Checking if kubectl get strimzi contains {}", resourceName);
        String strimziCRs = cmdKubeClient().execInCurrentNamespace("get", "strimzi").out();
        assertThat(strimziCRs, containsString(resourceName));
    }

    void checkContainersHaveProcessOneAsTini(String resourceClusterName) {
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

    private String configMap(String configMapName) {
        Map<String, String> configMapData = kubeClient().getConfigMap(configMapName).getData();
        // tries to get a log4j2 configuration file first (operator, bridge, ...) otherwise log4j one (kafka, zookeeper, ...)
        String configMapKey = configMapData.keySet()
                .stream()
                .filter(key -> key.equals("log4j2.properties") || key.equals("log4j.properties"))
                .findAny()
                .get();
        return configMapData.get(configMapKey);
    }

    private boolean checkLoggersLevel(Map<String, String> loggers, String configMapName) {
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

    private Boolean checkGcLoggingDeployments(String deploymentName, String containerName) {
        LOGGER.info("Checking deployment: {}", deploymentName);
        List<Container> containers = kubeClient().getDeployment(deploymentName).getSpec().getTemplate().getSpec().getContainers();
        Container container = getContainerByName(containerName, containers);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private Boolean checkGcLoggingDeployments(String deploymentName) {
        LOGGER.info("Checking deployment: {}", deploymentName);
        Container container = kubeClient().getDeployment(deploymentName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private Boolean checkGcLoggingDeploymentConfig(String depConfName) {
        LOGGER.info("Checking deployment config: {}", depConfName);
        Container container = kubeClient().getDeploymentConfig(depConfName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private Boolean checkGcLoggingStatefulSets(String statefulSetName) {
        LOGGER.info("Checking stateful set: {}", statefulSetName);
        Container container = kubeClient().getStatefulSet(statefulSetName).getSpec().getTemplate().getSpec().getContainers().get(0);
        LOGGER.info("Checking container with name: {}", container.getName());
        return checkEnvVarValue(container);
    }

    private Container getContainerByName(String containerName, List<Container> containers) {
        return containers.stream().filter(c -> c.getName().equals(containerName)).findFirst().orElse(null);
    }

    private Boolean checkEnvVarValue(Container container) {
        assertThat("Container is null!", container, is(notNullValue()));

        List<EnvVar> loggingEnvVar = container.getEnv().stream().filter(envVar -> envVar.getName().contains("GC_LOG_ENABLED")).collect(Collectors.toList());
        LOGGER.info("{}={}", loggingEnvVar.get(0).getName(), loggingEnvVar.get(0).getValue());
        return loggingEnvVar.get(0).getValue().contains("true");
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        timeMeasuringSystem.setOperationID(startDeploymentMeasuring());

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 1)
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
            .done();

        // deploying second Kafka here because of MM and MM2 tests
        KafkaResource.kafkaPersistent(GC_LOGGING_SET_NAME, 1, 1)
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
            .done();

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();
    }

    private String startDeploymentMeasuring() {
        timeMeasuringSystem.setTestName(testClass, testClass);
        return timeMeasuringSystem.startOperation(Operation.CLASS_EXECUTION);
    }

    @Override
    protected void tearDownEnvironmentAfterAll() {
        teardownEnvForOperator();
    }
}
