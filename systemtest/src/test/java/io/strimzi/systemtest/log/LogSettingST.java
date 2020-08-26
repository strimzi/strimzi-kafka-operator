/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.log;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
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
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
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
    @Order(1)
    void testLoggersKafka() {
        assertThat("Kafka's log level is set properly", checkLoggersLevel(KAFKA_LOGGERS, KAFKA_MAP), is(true));
    }

    @Test
    @Order(2)
    void testLoggersZookeeper() {
        assertThat("Zookeeper's log level is set properly", checkLoggersLevel(ZOOKEEPER_LOGGERS, ZOOKEEPER_MAP), is(true));
    }

    @Test
    @Order(3)
    void testLoggersTO() {
        assertThat("Topic operator's log level is set properly", checkLoggersLevel(OPERATORS_LOGGERS, TO_MAP), is(true));
    }

    @Test
    @Order(4)
    void testLoggersUO() {
        assertThat("User operator's log level is set properly", checkLoggersLevel(OPERATORS_LOGGERS, UO_MAP), is(true));
    }

    @Test
    @Order(5)
    void testLoggersKafkaConnect() {
        assertThat("Kafka connect's log level is set properly", checkLoggersLevel(CONNECT_LOGGERS, CONNECT_MAP), is(true));
    }

    @Test
    @Order(6)
    void testLoggersMirrorMaker() {
        assertThat("KafkaMirrorMaker's log level is set properly", checkLoggersLevel(MIRROR_MAKER_LOGGERS, MM_MAP), is(true));
    }

    @Test
    @Order(7)
    void testLoggersMirrorMaker2() {
        assertThat("KafkaMirrorMaker2's log level is set properly", checkLoggersLevel(MIRROR_MAKER_LOGGERS, MM2_MAP), is(true));
    }

    @Test
    @Order(8)
    void testLoggersBridge() {
        assertThat("Bridge's log level is set properly", checkLoggersLevel(BRIDGE_LOGGERS, BRIDGE_MAP), is(true));
    }

    @Test
    @OpenShiftOnly
    @Order(9)
    void testLoggersConnectS2I() {
        assertThat("KafkaConnectS2I's log level is set properly", checkLoggersLevel(CONNECT_LOGGERS, CONNECTS2I_MAP), is(true));
    }

    @Test
    @Order(10)
    void testGcLoggingNonSetDisabled() {
        assertThat("Kafka GC logging is enabled", checkGcLoggingStatefulSets(KafkaResources.kafkaStatefulSetName(GC_LOGGING_SET_NAME)), is(false));
        assertThat("Zookeeper GC logging is enabled", checkGcLoggingStatefulSets(KafkaResources.zookeeperStatefulSetName(GC_LOGGING_SET_NAME)), is(false));

        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(KafkaResources.entityOperatorDeploymentName(GC_LOGGING_SET_NAME), "topic-operator"), is(false));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(KafkaResources.entityOperatorDeploymentName(GC_LOGGING_SET_NAME), "user-operator"), is(false));
    }

    @Test
    @Order(11)
    void testGcLoggingSetEnabled() {
        assertThat("Kafka GC logging is enabled", checkGcLoggingStatefulSets(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)), is(true));
        assertThat("Zookeeper GC logging is enabled", checkGcLoggingStatefulSets(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)), is(true));

        assertThat("TO GC logging is enabled", checkGcLoggingDeployments(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "topic-operator"), is(true));
        assertThat("UO GC logging is enabled", checkGcLoggingDeployments(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "user-operator"), is(true));

        assertThat("Connect GC logging is enabled", checkGcLoggingDeployments(KafkaConnectResources.deploymentName(CONNECT_NAME)), is(true));
        assertThat("Mirror-maker GC logging is enabled", checkGcLoggingDeployments(KafkaMirrorMakerResources.deploymentName(MM_NAME)), is(true));
        assertThat("Mirror-maker-2 GC logging is enabled", checkGcLoggingDeployments(KafkaMirrorMaker2Resources.deploymentName(MM2_NAME)), is(true));

        if (cluster.isNotKubernetes()) {
            assertThat("ConnectS2I GC logging is enabled", checkGcLoggingDeploymentConfig(KafkaConnectS2IResources.deploymentName(CONNECTS2I_NAME)), is(true));
        }
    }

    @Test
    @Order(12)
    void testGcLoggingSetDisabled() {
        String connectName = KafkaConnectResources.deploymentName(CONNECT_NAME);
        String connectS2IName = KafkaConnectS2IResources.deploymentName(CONNECTS2I_NAME);
        String mmName = KafkaMirrorMakerResources.deploymentName(MM_NAME);
        String mm2Name = KafkaMirrorMaker2Resources.deploymentName(MM2_NAME);
        String eoName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);
        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        String zkName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        Map<String, String> connectPods = DeploymentUtils.depSnapshot(connectName);
        Map<String, String> mmPods = DeploymentUtils.depSnapshot(mmName);
        Map<String, String> mm2Pods = DeploymentUtils.depSnapshot(mm2Name);
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(eoName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zkName);

        JvmOptions jvmOptions = new JvmOptions();
        jvmOptions.setGcLoggingEnabled(false);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getKafka().setJvmOptions(jvmOptions);
            k.getSpec().getZookeeper().setJvmOptions(jvmOptions);
            k.getSpec().getEntityOperator().getTopicOperator().setJvmOptions(jvmOptions);
            k.getSpec().getEntityOperator().getUserOperator().setJvmOptions(jvmOptions);
        });

        StatefulSetUtils.waitTillSsHasRolled(zkName, 1, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(kafkaName, 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(eoName, 1, eoPods);

        KafkaConnectResource.replaceKafkaConnectResource(CONNECT_NAME, kc -> kc.getSpec().setJvmOptions(jvmOptions));
        DeploymentUtils.waitTillDepHasRolled(connectName, 1, connectPods);

        if (cluster.isNotKubernetes()) {
            KafkaConnectS2IResource.replaceConnectS2IResource(CONNECTS2I_NAME, cs2i -> cs2i.getSpec().setJvmOptions(jvmOptions));
            DeploymentConfigUtils.waitTillDepConfigHasRolled(connectS2IName, connectPods);
        }

        KafkaMirrorMakerResource.replaceMirrorMakerResource(MM_NAME, mm -> mm.getSpec().setJvmOptions(jvmOptions));
        DeploymentUtils.waitTillDepHasRolled(mmName, 1, mmPods);

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(MM2_NAME, mm2 -> mm2.getSpec().setJvmOptions(jvmOptions));
        DeploymentUtils.waitTillDepHasRolled(mm2Name, 1, mm2Pods);

        assertThat("Kafka GC logging is disabled", checkGcLoggingStatefulSets(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)), is(false));
        assertThat("Zookeeper GC logging is disabled", checkGcLoggingStatefulSets(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)), is(false));

        assertThat("TO GC logging is disabled", checkGcLoggingDeployments(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "topic-operator"), is(false));
        assertThat("UO GC logging is disabled", checkGcLoggingDeployments(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "user-operator"), is(false));

        assertThat("Connect GC logging is disabled", checkGcLoggingDeployments(KafkaConnectResources.deploymentName(CONNECT_NAME)), is(false));
        assertThat("Mirror-maker GC logging is disabled", checkGcLoggingDeployments(KafkaMirrorMakerResources.deploymentName(MM_NAME)), is(false));
        assertThat("Mirror-maker2 GC logging is disabled", checkGcLoggingDeployments(KafkaMirrorMaker2Resources.deploymentName(MM2_NAME)), is(false));

        if (cluster.isNotKubernetes()) {
            assertThat("ConnectS2I GC logging is disabled", checkGcLoggingDeploymentConfig(KafkaConnectS2IResources.deploymentName(CONNECTS2I_NAME)), is(false));
        }
    }

    @Test
    @Order(13)
    void testKubectlGetStrimzi() {
        String userName = "test-user";
        String topicName = "test-topic";

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        String strimziCRs = cmdKubeClient().execInCurrentNamespace("get", "strimzi").out();

        assertThat(strimziCRs, containsString(CLUSTER_NAME));
        assertThat(strimziCRs, containsString(GC_LOGGING_SET_NAME));
        assertThat(strimziCRs, containsString(MM_NAME));
        assertThat(strimziCRs, containsString(MM2_NAME));
        assertThat(strimziCRs, containsString(BRIDGE_NAME));
        assertThat(strimziCRs, containsString(CONNECT_NAME));
        assertThat(strimziCRs, containsString(userName));
        assertThat(strimziCRs, containsString(topicName));
    }

    @Test
    @Order(14)
    void testCheckContainersHaveProcessOneAsTini() {
        //Used [/] in the grep command so that grep process does not return itself
        String command = "ps -ef | grep '[/]usr/bin/tini' | awk '{ print $2}'";

        for (Pod pod : kubeClient().listPods()) {
            String podName = pod.getMetadata().getName();
            if (!podName.contains("build") && !podName.contains("deploy") && !podName.contains("kafka-clients") && !podName.contains(CONNECTS2I_NAME)) {
                for (Container container : pod.getSpec().getContainers()) {
                    LOGGER.info("Checking tini process for pod {} with container {}", pod, container);
                    boolean isPresent = cmdKubeClient().execInPodContainer(false, podName, container.getName(), "/bin/bash", "-c", command).out().trim().equals("1");
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

        KafkaConnectResource.kafkaConnect(CONNECT_NAME, CLUSTER_NAME, 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLogging()
                .withNewJvmOptions()
                    .withGcLoggingEnabled(true)
                .endJvmOptions()
            .endSpec().done();

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

        KafkaBridgeResource.kafkaBridge(BRIDGE_NAME, CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(BRIDGE_LOGGERS)
                .endInlineLogging()
            .endSpec().done();

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

        if (cluster.isNotKubernetes()) {
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
        }
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
