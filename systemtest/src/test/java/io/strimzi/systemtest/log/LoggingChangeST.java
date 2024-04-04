/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.log;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ExternalLogging;
import io.strimzi.api.kafka.model.common.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.InlineLoggingBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.ROLLING_UPDATE;
import static io.strimzi.systemtest.TestConstants.STRIMZI_DEPLOYMENT_NAME;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
class LoggingChangeST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(LoggingChangeST.class);

    private static final Pattern DEFAULT_LOG4J_PATTERN = Pattern.compile("^(?<date>[\\d-]+) (?<time>[\\d:,]+) (?<status>\\w+) (?<message>.+)");

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testJSONFormatLogging() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // In this test scenario we change configuration for CO and we have to be sure, that CO is installed via YAML bundle instead of helm or OLM
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        String loggersConfigKafka = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
            "log4j.appender.CONSOLE.layout=net.logstash.log4j.JSONEventLayoutV1\n" +
            "kafka.root.logger.level=INFO\n" +
            "log4j.rootLogger=${kafka.root.logger.level}, CONSOLE\n" +
            "log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n" +
            "log4j.logger.org.apache.zookeeper=INFO\n" +
            "log4j.logger.kafka=INFO\n" +
            "log4j.logger.org.apache.kafka=INFO\n" +
            "log4j.logger.kafka.request.logger=WARN, CONSOLE\n" +
            "log4j.logger.kafka.network.Processor=OFF\n" +
            "log4j.logger.kafka.server.KafkaApis=OFF\n" +
            "log4j.logger.kafka.network.RequestChannel$=WARN\n" +
            "log4j.logger.kafka.controller=TRACE\n" +
            "log4j.logger.kafka.log.LogCleaner=INFO\n" +
            "log4j.logger.state.change.logger=TRACE\n" +
            "log4j.logger.kafka.authorizer.logger=INFO";

        String loggersConfigOperators = "appender.console.type=Console\n" +
            "appender.console.name=STDOUT\n" +
            "appender.console.layout.type=JsonLayout\n" +
            "rootLogger.level=INFO\n" +
            "rootLogger.appenderRefs=stdout\n" +
            "rootLogger.appenderRef.console.ref=STDOUT\n" +
            "rootLogger.additivity=false";

        String loggersConfigZookeeper = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
            "log4j.appender.CONSOLE.layout=net.logstash.log4j.JSONEventLayoutV1\n" +
            "zookeeper.root.logger=INFO\n" +
            "log4j.rootLogger=${zookeeper.root.logger}, CONSOLE";

        String loggersConfigCO = "name = COConfig\n" +
            "appender.console.type = Console\n" +
            "appender.console.name = STDOUT\n" +
            "appender.console.layout.type = JsonLayout\n" +
            "rootLogger.level = ${env:STRIMZI_LOG_LEVEL:-INFO}\n" +
            "rootLogger.appenderRefs = stdout\n" +
            "rootLogger.appenderRef.console.ref = STDOUT\n" +
            "rootLogger.additivity = false\n" +
            "logger.kafka.name = org.apache.kafka\n" +
            "logger.kafka.level = ${env:STRIMZI_AC_LOG_LEVEL:-WARN}\n" +
            "logger.kafka.additivity = false";

        String configMapOpName = "json-layout-operators";
        String configMapZookeeperName = "json-layout-zookeeper";
        String configMapKafkaName = "json-layout-kafka";
        String configMapCOName = TestConstants.STRIMZI_DEPLOYMENT_NAME;

        String originalCoLoggers = kubeClient().getClient().configMaps()
            .inNamespace(clusterOperator.getDeploymentNamespace()).withName(configMapCOName).get().getData().get("log4j2.properties");

        ConfigMap configMapKafka = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapKafkaName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addToData("log4j.properties", loggersConfigKafka)
            .build();

        ConfigMapKeySelector kafkaLoggingCMselector = new ConfigMapKeySelectorBuilder()
                .withName(configMapKafkaName)
                .withKey("log4j.properties")
                .build();

        ConfigMap configMapOperators = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapOpName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addToData("log4j2.properties", loggersConfigOperators)
            .build();

        ConfigMapKeySelector operatorsLoggimgCMselector = new ConfigMapKeySelectorBuilder()
                .withName(configMapOpName)
                .withKey("log4j2.properties")
                .build();

        ConfigMap configMapZookeeper = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapZookeeperName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addToData("log4j-custom.properties", loggersConfigZookeeper)
            .build();

        ConfigMapKeySelector zkLoggingCMselector = new ConfigMapKeySelectorBuilder()
                .withName(configMapZookeeperName)
                .withKey("log4j-custom.properties")
                .build();

        ConfigMap configMapCO = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapCOName)
                // we are using this namespace because CO is deployed @BeforeAll
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .addToData("log4j2.properties", loggersConfigCO)
            .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMapKafka);
        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMapOperators);
        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMapZookeeper);
        kubeClient().updateConfigMapInNamespace(clusterOperator.getDeploymentNamespace(), configMapCO);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
            .editOrNewSpec()
                .editKafka()
                    //.withLogging(new ExternalLoggingBuilder().withName(configMapKafkaName).build())
                    .withLogging(new ExternalLoggingBuilder()
                            .withNewValueFrom()
                                .withConfigMapKeyRef(kafkaLoggingCMselector)
                            .endValueFrom()
                            .build())
                .endKafka()
                .editZookeeper()
                    .withLogging(new ExternalLoggingBuilder()
                            .withNewValueFrom()
                                .withConfigMapKeyRef(zkLoggingCMselector)
                            .endValueFrom()
                            .build())
                .endZookeeper()
                .editEntityOperator()
                    .editTopicOperator()
                        .withLogging(new ExternalLoggingBuilder()
                                .withNewValueFrom()
                                    .withConfigMapKeyRef(operatorsLoggimgCMselector)
                                .endValueFrom()
                                .build())
                    .endTopicOperator()
                    .editUserOperator()
                        .withLogging(new ExternalLoggingBuilder()
                                .withNewValueFrom()
                                    .withConfigMapKeyRef(operatorsLoggimgCMselector)
                                .endValueFrom()
                                .build())
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build();

        if (Environment.isKRaftModeEnabled()) {
            kafka.getSpec().setZookeeper(null);
        }

        resourceManager.createResourceWithWait(kafka);

        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()));
        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName());

        StUtils.checkLogForJSONFormat(clusterOperator.getDeploymentNamespace(), operatorSnapshot, ResourceManager.getCoDeploymentName());
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), brokerPods, "");
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), controllerPods, "");
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), eoPods, "topic-operator");
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), eoPods, "user-operator");

        // set loggers of CO back to original
        configMapCO.getData().put("log4j2.properties", originalCoLoggers);
        kubeClient().updateConfigMapInNamespace(clusterOperator.getDeploymentNamespace(), configMapCO);
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    void testDynamicallySetEOloggingLevels() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        InlineLogging ilOff = new InlineLogging();
        ilOff.setLoggers(Collections.singletonMap("rootLogger.level", "OFF"));

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withLogging(ilOff)
                    .endTopicOperator()
                    .editUserOperator()
                        .withLogging(ilOff)
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        String eoDeploymentName = KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName());
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), eoDeploymentName);

        final String eoPodName = eoPods.keySet().iterator().next();

        LOGGER.info("Checking if EO Pod contains any log (except configuration)");
        assertFalse(DEFAULT_LOG4J_PATTERN.matcher(StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), eoPodName, "user-operator", "30s")).find());

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        ilDebug.setLoggers(Collections.singletonMap("rootLogger.level", "DEBUG"));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setLogging(ilDebug);
            k.getSpec().getEntityOperator().getUserOperator().setLogging(ilDebug);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                        && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String uoLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), eoPodName, "user-operator", "30s");
                String toLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), eoPodName, "topic-operator", "30s");

                return uoLog != null && toLog != null &&
                    !(uoLog.isEmpty() && toLog.isEmpty()) &&
                    DEFAULT_LOG4J_PATTERN.matcher(uoLog).find() &&
                    DEFAULT_LOG4J_PATTERN.matcher(toLog).find();
            });

        LOGGER.info("Setting external logging OFF");
        ConfigMap configMapTo = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("external-configmap-to")
            .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(Collections.singletonMap("log4j2.properties", "name=TOConfig\n" +
                "appender.console.type=Console\n" +
                "appender.console.name=STDOUT\n" +
                "appender.console.layout.type=PatternLayout\n" +
                "appender.console.layout.pattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n\n" +
                "rootLogger.level=OFF\n" +
                "rootLogger.appenderRefs=stdout\n" +
                "rootLogger.appenderRef.console.ref=STDOUT\n" +
                "rootLogger.additivity=false"))
            .build();

        ConfigMap configMapUo = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("external-configmap-uo")
            .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addToData(Collections.singletonMap("log4j2.properties", "name=UOConfig\n" +
                "appender.console.type=Console\n" +
                "appender.console.name=STDOUT\n" +
                "appender.console.layout.type=PatternLayout\n" +
                "appender.console.layout.pattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n\n" +
                "rootLogger.level=OFF\n" +
                "rootLogger.appenderRefs=stdout\n" +
                "rootLogger.appenderRef.console.ref=STDOUT\n" +
                "rootLogger.additivity=false"))
            .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMapTo);
        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMapUo);

        ExternalLogging elTo = new ExternalLoggingBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(
                    new ConfigMapKeySelectorBuilder()
                        .withName("external-configmap-to")
                        .withKey("log4j2.properties")
                        .build()
                )
            .endValueFrom()
            .build();

        ExternalLogging elUo = new ExternalLoggingBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(
                    new ConfigMapKeySelectorBuilder()
                        .withName("external-configmap-uo")
                        .withKey("log4j2.properties")
                        .build()
                )
            .endValueFrom()
            .build();

        LOGGER.info("Setting log level of TO and UO to OFF - records should not appear in log");
        // change to external logging
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setLogging(elTo);
            k.getSpec().getEntityOperator().getUserOperator().setLogging(elUo);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                    && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                    && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
                    && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String uoLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), eoPodName, "user-operator", "30s");
                String toLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), eoPodName, "topic-operator", "30s");

                return uoLog != null && toLog != null &&
                    uoLog.isEmpty() && toLog.isEmpty() &&
                    !(DEFAULT_LOG4J_PATTERN.matcher(uoLog).find() && DEFAULT_LOG4J_PATTERN.matcher(toLog).find());
            });

        LOGGER.info("Setting external logging OFF");
        configMapTo = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("external-configmap-to")
            .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(Collections.singletonMap("log4j2.properties", "name=TOConfig\n" +
                "appender.console.type=Console\n" +
                "appender.console.name=STDOUT\n" +
                "appender.console.layout.type=PatternLayout\n" +
                "appender.console.layout.pattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n\n" +
                "rootLogger.level=DEBUG\n" +
                "rootLogger.appenderRefs=stdout\n" +
                "rootLogger.appenderRef.console.ref=STDOUT\n" +
                "rootLogger.additivity=false"))
            .build();

        configMapUo = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("external-configmap-uo")
            .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addToData(Collections.singletonMap("log4j2.properties", "name=UOConfig\n" +
                "appender.console.type=Console\n" +
                "appender.console.name=STDOUT\n" +
                "appender.console.layout.type=PatternLayout\n" +
                "appender.console.layout.pattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n\n" +
                "rootLogger.level=DEBUG\n" +
                "rootLogger.appenderRefs=stdout\n" +
                "rootLogger.appenderRef.console.ref=STDOUT\n" +
                "rootLogger.additivity=false"))
            .build();

        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), configMapTo);
        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), configMapUo);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                        && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String uoLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), eoPodName, "user-operator", "30s");
                String toLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), eoPodName, "topic-operator", "30s");

                return uoLog != null && toLog != null &&
                    !(uoLog.isEmpty() && toLog.isEmpty()) &&
                    DEFAULT_LOG4J_PATTERN.matcher(uoLog).find() &&
                    DEFAULT_LOG4J_PATTERN.matcher(toLog).find();
            });

        assertThat("EO Pod should not roll", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), eoDeploymentName), equalTo(eoPods));
    }

    @ParallelNamespaceTest
    @Tag(BRIDGE)
    @Tag(ROLLING_UPDATE)
    void testDynamicallySetBridgeLoggingLevels() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        InlineLogging ilOff = new InlineLogging();
        Map<String, String> loggers = new HashMap<>();
        loggers.put("rootLogger.level", "OFF");
        loggers.put("logger.bridge.level", "OFF");
        loggers.put("logger.healthy.level", "OFF");
        loggers.put("logger.ready.level", "OFF");
        ilOff.setLoggers(loggers);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        // create resources async
        resourceManager.createResourceWithoutWait(
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaBridgeTemplates.kafkaBridge(testStorage.getClusterName(), KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()), 1)
                .editSpec()
                    .withLogging(ilOff)
                .endSpec()
                .build());

        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());
        DeploymentUtils.waitForDeploymentReady(testStorage.getNamespaceName(), testStorage.getScraperName());
        KafkaBridgeUtils.waitForKafkaBridgeReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        Map<String, String> bridgeSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaBridgeResources.componentName(testStorage.getClusterName()));
        final String bridgePodName = bridgeSnapshot.keySet().iterator().next();

        LOGGER.info("Asserting if log is without records");
        assertFalse(DEFAULT_LOG4J_PATTERN.matcher(StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), bridgePodName, "", "30s")).find());

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        loggers.put("rootLogger.level", "DEBUG");
        loggers.put("logger.bridge.level", "OFF");
        loggers.put("logger.healthy.level", "OFF");
        loggers.put("logger.ready.level", "OFF");
        ilDebug.setLoggers(loggers);

        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(testStorage.getClusterName(), bridz -> {
            bridz.getSpec().setLogging(ilDebug);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String bridgeLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "30s");
                return bridgeLog != null && !bridgeLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(bridgeLog).find();
            });

        ConfigMap configMapBridge = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("external-configmap-bridge")
            .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(Collections.singletonMap("log4j2.properties",
                "name = BridgeConfig\n" +
                    "\n" +
                    "appender.console.type = Console\n" +
                    "appender.console.name = STDOUT\n" +
                    "appender.console.layout.type = PatternLayout\n" +
                    "appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n\n" +
                    "\n" +
                    "rootLogger.level = OFF\n" +
                    "rootLogger.appenderRefs = console\n" +
                    "rootLogger.appenderRef.console.ref = STDOUT\n" +
                    "rootLogger.additivity = false\n" +
                    "\n" +
                    "logger.bridge.name = io.strimzi.kafka.bridge\n" +
                    "logger.bridge.level = OFF\n" +
                    "logger.bridge.appenderRefs = console\n" +
                    "logger.bridge.appenderRef.console.ref = STDOUT\n" +
                    "logger.bridge.additivity = false\n" +
                    "\n" +
                    "# HTTP OpenAPI specific logging levels (default is INFO)\n" +
                    "# Logging healthy and ready endpoints is very verbose because of Kubernetes health checking.\n" +
                    "logger.healthy.name = http.openapi.operation.healthy\n" +
                    "logger.healthy.level = OFF\n" +
                    "logger.ready.name = http.openapi.operation.ready\n" +
                    "logger.ready.level = OFF"))
            .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMapBridge);

        ExternalLogging bridgeXternalLogging = new ExternalLoggingBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(
                    new ConfigMapKeySelectorBuilder()
                        .withName("external-configmap-bridge")
                        .withKey("log4j2.properties")
                        .build()
                )
            .endValueFrom()
            .build();

        LOGGER.info("Setting log level of Bridge to OFF - records should not appear in the log");
        // change to the external logging
        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(testStorage.getClusterName(), bridz -> {
            bridz.getSpec().setLogging(bridgeXternalLogging);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("rootLogger.level = OFF")
                && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String bridgeLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "30s");
                return bridgeLog != null && bridgeLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(bridgeLog).find();
            });

        assertThat("Bridge Pod should not roll", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaBridgeResources.componentName(testStorage.getClusterName())), equalTo(bridgeSnapshot));
    }

    @IsolatedTest("Scraping log from shared Cluster Operator")
    @Tag(ROLLING_UPDATE)
    void testDynamicallySetClusterOperatorLoggingLevels() {
        final Map<String, String> coPod = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), STRIMZI_DEPLOYMENT_NAME);
        final String coPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(clusterOperator.getDeploymentNamespace(), STRIMZI_DEPLOYMENT_NAME).get(0).getMetadata().getName();
        final String command = "cat /opt/strimzi/custom-config/log4j2.properties";

        String log4jConfig =
            "name = COConfig\n" +
            "monitorInterval = 30\n" +
            "\n" +
            "    appender.console.type = Console\n" +
            "    appender.console.name = STDOUT\n" +
            "    appender.console.layout.type = PatternLayout\n" +
            "    appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n\n" +
            "\n" +
            "    rootLogger.level = OFF\n" +
            "    rootLogger.appenderRefs = stdout\n" +
            "    rootLogger.appenderRef.console.ref = STDOUT\n" +
            "    rootLogger.additivity = false\n" +
            "\n" +
            "    # Kafka AdminClient logging is a bit noisy at INFO level\n" +
            "    logger.kafka.name = org.apache.kafka\n" +
            "    logger.kafka.level = OFF\n" +
            "    logger.kafka.additivity = false\n" +
            "\n" +
            "    # Zookeeper is very verbose even on INFO level -> We set it to WARN by default\n" +
            "    logger.zookeepertrustmanager.name = org.apache.zookeeper\n" +
            "    logger.zookeepertrustmanager.level = OFF\n" +
            "    logger.zookeepertrustmanager.additivity = false";

        ConfigMap coMap = new ConfigMapBuilder()
            .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(STRIMZI_DEPLOYMENT_NAME)
                .withNamespace(clusterOperator.getDeploymentNamespace())
            .endMetadata()
            .withData(Collections.singletonMap("log4j2.properties", log4jConfig))
            .build();

        LOGGER.info("Checking that original logging config is different from the new one");
        assertThat(log4jConfig, not(equalTo(cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(coPodName, "/bin/bash", "-c", command).out().trim())));

        LOGGER.info("Changing logging for cluster-operator");
        kubeClient().updateConfigMapInNamespace(clusterOperator.getDeploymentNamespace(), coMap);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(coPodName, "/bin/bash", "-c", command).out().contains("rootLogger.level = OFF")
        );

        LOGGER.info("Checking log4j2.properties in CO Pod");
        String podLogConfig = cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(coPodName, "/bin/bash", "-c", command).out().trim();
        assertThat(podLogConfig, equalTo(log4jConfig));

        LOGGER.info("Checking if CO rolled its Pod");
        assertThat(coPod, equalTo(DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), STRIMZI_DEPLOYMENT_NAME)));

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String coLog = StUtils.getLogFromPodByTime(clusterOperator.getDeploymentNamespace(), coPodName, STRIMZI_DEPLOYMENT_NAME, "30s");
                LOGGER.debug(coLog);
                return coLog != null && coLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(coLog).find();
            });

        LOGGER.info("Changing all levels from OFF to INFO/WARN");
        log4jConfig = log4jConfig.replaceAll("OFF", "INFO");
        coMap.setData(Collections.singletonMap("log4j2.properties", log4jConfig));

        LOGGER.info("Changing logging for cluster-operator");
        kubeClient().updateConfigMapInNamespace(clusterOperator.getDeploymentNamespace(), coMap);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(coPodName, "/bin/bash", "-c", command).out().contains("rootLogger.level = INFO")
        );

        LOGGER.info("Checking log4j2.properties in CO Pod");
        podLogConfig = cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(coPodName, "/bin/bash", "-c", command).out().trim();
        assertThat(podLogConfig, equalTo(log4jConfig));

        LOGGER.info("Checking if CO rolled its Pod");
        assertThat(coPod, equalTo(DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), STRIMZI_DEPLOYMENT_NAME)));

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String coLog = StUtils.getLogFromPodByTime(clusterOperator.getDeploymentNamespace(), coPodName, STRIMZI_DEPLOYMENT_NAME, "30s");
                return coLog != null && !coLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(coLog).find();
            });
    }

    /**
     * @description This test case verifies dynamic changes in KafkaConnect logging level.
     *
     * @steps
     *  1. - Deploy Kafka cluster and KafkaConnect cluster, the latter with Log level Off.
     *  2. - Deploy all additional resources, scraper Pod and network policies.
     *  3. - Verify that no logs are present in KafkaConnect Pods.
     *  4. - Set inline log level to Debug in KafkaConnect custom resource.
     *     - log4j.properties file for given cluster has log level Debug, and pods provide logs on respective level.
     *  5. - Change inline log level from Debug to Info in KafkaConnect custom resource.
     *     - log4j.properties file for given cluster has log level Info, and pods provide logs on respective level.
     *  6. - Create ConfigMap with necessary data for external logging and modify KafkaConnect custom resource to use external logging setting log level Off.
     *     - log4j.properties file for given cluster has log level Off, and pods provide no more logs.
     * @usecase
     *  - logging
     *  - logging-change
     *  - kafka-connect
     */
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testDynamicallySetConnectLoggingLevels() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final Pattern log4jPatternDebugLevel = Pattern.compile("^(?<date>[\\d-]+) (?<time>[\\d:,]+) DEBUG (?<message>.+)");
        final Pattern log4jPatternInfoLevel = Pattern.compile("^(?<date>[\\d-]+) (?<time>[\\d:,]+) INFO (?<message>.+)");

        // logging changes on multiple Connect instances are possible from Kafka 3.7.0
        // TODO: change once support for Kafka 3.6.x is removed - https://github.com/strimzi/strimzi-kafka-operator/issues/9921
        final int connectReplicas = TestKafkaVersion.compareDottedVersions(Environment.ST_KAFKA_VERSION, "3.7.0") >= 0 ? 3 : 1;

        final KafkaConnect connect = KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), connectReplicas)
            .editSpec()
                .withLogging(new InlineLoggingBuilder()
                    .withLoggers(Map.of("connect.root.logger.level", "OFF")).build())
            .endSpec()
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithoutWait(
            KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build(),
            connect,
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        resourceManager.synchronizeResources();

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        final String scraperPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        final Map<String, String> connectPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());

        LOGGER.info("Asserting if logs in connect Pods are without records in last 60 seconds");
        final Predicate<String> logsDisabled = pod -> !DEFAULT_LOG4J_PATTERN.matcher(StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), pod, "", "60s")).find();
        connectPods.keySet().forEach(pod -> assertTrue(logsDisabled.test(pod)));

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        final InlineLogging inlineLoggingDebugLevel = new InlineLoggingBuilder().addToLoggers("connect.root.logger.level", "DEBUG").build();
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), conn -> {
            conn.getSpec().setLogging(inlineLoggingDebugLevel);
        }, testStorage.getNamespaceName());

        // check if lines from Logs contain messages with log level DEBUG
        final Predicate<String> hasLogLevelDebug = connectLogs -> connectLogs != null && !connectLogs.isEmpty() && log4jPatternDebugLevel.matcher(connectLogs).find();
        KafkaConnectUtils.waitForConnectLogLevelChangePropagation(testStorage, connectPods, scraperPodName, hasLogLevelDebug, "DEBUG");

        assertThat("Connect Pod should not roll", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()), equalTo(connectPods));

        LOGGER.info("Changing rootLogger level from DEBUG to INFO with inline logging");
        final InlineLogging inlineLoggingInfoLevel = new InlineLoggingBuilder().addToLoggers("connect.root.logger.level", "INFO").build();
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), conn -> {
            conn.getSpec().setLogging(inlineLoggingInfoLevel);
        }, testStorage.getNamespaceName());

        // check if lines from Logs contain log Level INFO but no longer DEBUG
        final Predicate<String> hasLogLevelInfo = connectLogs -> connectLogs != null && !connectLogs.isEmpty() &&
            !log4jPatternDebugLevel.matcher(connectLogs).find() &&
            log4jPatternInfoLevel.matcher(connectLogs).find();
        KafkaConnectUtils.waitForConnectLogLevelChangePropagation(testStorage, connectPods, scraperPodName, hasLogLevelInfo, "INFO");

        assertThat("Connect Pod should not roll", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()), equalTo(connectPods));

        // external logging

        final String log4jConfig = """
            log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
            log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
            log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %X{connector.context}%m (%c) [%t]%n
            log4j.rootLogger=OFF, CONSOLE
            log4j.logger.org.apache.zookeeper=ERROR
            log4j.logger.org.I0Itec.zkclient=ERROR
            log4j.logger.org.reflections=ERROR""";

        final String externalCmName = "external-cm";

        final ConfigMap connectLoggingMap = new ConfigMapBuilder()
            .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addToData("log4j.properties", log4jConfig)
            .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), connectLoggingMap);

        final ExternalLogging connectXternalLogging = new ExternalLoggingBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(
                    new ConfigMapKeySelectorBuilder()
                        .withName(externalCmName)
                        .withKey("log4j.properties")
                        .build()
                )
            .endValueFrom()
            .build();

        LOGGER.info("Setting log level of Connect to OFF");

        // change to the external logging
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), conn -> {
            conn.getSpec().setLogging(connectXternalLogging);
        }, testStorage.getNamespaceName());

        // check if there are no more new lines in Logs
        final Predicate<String> hasLogLevelOff = connectLogs -> connectLogs != null && connectLogs.isEmpty();
        KafkaConnectUtils.waitForConnectLogLevelChangePropagation(testStorage, connectPods, scraperPodName, hasLogLevelOff, "OFF");

        assertThat("Connect Pod should not roll", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()), equalTo(connectPods));
    }

    @ParallelNamespaceTest
    void testDynamicallySetKafkaLoggingLevels() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        InlineLogging ilOff = new InlineLogging();
        Map<String, String> log4jConfig = new HashMap<>();
        log4jConfig.put("kafka.root.logger.level", "OFF");
        log4jConfig.put("log4j.logger.org.I0Itec.zkclient.ZkClient", "OFF");
        log4jConfig.put("log4j.logger.org.apache.zookeeper", "OFF");
        log4jConfig.put("log4j.logger.kafka", "OFF");
        log4jConfig.put("log4j.logger.org.apache.kafka", "OFF");
        log4jConfig.put("log4j.logger.kafka.request.logger", "OFF, CONSOLE");
        log4jConfig.put("log4j.logger.kafka.network.Processor", "OFF");
        log4jConfig.put("log4j.logger.kafka.server.KafkaApis", "OFF");
        log4jConfig.put("log4j.logger.kafka.network.RequestChannel$", "OFF");
        log4jConfig.put("log4j.logger.kafka.controller", "OFF");
        log4jConfig.put("log4j.logger.kafka.log.LogCleaner", "OFF");
        log4jConfig.put("log4j.logger.state.change.logger", "OFF");
        log4jConfig.put("log4j.logger.kafka.authorizer.logger", "OFF");

        ilOff.setLoggers(log4jConfig);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
                .editSpec()
                    .editKafka()
                        .withLogging(ilOff)
                    .endKafka()
                .endSpec()
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        int podNum = KafkaResource.getPodNumFromPodName(testStorage.getBrokerComponentName(), brokerPodName);
        String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                boolean[] correctLogging = {true};

                brokerPods.keySet().forEach(podName -> {
                    String kafkaLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), podName, "kafka", "30s");
                    correctLogging[0] = kafkaLog != null && kafkaLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(kafkaLog).find();
                });

                return correctLogging[0];
            });

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        ilDebug.setLoggers(Collections.singletonMap("kafka.root.logger.level", "DEBUG"));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setLogging(ilDebug);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for dynamic change in the Kafka Pod");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("root=DEBUG"));

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                boolean[] correctLogging = {true};

                brokerPods.keySet().forEach(podName -> {
                    String kafkaLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), podName, "kafka", "30s");
                    correctLogging[0] = kafkaLog != null && !kafkaLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(kafkaLog).find();
                });

                return correctLogging[0];
            });

        LOGGER.info("Setting external logging INFO");
        ConfigMap configMap = new ConfigMapBuilder()
            .withNewMetadata()
              .withName("external-configmap")
              .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(Collections.singletonMap("log4j.properties", "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                "log4j.rootLogger=INFO, CONSOLE\n" +
                "log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n" +
                "log4j.logger.org.apache.zookeeper=INFO\n" +
                "log4j.logger.kafka=INFO\n" +
                "log4j.logger.org.apache.kafka=INFO\n" +
                "log4j.logger.kafka.request.logger=WARN\n" +
                "log4j.logger.kafka.network.Processor=ERROR\n" +
                "log4j.logger.kafka.server.KafkaApis=ERROR\n" +
                "log4j.logger.kafka.network.RequestChannel$=WARN\n" +
                "log4j.logger.kafka.controller=TRACE\n" +
                "log4j.logger.kafka.log.LogCleaner=INFO\n" +
                "log4j.logger.state.change.logger=TRACE\n" +
                "log4j.logger.kafka.authorizer.logger=INFO"))
            .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMap);

        ExternalLogging elKafka = new ExternalLoggingBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                    .withKey("log4j.properties")
                    .withName("external-configmap")
                    .withOptional(false)
                    .build())
            .endValueFrom()
            .build();

        LOGGER.info("Setting log level of Kafka INFO");
        // change to external logging
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setLogging(elKafka);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for dynamic change in the Kafka Pod");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("root=INFO"));

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                boolean[] correctLogging = {true};

                brokerPods.keySet().forEach(podName -> {
                    String kafkaLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), podName, "kafka", "30s");
                    correctLogging[0] = kafkaLog != null && !kafkaLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(kafkaLog).find();
                });

                return correctLogging[0];
            });

        assertThat("Kafka Pod should not roll", RollingUpdateUtils.componentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods), is(false));
    }

    @ParallelNamespaceTest
    void testDynamicallySetUnknownKafkaLogger() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        int podNum = KafkaResource.getPodNumFromPodName(testStorage.getBrokerComponentName(), brokerPodName);

        String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        InlineLogging il = new InlineLogging();
        il.setLoggers(Collections.singletonMap("log4j.logger.paprika", "INFO"));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setLogging(il);
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods);
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("paprika=INFO"));
    }

    @ParallelNamespaceTest
    void testDynamicallySetUnknownKafkaLoggerValue() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        InlineLogging il = new InlineLogging();
        il.setLoggers(Collections.singletonMap("kafka.root.logger.level", "PAPRIKA"));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setLogging(il);
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods);
        assertThat("Kafka Pod should not roll", RollingUpdateUtils.componentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods), is(false));
    }

    @ParallelNamespaceTest
    void testDynamicallySetKafkaExternalLogging() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // this test changes dynamically unchangeable logging config and thus RU is expected
        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                        "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                        "log4j.rootLogger=INFO, CONSOLE\n" +
                        "log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n" +
                        "log4j.logger.org.apache.zookeeper=INFO\n" +
                        "log4j.logger.kafka=INFO\n" +
                        "log4j.logger.org.apache.kafka=INFO\n" +
                        "log4j.logger.kafka.request.logger=WARN\n" +
                        "log4j.logger.kafka.network.Processor=ERROR\n" +
                        "log4j.logger.kafka.server.KafkaApis=ERROR\n" +
                        "log4j.logger.kafka.network.RequestChannel$=WARN\n" +
                        "log4j.logger.kafka.controller=TRACE\n" +
                        "log4j.logger.kafka.log.LogCleaner=INFO\n" +
                        "log4j.logger.state.change.logger=TRACE\n" +
                        "log4j.logger.kafka.authorizer.logger=${kafka.my.level.string}\n" +
                        "kafka.my.level.string=INFO"
                        ))
                .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMap);

        ExternalLogging el = new ExternalLoggingBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(
                    new ConfigMapKeySelectorBuilder()
                        .withName("external-cm")
                        .withKey("log4j.properties")
                        .build()
                )
            .endValueFrom()
            .build();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );

        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1)
            .editOrNewSpec()
                .editKafka()
                    .withLogging(el)
                .endKafka()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        int podNum = KafkaResource.getPodNumFromPodName(testStorage.getBrokerComponentName(), brokerPodName);
        String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                        "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                        "log4j.rootLogger=INFO, CONSOLE\n" +
                        "log4j.logger.org.I0Itec.zkclient.ZkClient=ERROR\n" +
                        "log4j.logger.org.apache.zookeeper=ERROR\n" +
                        "log4j.logger.kafka=ERROR\n" +
                        "log4j.logger.org.apache.kafka=ERROR\n" +
                        "log4j.logger.kafka.request.logger=WARN\n" +
                        "log4j.logger.kafka.network.Processor=ERROR\n" +
                        "log4j.logger.kafka.server.KafkaApis=ERROR\n" +
                        "log4j.logger.kafka.network.RequestChannel$=ERROR\n" +
                        "log4j.logger.kafka.controller=ERROR\n" +
                        "log4j.logger.kafka.log.LogCleaner=ERROR\n" +
                        "log4j.logger.state.change.logger=TRACE\n" +
                        "log4j.logger.kafka.authorizer.logger=${kafka.my.level.string}\n" +
                        "kafka.my.level.string=ERROR"
                        ))
                .build();

        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), configMap);
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods);
        assertThat("Kafka Pod should not roll", RollingUpdateUtils.componentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods), is(false));
        TestUtils.waitFor("Verify logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("kafka.authorizer.logger=ERROR"));

        // log4j.appender.CONSOLE.layout.ConversionPattern is changed and thus we need RU
        configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                        "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]\n" +
                        "log4j.rootLogger=INFO, CONSOLE\n" +
                        "log4j.logger.org.I0Itec.zkclient.ZkClient=ERROR\n" +
                        "log4j.logger.org.apache.zookeeper=ERROR\n" +
                        "log4j.logger.kafka=ERROR\n" +
                        "log4j.logger.org.apache.kafka=ERROR\n" +
                        "log4j.logger.kafka.request.logger=WARN\n" +
                        "log4j.logger.kafka.network.Processor=ERROR\n" +
                        "log4j.logger.kafka.server.KafkaApis=ERROR\n" +
                        "log4j.logger.kafka.network.RequestChannel$=ERROR\n" +
                        "log4j.logger.kafka.controller=ERROR\n" +
                        "log4j.logger.kafka.log.LogCleaner=ERROR\n" +
                        "log4j.logger.state.change.logger=TRACE\n" +
                        "log4j.logger.kafka.authorizer.logger=${kafka.my.level.string}\n" +
                        "kafka.my.level.string=DEBUG"
                ))
                .build();

        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), configMap);
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods);

        TestUtils.waitFor("Verify logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("kafka.authorizer.logger=DEBUG"));
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    void testDynamicallySetMM2LoggingLevels() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        InlineLogging ilOff = new InlineLogging();
        Map<String, String> loggers = new HashMap<>();
        loggers.put("connect.root.logger.level", "OFF");
        loggers.put("log4j.logger.org.apache.zookeeper", "OFF");
        loggers.put("log4j.logger.org.I0Itec.zkclient", "OFF");
        loggers.put("log4j.logger.org.reflections", "OFF");

        ilOff.setLoggers(loggers);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getTargetClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), testStorage.getTargetClusterName(), testStorage.getSourceClusterName(), 1, false)
                .editOrNewSpec()
                    .withLogging(ilOff)
                .endSpec()
                .build());

        String kafkaMM2PodName = kubeClient().namespace(testStorage.getNamespaceName()).listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getName();
        String mm2LogCheckCmd = "http://localhost:8083/admin/loggers/root";

        Map<String, String> mm2Snapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector());

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String mmLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), kafkaMM2PodName, "", "30s");
                return mmLog != null && mmLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(mmLog).find();
            });

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        loggers.put("connect.root.logger.level", "DEBUG");
        ilDebug.setLoggers(loggers);

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getClusterName(), mm2 -> {
            mm2.getSpec().setLogging(ilDebug);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(kafkaMM2PodName, "curl", mm2LogCheckCmd).out().contains("DEBUG")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String mmLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), kafkaMM2PodName, "", "30s");
                return mmLog != null && !mmLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(mmLog).find();
            });

        String log4jConfig =
                "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                        "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %X{connector.context}%m (%c) [%t]%n\n" +
                        "log4j.rootLogger=OFF, CONSOLE\n" +
                        "log4j.logger.org.apache.zookeeper=ERROR\n" +
                        "log4j.logger.org.I0Itec.zkclient=ERROR\n" +
                        "log4j.logger.org.reflections=ERROR";

        String externalCmName = "external-cm";

        ConfigMap mm2LoggingMap = new ConfigMapBuilder()
                .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), mm2LoggingMap);

        ExternalLogging mm2XternalLogging = new ExternalLoggingBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(
                    new ConfigMapKeySelectorBuilder()
                        .withName(externalCmName)
                        .withKey("log4j.properties")
                        .build()
                )
            .endValueFrom()
            .build();

        LOGGER.info("Setting log level of MirrorMaker2 to OFF");
        // change to the external logging
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getClusterName(), mm2 -> {
            mm2.getSpec().setLogging(mm2XternalLogging);
        }, testStorage.getNamespaceName());

        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(kafkaMM2PodName, "curl", mm2LogCheckCmd).out().contains("OFF")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String mmLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), kafkaMM2PodName, "", "30s");
                return mmLog != null && !mmLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(mmLog).find();
            });

        assertThat("MirrorMaker2 pod should not roll", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector()), equalTo(mm2Snapshot));
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(MIRROR_MAKER2)
    void testMM2LoggingLevelsHierarchy() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getSourceClusterName(), 1).build());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getTargetClusterName(), 1).build());

        String log4jConfig =
                "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                        "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                        "log4j.rootLogger=OFF, CONSOLE\n" +
                        "log4j.logger.org.apache.zookeeper=ERROR\n" +
                        "log4j.logger.org.I0Itec.zkclient=ERROR\n" +
                        "log4j.logger.org.eclipse.jetty.util.thread=FATAL\n" +
                        "log4j.logger.org.apache.kafka.connect.runtime.WorkerTask=OFF\n" +
                        "log4j.logger.org.eclipse.jetty.util.thread.strategy.EatWhatYouKill=OFF\n" +
                        "log4j.logger.org.reflections=ERROR";

        String externalCmName = "external-cm-hierarchy";

        ConfigMap mm2LoggingMap = new ConfigMapBuilder()
                .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), mm2LoggingMap);

        ExternalLogging mm2XternalLogging = new ExternalLoggingBuilder()
                .withNewValueFrom()
                .withConfigMapKeyRef(
                        new ConfigMapKeySelectorBuilder()
                                .withName(externalCmName)
                                .withKey("log4j.properties")
                                .build()
                )
                .endValueFrom()
                .build();

        resourceManager.createResourceWithWait(
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), testStorage.getTargetClusterName(), testStorage.getSourceClusterName(), 1, false)
                .editOrNewSpec()
                    .withLogging(mm2XternalLogging)
                .endSpec()
                .build());

        String kafkaMM2PodName = kubeClient().namespace(testStorage.getNamespaceName()).listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getName();

        Map<String, String> mm2Snapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector());

        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger init levels", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/root").out().contains("OFF")
        );

        LOGGER.info("Changing log levels");
        String updatedLog4jConfig =
                "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                        "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                        "log4j.rootLogger=INFO, CONSOLE\n" +
                        "log4j.logger.org.apache.zookeeper=ERROR\n" +
                        "log4j.logger.org.I0Itec.zkclient=ERROR\n" +
                        "log4j.logger.org.eclipse.jetty.util.thread=WARN\n" +
                        "log4j.logger.org.reflections=ERROR";

        mm2LoggingMap = new ConfigMapBuilder()
                .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", updatedLog4jConfig))
                .build();

        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), mm2LoggingMap);

        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/root").out().contains("INFO")
                    // not set logger should inherit parent level (in this case 'org.eclipse.jetty.util.thread')
                    && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/org.eclipse.jetty.util.thread.strategy.EatWhatYouKill").out().contains("WARN")
                    // logger with not set parent should inherit root
                    && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/org.apache.kafka.connect.runtime.WorkerTask").out().contains("INFO")
        );

        assertThat("MirrorMaker2 pod should not roll", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector()), equalTo(mm2Snapshot));
    }

    @ParallelNamespaceTest
    void testNotExistingCMSetsDefaultLogging() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String defaultProps = TestUtils.getFileAsString(TestUtils.USER_PATH + "/../cluster-operator/src/main/resources/default-logging/KafkaCluster.properties");

        String cmData = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
            "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
            "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
            "log4j.rootLogger=INFO, CONSOLE\n" +
            "log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n" +
            "log4j.logger.org.apache.zookeeper=INFO\n" +
            "log4j.logger.kafka=INFO\n" +
            "log4j.logger.org.apache.kafka=INFO";

        String existingCmName = "external-cm";
        String nonExistingCmName = "non-existing-cm-name";

        ConfigMap configMap = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(existingCmName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(Collections.singletonMap("log4j.properties", cmData))
            .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMap);

        LOGGER.info("Deploying Kafka with custom logging");
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1)
            .editOrNewSpec()
                .editKafka()
                .withLogging(new ExternalLoggingBuilder()
                    .withNewValueFrom()
                        .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                                .withKey("log4j.properties")
                                .withName(existingCmName)
                                .withOptional(false)
                                .build())
                    .endValueFrom()
                    .build())
                .endKafka()
            .endSpec()
            .build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        
        String log4jFile =  cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.DEBUG,
            brokerPodName,
            "kafka", "/bin/bash", "-c", "cat custom-config/log4j.properties").out();
        assertTrue(log4jFile.contains(cmData));

        LOGGER.info("Changing external logging's CM to not existing one");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setLogging(
            new ExternalLoggingBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                            .withKey("log4j.properties")
                            .withName(nonExistingCmName)
                            .withOptional(false)
                            .build())
                .endValueFrom()
                .build()), testStorage.getNamespaceName());

        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods);

        LOGGER.info("Checking that log4j.properties in custom-config isn't empty and configuration is default");
        log4jFile = cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.DEBUG,
            brokerPodName,
            "kafka", "/bin/bash", "-c", "cat custom-config/log4j.properties").out();

        assertFalse(log4jFile.isEmpty());
        assertTrue(log4jFile.contains(cmData));
        assertFalse(log4jFile.contains(defaultProps));

        LOGGER.info("Checking if Kafka: {} contains error about non-existing CM", testStorage.getClusterName());
        Condition condition = KafkaResource.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().filter(con -> CustomResourceStatus.NotReady.toString().equals(con.getType())).findFirst().orElse(null);
        assertThat(condition, is(notNullValue()));
        assertTrue(condition.getMessage().matches("ConfigMap " + nonExistingCmName + " with external logging configuration does not exist .*"));
    }

    @ParallelNamespaceTest
    void testLoggingHierarchy() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(
            connect,
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaConnectorTemplates.defaultKafkaConnector(testStorage.getClusterName(), testStorage.getClusterName(), 1).build()
        );

        LOGGER.info("Deploying network policies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(connect, KafkaConnectResources.componentName(testStorage.getClusterName()));


        String connectorClassName = "org.apache.kafka.connect.file.FileStreamSourceConnector";
        final String scraperPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Changing rootLogger level in KafkaConnector to ERROR with inline logging");
        InlineLogging inlineError = new InlineLogging();
        inlineError.setLoggers(Collections.singletonMap("log4j.logger." + connectorClassName, "ERROR"));

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kc -> kc.getSpec().setLogging(inlineError), testStorage.getNamespaceName());

        LOGGER.info("Waiting for Connect API loggers will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl",
                "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/admin/loggers/" + connectorClassName).out().contains("ERROR")
        );

        LOGGER.info("Restarting KafkaConnector {} with class name {}", testStorage.getClusterName(), connectorClassName);
        cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(scraperPodName,
            "curl", "-X", "POST",
            "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/connectors/" + testStorage.getClusterName() + "/restart");

        KafkaConnectorUtils.waitForConnectorWorkerStatus(testStorage.getNamespaceName(), scraperPodName, testStorage.getClusterName(), testStorage.getClusterName(), "RUNNING");

        LOGGER.info("Checking that logger is same for connector with class name {}", connectorClassName);
        String connectorLogger = cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl",
            "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/admin/loggers/" + connectorClassName).out();

        assertTrue(connectorLogger.contains("ERROR"));

        LOGGER.info("Changing KafkaConnect's root logger to WARN, KafkaConnector: {} shouldn't inherit it", testStorage.getClusterName());

        InlineLogging inlineWarn = new InlineLogging();
        inlineWarn.setLoggers(Collections.singletonMap("connect.root.logger.level", "WARN"));

        inlineWarn.setLoggers(Map.of("connect.root.logger.level", "WARN", "log4j.logger." + connectorClassName, "ERROR"));

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kc -> kc.getSpec().setLogging(inlineWarn), testStorage.getNamespaceName());

        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl",
                "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/admin/loggers/root").out().contains("WARN")
        );

        LOGGER.info("Checking if KafkaConnector {} doesn't inherit logger from KafkaConnect", connectorClassName);

        KafkaConnectorUtils.loggerStabilityWait(testStorage.getNamespaceName(), testStorage.getClusterName(), scraperPodName, "ERROR", connectorClassName);
    }

    /**
     * @description This test case check that changing Logging configuration from internal to external triggers Rolling Update.
     *
     * @steps
     *  1. - Deploy Kafka Cluster, without any logging related configuration
     *     - Cluster is deployed
     *  2. - Modify Kafka by changing specification of logging to new external value
     *     - Change in logging specification triggers Rolling Update
     *  3. - Modify Kafka by changing specification of logging to new logging format
     *     - Change in logging specification triggers Rolling Update
     *
     * @usecase
     *  - logging
     *  - rolling-update
     */
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testChangingInternalToExternalLoggingTriggerRollingUpdate() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // EO dynamic logging is tested in io.strimzi.systemtest.log.LoggingChangeST.testDynamicallySetEOloggingLevels
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = null;
        if (!Environment.isKRaftModeEnabled()) {
            controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        }

        final String loggersConfig = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
            "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
            "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]\n" +
            "kafka.root.logger.level=INFO\n" +
            "log4j.rootLogger=${kafka.root.logger.level}, CONSOLE\n" +
            "log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n" +
            "log4j.logger.org.apache.zookeeper=INFO\n" +
            "log4j.logger.kafka=INFO\n" +
            "log4j.logger.org.apache.kafka=INFO\n" +
            "log4j.logger.kafka.request.logger=WARN, CONSOLE\n" +
            "log4j.logger.kafka.network.Processor=INFO\n" +
            "log4j.logger.kafka.server.KafkaApis=INFO\n" +
            "log4j.logger.kafka.network.RequestChannel$=INFO\n" +
            "log4j.logger.kafka.controller=INFO\n" +
            "log4j.logger.kafka.log.LogCleaner=INFO\n" +
            "log4j.logger.state.change.logger=TRACE\n" +
            "log4j.logger.kafka.authorizer.logger=INFO";

        final String configMapLoggersName = "loggers-config-map";
        ConfigMap configMapLoggers = new ConfigMapBuilder()
            .withNewMetadata()
                .withNamespace(testStorage.getNamespaceName())
                .withName(configMapLoggersName)
            .endMetadata()
            .addToData("log4j-custom.properties", loggersConfig)
            .build();

        ConfigMapKeySelector log4jLoggimgCMselector = new ConfigMapKeySelectorBuilder()
            .withName(configMapLoggersName)
            .withKey("log4j-custom.properties")
            .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), configMapLoggers);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setLogging(new ExternalLoggingBuilder()
                .withNewValueFrom()
                .withConfigMapKeyRef(log4jLoggimgCMselector)
                .endValueFrom()
                .build());
            if (!Environment.isKRaftModeEnabled()) {
                kafka.getSpec().getZookeeper().setLogging(new ExternalLoggingBuilder()
                    .withNewValueFrom()
                    .withConfigMapKeyRef(log4jLoggimgCMselector)
                    .endValueFrom()
                    .build());
            }
        }, testStorage.getNamespaceName());

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Waiting for Zookeeper pods to roll after change in logging");
            controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        }

        LOGGER.info("Waiting for Kafka pods to roll after change in logging");
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        configMapLoggers.getData().put("log4j-custom.properties", loggersConfig.replace("%p %m (%c) [%t]", "%p %m (%c) [%t]%n"));
        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), configMapLoggers);

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Waiting for Zookeeper pods to roll after change in logging properties config map");
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        }
        LOGGER.info("Waiting for Kafka pods to roll after change in logging properties config map");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
