/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.log;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
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
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.annotations.ParallelSuite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.systemtest.Constants.STRIMZI_DEPLOYMENT_NAME;
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
@ParallelSuite
class LoggingChangeST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(LoggingChangeST.class);

    private static final Pattern DEFAULT_LOG4J_PATTERN = Pattern.compile("^(?<date>[\\d-]+) (?<time>[\\d:,]+) (?<status>\\w+) (?<message>.+)");

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(LoggingChangeST.class.getSimpleName()).stream().findFirst().get();

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testJSONFormatLogging(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

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
        String configMapCOName = Constants.STRIMZI_DEPLOYMENT_NAME;

        String originalCoLoggers = kubeClient().getClient().configMaps()
            .inNamespace(clusterOperator.getDeploymentNamespace()).withName(configMapCOName).get().getData().get("log4j2.properties");

        ConfigMap configMapKafka = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapKafkaName)
                .withNamespace(namespaceName)
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
                .withNamespace(namespaceName)
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
                .withNamespace(namespaceName)
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

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMapKafka).createOrReplace();
        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMapOperators).createOrReplace();
        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMapZookeeper).createOrReplace();
        kubeClient().getClient().configMaps().inNamespace(clusterOperator.getDeploymentNamespace()).resource(configMapCO).createOrReplace();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
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
            .build());

        Map<String, String> zkPods = PodUtils.podSnapshot(namespaceName, zkSelector);
        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));
        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName());

        StUtils.checkLogForJSONFormat(clusterOperator.getDeploymentNamespace(), operatorSnapshot, ResourceManager.getCoDeploymentName());
        StUtils.checkLogForJSONFormat(namespaceName, kafkaPods, "kafka");
        if (!Environment.isKRaftModeEnabled()) {
            StUtils.checkLogForJSONFormat(namespaceName, zkPods, "zookeeper");
            StUtils.checkLogForJSONFormat(namespaceName, eoPods, "topic-operator");
        }
        StUtils.checkLogForJSONFormat(namespaceName, eoPods, "user-operator");

        // set loggers of CO back to original
        configMapCO.getData().put("log4j2.properties", originalCoLoggers);
        kubeClient().getClient().configMaps().inNamespace(clusterOperator.getDeploymentNamespace()).resource(configMapCO).createOrReplace();
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    void testDynamicallySetEOloggingLevels(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        InlineLogging ilOff = new InlineLogging();
        ilOff.setLoggers(Collections.singletonMap("rootLogger.level", "OFF"));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 1, 1)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withInlineLogging(ilOff)
                    .endTopicOperator()
                    .editUserOperator()
                        .withInlineLogging(ilOff)
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        String eoDeploymentName = KafkaResources.entityOperatorDeploymentName(clusterName);
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(namespaceName, eoDeploymentName);

        final String eoPodName = eoPods.keySet().iterator().next();

        LOGGER.info("Checking if EO pod contains any log (except configuration)");
        assertFalse(DEFAULT_LOG4J_PATTERN.matcher(StUtils.getLogFromPodByTime(namespaceName, eoPodName, "user-operator", "30s")).find());

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        ilDebug.setLoggers(Collections.singletonMap("rootLogger.level", "DEBUG"));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setLogging(ilDebug);
            k.getSpec().getEntityOperator().getUserOperator().setLogging(ilDebug);
        }, namespaceName);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.TRACE, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                        && cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.TRACE, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String uoLog = StUtils.getLogFromPodByTime(namespaceName, eoPodName, "user-operator", "30s");
                String toLog = StUtils.getLogFromPodByTime(namespaceName, eoPodName, "topic-operator", "30s");

                return uoLog != null && toLog != null &&
                    !(uoLog.isEmpty() && toLog.isEmpty()) &&
                    DEFAULT_LOG4J_PATTERN.matcher(uoLog).find() &&
                    DEFAULT_LOG4J_PATTERN.matcher(toLog).find();
            });

        LOGGER.info("Setting external logging OFF");
        ConfigMap configMapTo = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("external-configmap-to")
            .withNamespace(namespaceName)
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
            .withNamespace(namespaceName)
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

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMapTo).createOrReplace();
        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMapUo).createOrReplace();

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
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setLogging(elTo);
            k.getSpec().getEntityOperator().getUserOperator().setLogging(elUo);
        }, namespaceName);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.TRACE, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                    && cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.TRACE, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                    && cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.TRACE, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
                    && cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.TRACE, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String uoLog = StUtils.getLogFromPodByTime(namespaceName, eoPodName, "user-operator", "30s");
                String toLog = StUtils.getLogFromPodByTime(namespaceName, eoPodName, "topic-operator", "30s");

                return uoLog != null && toLog != null &&
                    uoLog.isEmpty() && toLog.isEmpty() &&
                    !(DEFAULT_LOG4J_PATTERN.matcher(uoLog).find() && DEFAULT_LOG4J_PATTERN.matcher(toLog).find());
            });

        LOGGER.info("Setting external logging OFF");
        configMapTo = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("external-configmap-to")
            .withNamespace(namespaceName)
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
            .withNamespace(namespaceName)
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

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMapTo).createOrReplace();
        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMapUo).createOrReplace();

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.TRACE, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                        && cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.TRACE, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String uoLog = StUtils.getLogFromPodByTime(namespaceName, eoPodName, "user-operator", "30s");
                String toLog = StUtils.getLogFromPodByTime(namespaceName, eoPodName, "topic-operator", "30s");

                return uoLog != null && toLog != null &&
                    !(uoLog.isEmpty() && toLog.isEmpty()) &&
                    DEFAULT_LOG4J_PATTERN.matcher(uoLog).find() &&
                    DEFAULT_LOG4J_PATTERN.matcher(toLog).find();
            });

        assertThat("EO pod should not roll", DeploymentUtils.depSnapshot(namespaceName, eoDeploymentName), equalTo(eoPods));
    }

    @ParallelNamespaceTest
    @Tag(BRIDGE)
    @Tag(ROLLING_UPDATE)
    void testDynamicallySetBridgeLoggingLevels(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        InlineLogging ilOff = new InlineLogging();
        Map<String, String> loggers = new HashMap<>();
        loggers.put("rootLogger.level", "OFF");
        loggers.put("logger.bridge.level", "OFF");
        loggers.put("logger.healthy.level", "OFF");
        loggers.put("logger.ready.level", "OFF");
        ilOff.setLoggers(loggers);

        // create resources async
        resourceManager.createResource(extensionContext, false,
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaBridgeTemplates.kafkaBridge(testStorage.getClusterName(), KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()), 1)
                .editSpec()
                    .withInlineLogging(ilOff)
                .endSpec()
                .build());

        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());
        DeploymentUtils.waitForDeploymentReady(testStorage.getNamespaceName(), testStorage.getScraperName());
        KafkaBridgeUtils.waitForKafkaBridgeReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        Map<String, String> bridgeSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaBridgeResources.deploymentName(testStorage.getClusterName()));
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
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, bridgePodName, KafkaBridgeResources.deploymentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, bridgePodName, KafkaBridgeResources.deploymentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String bridgeLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), bridgePodName, KafkaBridgeResources.deploymentName(testStorage.getClusterName()), "30s");
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
                    "appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n\n" +
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

        kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).resource(configMapBridge).createOrReplace();

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
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, bridgePodName, KafkaBridgeResources.deploymentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("rootLogger.level = OFF")
                && cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPodContainer(Level.TRACE, bridgePodName, KafkaBridgeResources.deploymentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String bridgeLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), bridgePodName, KafkaBridgeResources.deploymentName(testStorage.getClusterName()), "30s");
                return bridgeLog != null && bridgeLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(bridgeLog).find();
            });

        assertThat("Bridge pod should not roll", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaBridgeResources.deploymentName(testStorage.getClusterName())), equalTo(bridgeSnapshot));
    }

    @IsolatedTest("Scraping log from shared Cluster Operator")
    @Tag(ROLLING_UPDATE)
    void testDynamicallySetClusterOperatorLoggingLevels(ExtensionContext extensionContext) throws InterruptedException {
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
        kubeClient().getClient().configMaps().inNamespace(clusterOperator.getDeploymentNamespace()).resource(coMap).createOrReplace();

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(coPodName, "/bin/bash", "-c", command).out().contains("rootLogger.level = OFF")
        );

        LOGGER.info("Checking log4j2.properties in CO pod");
        String podLogConfig = cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(coPodName, "/bin/bash", "-c", command).out().trim();
        assertThat(podLogConfig, equalTo(log4jConfig));

        LOGGER.info("Checking if CO rolled its pod");
        assertThat(coPod, equalTo(DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), STRIMZI_DEPLOYMENT_NAME)));

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String coLog = StUtils.getLogFromPodByTime(clusterOperator.getDeploymentNamespace(), coPodName, STRIMZI_DEPLOYMENT_NAME, "30s");
                LOGGER.debug(coLog);
                return coLog != null && coLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(coLog).find();
            });

        LOGGER.info("Changing all levels from OFF to INFO/WARN");
        log4jConfig = log4jConfig.replaceAll("OFF", "INFO");
        coMap.setData(Collections.singletonMap("log4j2.properties", log4jConfig));

        LOGGER.info("Changing logging for cluster-operator");
        kubeClient().getClient().configMaps().inNamespace(clusterOperator.getDeploymentNamespace()).resource(coMap).createOrReplace();

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(coPodName, "/bin/bash", "-c", command).out().contains("rootLogger.level = INFO")
        );

        LOGGER.info("Checking log4j2.properties in CO pod");
        podLogConfig = cmdKubeClient().namespace(clusterOperator.getDeploymentNamespace()).execInPod(coPodName, "/bin/bash", "-c", command).out().trim();
        assertThat(podLogConfig, equalTo(log4jConfig));

        LOGGER.info("Checking if CO rolled its pod");
        assertThat(coPod, equalTo(DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), STRIMZI_DEPLOYMENT_NAME)));

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String coLog = StUtils.getLogFromPodByTime(clusterOperator.getDeploymentNamespace(), coPodName, STRIMZI_DEPLOYMENT_NAME, "30s");
                return coLog != null && !coLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(coLog).find();
            });
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testDynamicallySetConnectLoggingLevels(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        InlineLogging ilOff = new InlineLogging();
        Map<String, String> loggers = new HashMap<>();
        loggers.put("connect.root.logger.level", "OFF");
        ilOff.setLoggers(loggers);

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editSpec()
                .withInlineLogging(ilOff)
            .endSpec()
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build();

        resourceManager.createResource(extensionContext, false,
            KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build(),
            connect,
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        resourceManager.synchronizeResources(extensionContext);

        LOGGER.info("Deploy NetworkPolicies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(extensionContext, connect, KafkaConnectResources.deploymentName(testStorage.getClusterName()));

        String scraperPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaConnectResources.deploymentName(testStorage.getClusterName()));
        final String connectPodName = connectSnapshot.keySet().iterator().next();

        LOGGER.info("Asserting if log is without records");
        assertFalse(DEFAULT_LOG4J_PATTERN.matcher(StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), connectPodName, "", "30s")).find());

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        loggers.put("connect.root.logger.level", "DEBUG");
        ilDebug.setLoggers(loggers);

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), conn -> {
            conn.getSpec().setLogging(ilDebug);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName())
                        + ":8083/admin/loggers/root").out().contains("DEBUG")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String kcLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), connectPodName, "", "30s");
                return kcLog != null && !kcLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(kcLog).find();
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

        ConfigMap connectLoggingMap = new ConfigMapBuilder()
                .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).resource(connectLoggingMap).createOrReplace();

        ExternalLogging connectXternalLogging = new ExternalLoggingBuilder()
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

        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName())
                + ":8083/admin/loggers/root").out().contains("OFF")
        );

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String kcLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), connectPodName, "", "30s");
                return kcLog != null && kcLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(kcLog).find();
            });

        assertThat("Connect pod should not roll", DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaConnectResources.deploymentName(testStorage.getClusterName())), equalTo(connectSnapshot));
    }

    @ParallelNamespaceTest
    void testDynamicallySetKafkaLoggingLevels(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String scraperName = mapWithScraperNames.get(extensionContext.getDisplayName());
        String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, kafkaName);

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

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withInlineLogging(ilOff)
                    .endKafka()
                .endSpec()
                .build(),
            ScraperTemplates.scraperPod(namespaceName, scraperName).build()
        );

        String scraperPodName = kubeClient().listPodsByPrefixInName(namespaceName, scraperName).get(0).getMetadata().getName();
        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                boolean[] correctLogging = {true};

                kafkaPods.keySet().forEach(podName -> {
                    String kafkaLog = StUtils.getLogFromPodByTime(namespaceName, podName, "kafka", "30s");
                    correctLogging[0] = kafkaLog != null && kafkaLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(kafkaLog).find();
                });

                return correctLogging[0];
            });

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        ilDebug.setLoggers(Collections.singletonMap("kafka.root.logger.level", "DEBUG"));
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
            k.getSpec().getKafka().setLogging(ilDebug);
        }, namespaceName);

        LOGGER.info("Waiting for dynamic change in the kafka pod");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), 0).contains("root=DEBUG"));

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                boolean[] correctLogging = {true};

                kafkaPods.keySet().forEach(podName -> {
                    String kafkaLog = StUtils.getLogFromPodByTime(namespaceName, podName, "kafka", "30s");
                    correctLogging[0] = kafkaLog != null && !kafkaLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(kafkaLog).find();
                });

                return correctLogging[0];
            });

        LOGGER.info("Setting external logging INFO");
        ConfigMap configMap = new ConfigMapBuilder()
            .withNewMetadata()
              .withName("external-configmap")
              .withNamespace(namespaceName)
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

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMap).createOrReplace();

        ExternalLogging elKafka = new ExternalLoggingBuilder()
            .withNewValueFrom()
                .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                    .withKey("log4j.properties")
                    .withName("external-configmap")
                    .withOptional(false)
                    .build())
            .endValueFrom()
            .build();

        LOGGER.info("Setting log level of kafka INFO");
        // change to external logging
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
            k.getSpec().getKafka().setLogging(elKafka);
        }, namespaceName);

        LOGGER.info("Waiting for dynamic change in the kafka pod");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), 0).contains("root=INFO"));

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                boolean[] correctLogging = {true};

                kafkaPods.keySet().forEach(podName -> {
                    String kafkaLog = StUtils.getLogFromPodByTime(namespaceName, podName, "kafka", "30s");
                    correctLogging[0] = kafkaLog != null && !kafkaLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(kafkaLog).find();
                });

                return correctLogging[0];
            });

        assertThat("Kafka pod should not roll", RollingUpdateUtils.componentHasRolled(namespaceName, kafkaSelector, kafkaPods), is(false));
    }

    @ParallelNamespaceTest
    void testDynamicallySetUnknownKafkaLogger(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String scraperName = mapWithScraperNames.get(extensionContext.getDisplayName());

        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaPersistent(clusterName, 3, 1).build(),
            ScraperTemplates.scraperPod(namespaceName, scraperName).build()
        );

        String scraperPodName = kubeClient().listPodsByPrefixInName(namespaceName, scraperName).get(0).getMetadata().getName();
        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        InlineLogging il = new InlineLogging();
        il.setLoggers(Collections.singletonMap("log4j.logger.paprika", "INFO"));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
            k.getSpec().getKafka().setLogging(il);
        }, namespaceName);

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, kafkaPods);
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), 0).contains("paprika=INFO"));
    }

    @ParallelNamespaceTest
    void testDynamicallySetUnknownKafkaLoggerValue(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 1).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        InlineLogging il = new InlineLogging();
        il.setLoggers(Collections.singletonMap("kafka.root.logger.level", "PAPRIKA"));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
            k.getSpec().getKafka().setLogging(il);
        }, namespaceName);

        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaPods);
        assertThat("Kafka pod should not roll", RollingUpdateUtils.componentHasRolled(namespaceName, kafkaSelector, kafkaPods), is(false));
    }

    @ParallelNamespaceTest
    void testDynamicallySetKafkaExternalLogging(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String scraperName = mapWithScraperNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        // this test changes dynamically unchangeable logging config and thus RU is expected
        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(namespaceName)
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

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMap).createOrReplace();
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

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 1)
            .editOrNewSpec()
                .editKafka()
                    .withExternalLogging(el)
                .endKafka()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(namespaceName, scraperName).build()
        );

        String scraperPodName = kubeClient().listPodsByPrefixInName(namespaceName, scraperName).get(0).getMetadata().getName();
        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(namespaceName)
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

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMap).createOrReplace();
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaPods);
        assertThat("Kafka pod should not roll", RollingUpdateUtils.componentHasRolled(namespaceName, kafkaSelector, kafkaPods), is(false));
        TestUtils.waitFor("Verify logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), 0).contains("kafka.authorizer.logger=ERROR"));

        // log4j.appender.CONSOLE.layout.ConversionPattern is changed and thus we need RU
        configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(namespaceName)
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

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMap).createOrReplace();
        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, kafkaPods);

        TestUtils.waitFor("Verify logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), 0).contains("kafka.authorizer.logger=DEBUG"));
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    void testDynamicallySetMM2LoggingLevels(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        InlineLogging ilOff = new InlineLogging();
        Map<String, String> loggers = new HashMap<>();
        loggers.put("connect.root.logger.level", "OFF");
        loggers.put("log4j.logger.org.apache.zookeeper", "OFF");
        loggers.put("log4j.logger.org.I0Itec.zkclient", "OFF");
        loggers.put("log4j.logger.org.reflections", "OFF");

        ilOff.setLoggers(loggers);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName + "-source", 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName + "-target", 1).build());
        resourceManager.createResource(extensionContext,
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, clusterName + "-target", clusterName + "-source", 1, false)
                .editOrNewSpec()
                    .withInlineLogging(ilOff)
                .endSpec()
                .build());

        String kafkaMM2PodName = kubeClient().namespace(namespaceName).listPods(namespaceName, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getName();
        String mm2LogCheckCmd = "http://localhost:8083/admin/loggers/root";

        Map<String, String> mm2Snapshot = DeploymentUtils.depSnapshot(namespaceName, KafkaMirrorMaker2Resources.deploymentName(clusterName));

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String mmLog = StUtils.getLogFromPodByTime(namespaceName, kafkaMM2PodName, "", "30s");
                return mmLog != null && mmLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(mmLog).find();
            });

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        loggers.put("connect.root.logger.level", "DEBUG");
        ilDebug.setLoggers(loggers);

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(clusterName, mm2 -> {
            mm2.getSpec().setLogging(ilDebug);
        }, namespaceName);

        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(namespaceName).execInPod(kafkaMM2PodName, "curl", mm2LogCheckCmd).out().contains("DEBUG")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String mmLog = StUtils.getLogFromPodByTime(namespaceName, kafkaMM2PodName, "", "30s");
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
                .withNamespace(namespaceName)
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(mm2LoggingMap).createOrReplace();

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

        LOGGER.info("Setting log level of MM2 to OFF");
        // change to the external logging
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(clusterName, mm2 -> {
            mm2.getSpec().setLogging(mm2XternalLogging);
        }, namespaceName);

        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(namespaceName).execInPod(kafkaMM2PodName, "curl", mm2LogCheckCmd).out().contains("OFF")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), Constants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String mmLog = StUtils.getLogFromPodByTime(namespaceName, kafkaMM2PodName, "", "30s");
                return mmLog != null && !mmLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(mmLog).find();
            });

        assertThat("MirrorMaker2 pod should not roll", DeploymentUtils.depSnapshot(namespaceName, KafkaMirrorMaker2Resources.deploymentName(clusterName)), equalTo(mm2Snapshot));
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testMM2LoggingLevelsHierarchy(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName + "-source", 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName + "-target", 1).build());

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
                .withNamespace(namespaceName)
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(mm2LoggingMap).createOrReplace();

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

        resourceManager.createResource(extensionContext,
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(clusterName, clusterName + "-target", clusterName + "-source", 1, false)
                .editOrNewSpec()
                    .withLogging(mm2XternalLogging)
                .endSpec()
                .build());

        String kafkaMM2PodName = kubeClient().namespace(namespaceName).listPods(namespaceName, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getName();

        Map<String, String> mm2Snapshot = DeploymentUtils.depSnapshot(namespaceName, KafkaMirrorMaker2Resources.deploymentName(clusterName));

        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger init levels", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(namespaceName).execInPod(kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/root").out().contains("OFF")
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
                .withNamespace(namespaceName)
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", updatedLog4jConfig))
                .build();

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(mm2LoggingMap).createOrReplace();

        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(namespaceName).execInPod(kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/root").out().contains("INFO")
                    // not set logger should inherit parent level (in this case 'org.eclipse.jetty.util.thread')
                    && cmdKubeClient().namespace(namespaceName).execInPod(kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/org.eclipse.jetty.util.thread.strategy.EatWhatYouKill").out().contains("WARN")
                    // logger with not set parent should inherit root
                    && cmdKubeClient().namespace(namespaceName).execInPod(kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/org.apache.kafka.connect.runtime.WorkerTask").out().contains("INFO")
        );

        assertThat("MirrorMaker2 pod should not roll", DeploymentUtils.depSnapshot(namespaceName, KafkaMirrorMaker2Resources.deploymentName(clusterName)), equalTo(mm2Snapshot));
    }

    @ParallelNamespaceTest
    void testNotExistingCMSetsDefaultLogging(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String defaultProps = TestUtils.getFileAsString(TestUtils.USER_PATH + "/../cluster-operator/src/main/resources/default-logging/KafkaCluster.properties");
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

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
                .withNamespace(namespaceName)
            .endMetadata()
            .withData(Collections.singletonMap("log4j.properties", cmData))
            .build();

        kubeClient().getClient().configMaps().inNamespace(namespaceName).resource(configMap).createOrReplace();

        LOGGER.info("Deploying Kafka with custom logging");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 1)
            .editOrNewSpec()
                .editKafka()
                .withExternalLogging(new ExternalLoggingBuilder()
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

        String kafkaSsName = KafkaResources.kafkaStatefulSetName(clusterName);
        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);

        String log4jFile =  cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.DEBUG, KafkaResources.kafkaPodName(clusterName, 0),
            "kafka", "/bin/bash", "-c", "cat custom-config/log4j.properties").out();
        assertTrue(log4jFile.contains(cmData));

        LOGGER.info("Changing external logging's CM to not existing one");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> kafka.getSpec().getKafka().setLogging(
            new ExternalLoggingBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                            .withKey("log4j.properties")
                            .withName(nonExistingCmName)
                            .withOptional(false)
                            .build())
                .endValueFrom()
                .build()), namespaceName);

        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaPods);

        LOGGER.info("Checking that log4j.properties in custom-config isn't empty and configuration is default");
        log4jFile = cmdKubeClient().namespace(namespaceName).execInPodContainer(Level.DEBUG, KafkaResources.kafkaPodName(clusterName, 0),
            "kafka", "/bin/bash", "-c", "cat custom-config/log4j.properties").out();

        assertFalse(log4jFile.isEmpty());
        assertTrue(log4jFile.contains(cmData));
        assertFalse(log4jFile.contains(defaultProps));

        LOGGER.info("Checking if Kafka:{} contains error about non-existing CM", clusterName);
        Condition condition = KafkaResource.kafkaClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getConditions().stream().filter(con -> CustomResourceStatus.NotReady.toString().equals(con.getType())).findFirst().orElse(null);
        assertThat(condition, is(notNullValue()));
        assertTrue(condition.getMessage().matches("ConfigMap " + nonExistingCmName + " with external logging configuration does not exist .*"));
    }

    @ParallelNamespaceTest
    void testLoggingHierarchy(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());

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

        resourceManager.createResource(extensionContext,
            connect,
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaConnectorTemplates.defaultKafkaConnector(testStorage.getClusterName(), testStorage.getClusterName(), 1).build()
        );

        String connectorClassName = "org.apache.kafka.connect.file.FileStreamSourceConnector";
        final String scraperPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Changing rootLogger level in KafkaConnector to ERROR with inline logging");
        InlineLogging inlineError = new InlineLogging();
        inlineError.setLoggers(Collections.singletonMap("log4j.logger." + connectorClassName, "ERROR"));

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kc -> kc.getSpec().setLogging(inlineError), testStorage.getNamespaceName());

        LOGGER.info("Waiting for Connect API loggers will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl",
                "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/admin/loggers/" + connectorClassName).out().contains("ERROR")
        );

        LOGGER.info("Restarting Kafka connector {} with class name {}", testStorage.getClusterName(), connectorClassName);
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

        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl",
                "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/admin/loggers/root").out().contains("WARN")
        );

        LOGGER.info("Checking if KafkaConnector {} doesn't inherit logger from KafkaConnect", connectorClassName);

        KafkaConnectorUtils.loggerStabilityWait(testStorage.getNamespaceName(), testStorage.getClusterName(), scraperPodName, "ERROR", connectorClassName);
    }
}
