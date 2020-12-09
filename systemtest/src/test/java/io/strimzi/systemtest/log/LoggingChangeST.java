/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.log;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.LOGGING_RELOADING_INTERVAL;
import static io.strimzi.systemtest.Constants.RECONCILIATION_INTERVAL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.systemtest.Constants.STRIMZI_DEPLOYMENT_NAME;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
class LoggingChangeST extends AbstractST {
    static final String NAMESPACE = "logging-change-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LoggingChangeST.class);

    @Test
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testJSONFormatLogging() {
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
        String configMapCOName = "json-layout-cluster-operator";

        ConfigMap configMapKafka = new ConfigMapBuilder()
            .withNewMetadata()
                .withNewName(configMapKafkaName)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .addToData("log4j.properties", loggersConfigKafka)
            .build();

        ConfigMap configMapOperators = new ConfigMapBuilder()
            .withNewMetadata()
                .withNewName(configMapOpName)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .addToData("log4j2.properties", loggersConfigOperators)
            .build();

        ConfigMap configMapZookeeper = new ConfigMapBuilder()
            .withNewMetadata()
                .withNewName(configMapZookeeperName)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .addToData("log4j.properties", loggersConfigZookeeper)
            .build();

        ConfigMap configMapCO = new ConfigMapBuilder()
            .withNewMetadata()
                .withNewName(configMapCOName)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .addToData("log4j2.properties", loggersConfigCO)
            .build();

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapKafka);
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapOperators);
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapZookeeper);
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapOperators);
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapCO);

        // We have to install CO in class stack, otherwise it will be deleted at the end of test case and all following tests will fail
        ResourceManager.setClassResources();
        BundleResource.clusterOperator(NAMESPACE)
            .editOrNewSpec()
                .editOrNewTemplate()
                    .editOrNewSpec()
                        .addNewVolume()
                            .withName("logging-config-volume")
                            .editOrNewConfigMap()
                                .withName(configMapCOName)
                            .endConfigMap()
                        .endVolume()
                        .editFirstContainer()
                            .withVolumeMounts(new VolumeMountBuilder().withName("logging-config-volume").withMountPath("/tmp/log-config-map-file").build())
                            .addToEnv(new EnvVarBuilder().withName("JAVA_OPTS").withValue("-Dlog4j2.configurationFile=file:/tmp/log-config-map-file/log4j2.properties").build())
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .done();
        // Now we set pointer stack to method again
        ResourceManager.setMethodResources();

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editOrNewSpec()
                .editKafka()
                    .withLogging(new ExternalLoggingBuilder().withName(configMapKafkaName).build())
                .endKafka()
                .editZookeeper()
                    .withLogging(new ExternalLoggingBuilder().withName(configMapZookeeperName).build())
                .endZookeeper()
                .editEntityOperator()
                    .editTopicOperator()
                        .withLogging(new ExternalLoggingBuilder().withName(configMapOpName).build())
                    .endTopicOperator()
                    .editUserOperator()
                        .withLogging(new ExternalLoggingBuilder().withName(configMapOpName).build())
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());

        assertThat(StUtils.checkLogForJSONFormat(operatorSnapshot, ResourceManager.getCoDeploymentName()), is(true));
        assertThat(StUtils.checkLogForJSONFormat(kafkaPods, "kafka"), is(true));
        assertThat(StUtils.checkLogForJSONFormat(zkPods, "zookeeper"), is(true));
        assertThat(StUtils.checkLogForJSONFormat(eoPods, "topic-operator"), is(true));
        assertThat(StUtils.checkLogForJSONFormat(eoPods, "user-operator"), is(true));
    }

    @Test
    @Tag(ROLLING_UPDATE)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testDynamicallySetEOloggingLevels() throws InterruptedException {
        InlineLogging ilOff = new InlineLogging();
        ilOff.setLoggers(Collections.singletonMap("rootLogger.level", "OFF"));

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 1, 1)
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
                .done();

        String eoDeploymentName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(eoDeploymentName);

        final String eoPodName = eoPods.keySet().iterator().next();

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        ilDebug.setLoggers(Collections.singletonMap("rootLogger.level", "DEBUG"));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setLogging(ilDebug);
            k.getSpec().getEntityOperator().getUserOperator().setLogging(ilDebug);
        });

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                        && cmdKubeClient().execInPodContainer(eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
        );

        LOGGER.info("Waiting {} ms for DEBUG log will appear", LOGGING_RELOADING_INTERVAL * 2);
        // wait some time and check whether logs (UO and TO) after this time contain anything
        Thread.sleep(LOGGING_RELOADING_INTERVAL * 2);

        LOGGER.info("Asserting if log will contain some records");
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "user-operator", "30s"), is(not(emptyString())));
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "topic-operator", "30s"), is(not(emptyString())));

        LOGGER.info("Setting external logging OFF");
        ConfigMap configMapTo = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-configmap-to")
                .withNamespace(NAMESPACE)
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
                .withNamespace(NAMESPACE)
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

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapTo);
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapUo);

        ExternalLogging elTo = new ExternalLoggingBuilder()
                .withName("external-configmap-to")
                .build();

        ExternalLogging elUo = new ExternalLoggingBuilder()
                .withName("external-configmap-uo")
                .build();

        LOGGER.info("Setting log level of TO and UO to OFF - records should not appear in log");
        // change to external logging
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setLogging(elTo);
            k.getSpec().getEntityOperator().getUserOperator().setLogging(elUo);
        });

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                        && cmdKubeClient().execInPodContainer(eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                        && cmdKubeClient().execInPodContainer(eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
                        && cmdKubeClient().execInPodContainer(eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        LOGGER.info("Waiting {} ms for DEBUG log will disappear", LOGGING_RELOADING_INTERVAL * 2);
        Thread.sleep(LOGGING_RELOADING_INTERVAL * 2);

        LOGGER.info("Asserting if log is without records");
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "topic-operator", "30s"), is(emptyString()));
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "user-operator", "30s"), is(emptyString()));

        LOGGER.info("Setting external logging OFF");
        configMapTo = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-configmap-to")
                .withNamespace(NAMESPACE)
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
                .withNamespace(NAMESPACE)
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

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapTo);
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapUo);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                        && cmdKubeClient().execInPodContainer(eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
        );

        LOGGER.info("Waiting {} ms for DEBUG log will appear", LOGGING_RELOADING_INTERVAL * 2);
        //wait some time if TO and UO will log something
        Thread.sleep(LOGGING_RELOADING_INTERVAL * 2);

        LOGGER.info("Asserting if log will contain some records");
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "user-operator", "30s"), is(not(emptyString())));
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "topic-operator", "30s"), is(not(emptyString())));

        assertThat("EO pod should not roll", DeploymentUtils.depSnapshot(eoDeploymentName), equalTo(eoPods));
    }

    @Test
    @Tag(BRIDGE)
    @Tag(ROLLING_UPDATE)
    void testDynamicallySetBridgeLoggingLevels() throws InterruptedException {
        InlineLogging ilOff = new InlineLogging();
        Map<String, String> loggers = new HashMap<>();
        loggers.put("rootLogger.level", "OFF");
        loggers.put("logger.bridge.level", "OFF");
        loggers.put("logger.healthy.level", "OFF");
        loggers.put("logger.ready.level", "OFF");
        ilOff.setLoggers(loggers);

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 1, 1).done();

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();

        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), 1)
                .editSpec()
                    .withInlineLogging(ilOff)
                .endSpec()
                .done();

        Map<String, String> bridgeSnapshot = DeploymentUtils.depSnapshot(KafkaBridgeResources.deploymentName(CLUSTER_NAME));

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        loggers.put("rootLogger.level", "DEBUG");
        loggers.put("logger.bridge.level", "OFF");
        loggers.put("logger.healthy.level", "OFF");
        loggers.put("logger.ready.level", "OFF");
        ilDebug.setLoggers(loggers);

        KafkaBridgeResource.replaceBridgeResource(CLUSTER_NAME, bridz -> {
            bridz.getSpec().setLogging(ilDebug);
        });

        final String bridgePodName = bridgeSnapshot.keySet().iterator().next();
        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(bridgePodName, KafkaBridgeResources.deploymentName(CLUSTER_NAME), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                && cmdKubeClient().execInPodContainer(bridgePodName, KafkaBridgeResources.deploymentName(CLUSTER_NAME), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        LOGGER.info("Waiting {} ms for DEBUG log will appear", LOGGING_RELOADING_INTERVAL * 2);
        // wait some time and check whether logs after this time contain anything
        Thread.sleep(LOGGING_RELOADING_INTERVAL * 2);

        LOGGER.info("Asserting if log will contain some records");
        assertThat(StUtils.getLogFromPodByTime(bridgePodName, KafkaBridgeResources.deploymentName(CLUSTER_NAME), "30s"), is(not(emptyString())));

        ConfigMap configMapBridge = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-configmap-bridge")
                .withNamespace(NAMESPACE)
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

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapBridge);

        ExternalLogging bridgeXternalLogging = new ExternalLoggingBuilder()
                .withName("external-configmap-bridge")
                .build();

        LOGGER.info("Setting log level of Bridge to OFF - records should not appear in the log");
        // change to the external logging
        KafkaBridgeResource.replaceBridgeResource(CLUSTER_NAME, bridz -> {
            bridz.getSpec().setLogging(bridgeXternalLogging);
        });

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(bridgePodName, KafkaBridgeResources.deploymentName(CLUSTER_NAME), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("rootLogger.level = OFF")
                && cmdKubeClient().execInPodContainer(bridgePodName, KafkaBridgeResources.deploymentName(CLUSTER_NAME), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        LOGGER.info("Waiting {} ms log to be empty", LOGGING_RELOADING_INTERVAL * 2);
        // wait some time and check whether logs after this time are empty
        Thread.sleep(LOGGING_RELOADING_INTERVAL * 2);

        LOGGER.info("Asserting if log will contain no records");
        assertThat(StUtils.getLogFromPodByTime(bridgePodName, KafkaBridgeResources.deploymentName(CLUSTER_NAME), "30s"), is(emptyString()));

        assertThat("Bridge pod should not roll", DeploymentUtils.depSnapshot(KafkaBridgeResources.deploymentName(CLUSTER_NAME)), equalTo(bridgeSnapshot));
    }

    @Test
    @Tag(ROLLING_UPDATE)
    void testDynamicallySetClusterOperatorLoggingLevels() throws InterruptedException {
        Map<String, String> coPod = DeploymentUtils.depSnapshot(STRIMZI_DEPLOYMENT_NAME);
        String coPodName = kubeClient().listPodsByPrefixInName(STRIMZI_DEPLOYMENT_NAME).get(0).getMetadata().getName();
        String command = "cat /opt/strimzi/custom-config/log4j2.properties";

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
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withData(Collections.singletonMap("log4j2.properties", log4jConfig))
            .build();

        LOGGER.info("Checking that original logging config is different from the new one");
        assertThat(log4jConfig, not(equalTo(cmdKubeClient().execInPod(coPodName, "/bin/bash", "-c", command).out().trim())));

        LOGGER.info("Changing logging for cluster-operator");
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(coMap);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPod(coPodName, "/bin/bash", "-c", command).out().contains("rootLogger.level = OFF")
        );

        LOGGER.info("Checking log4j2.properties in CO pod");
        String podLogConfig = cmdKubeClient().execInPod(coPodName, "/bin/bash", "-c", command).out().trim();
        assertThat(podLogConfig, equalTo(log4jConfig));

        LOGGER.info("Checking if CO rolled its pod");
        assertThat(coPod, equalTo(DeploymentUtils.depSnapshot(STRIMZI_DEPLOYMENT_NAME)));

        LOGGER.info("Waiting {} ms log to be empty", LOGGING_RELOADING_INTERVAL);
        // wait some time and check whether logs after this time are empty
        Thread.sleep(LOGGING_RELOADING_INTERVAL);

        LOGGER.info("Asserting if log will contain no records");
        assertThat(StUtils.getLogFromPodByTime(coPodName, STRIMZI_DEPLOYMENT_NAME, "30s"), is(emptyString()));

        LOGGER.info("Changing all levels from OFF to INFO/WARN");
        log4jConfig = log4jConfig.replaceAll("OFF", "INFO");
        coMap.setData(Collections.singletonMap("log4j2.properties", log4jConfig));

        LOGGER.info("Changing logging for cluster-operator");
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(coMap);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPod(coPodName, "/bin/bash", "-c", command).out().contains("rootLogger.level = INFO")
        );

        LOGGER.info("Checking log4j2.properties in CO pod");
        podLogConfig = cmdKubeClient().execInPod(coPodName, "/bin/bash", "-c", command).out().trim();
        assertThat(podLogConfig, equalTo(log4jConfig));

        LOGGER.info("Checking if CO rolled its pod");
        assertThat(coPod, equalTo(DeploymentUtils.depSnapshot(STRIMZI_DEPLOYMENT_NAME)));

        long reconciliationSleep = RECONCILIATION_INTERVAL + Duration.ofSeconds(10).toMillis();
        LOGGER.info("Waiting {} ms log not to be empty", reconciliationSleep);
        // wait enough time (at least for reconciliation time + 10) and check whether logs after this time are not empty
        Thread.sleep(reconciliationSleep);

        LOGGER.info("Asserting if log will contain no records");
        String coLog = StUtils.getLogFromPodByTime(coPodName, STRIMZI_DEPLOYMENT_NAME, "30s");
        assertThat(coLog, is(not(emptyString())));
        assertThat(coLog.contains("INFO"), is(true));
    }

    @Test
    @Tag(ROLLING_UPDATE)
    void testDynamicallySetConnectLoggingLevels() {
        InlineLogging ilOff = new InlineLogging();
        Map<String, String> loggers = new HashMap<>();
        loggers.put("connect.root.logger.level", "OFF");
        ilOff.setLoggers(loggers);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();
        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();
        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editSpec()
                .withInlineLogging(ilOff)
                .endSpec()
                .editMetadata()
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .done();

        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(KafkaConnectResources.deploymentName(CLUSTER_NAME));

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        loggers.put("connect.root.logger.level", "DEBUG");
        ilDebug.setLoggers(loggers);

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, conn -> {
            conn.getSpec().setLogging(ilDebug);
        });

        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "http://" + KafkaConnectResources.serviceName(CLUSTER_NAME)
                        + ":8083/admin/loggers/root").out().contains("DEBUG")
        );

        String log4jConfig =
                "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                        "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                        "log4j.rootLogger=OFF, CONSOLE\n" +
                        "log4j.logger.org.apache.zookeeper=ERROR\n" +
                        "log4j.logger.org.I0Itec.zkclient=ERROR\n" +
                        "log4j.logger.org.reflections=ERROR";

        String externalCmName = "external-cm";

        ConfigMap connectLoggingMap = new ConfigMapBuilder()
                .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(connectLoggingMap);

        ExternalLogging connectXternalLogging = new ExternalLoggingBuilder()
                .withName(externalCmName)
                .build();

        LOGGER.info("Setting log level of Connect to OFF");
        // change to the external logging
        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, conn -> {
            conn.getSpec().setLogging(connectXternalLogging);
        });

        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "http://" + KafkaConnectResources.serviceName(CLUSTER_NAME)
                        + ":8083/admin/loggers/root").out().contains("OFF")
        );

        assertThat("Connect pod should not roll", DeploymentUtils.depSnapshot(KafkaConnectResources.deploymentName(CLUSTER_NAME)), equalTo(connectSnapshot));
    }

    @Test
    void testDynamicallySetKafkaLoggingLevels() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 1).done();
        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        InlineLogging ilOff = new InlineLogging();
        ilOff.setLoggers(Collections.singletonMap("kafka.root.logger.level", "INFO"));

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        ilDebug.setLoggers(Collections.singletonMap("kafka.root.logger.level", "DEBUG"));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getKafka().setLogging(ilDebug);
        });

        LOGGER.info("Waiting for dynamic change in the kafka pod");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                "kafka", "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type broker-loggers --entity-name 0").out()
                .contains("root=DEBUG"));

        LOGGER.info("Setting external logging INFO");
        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-configmap")
                .withNamespace(NAMESPACE)
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

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMap);

        ExternalLogging elKafka = new ExternalLoggingBuilder()
                .withName("external-configmap")
                .build();

        LOGGER.info("Setting log level of kafka INFO");
        // change to external logging
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getKafka().setLogging(elKafka);
        });

        LOGGER.info("Waiting for dynamic change in the kafka pod");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                    "kafka", "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type broker-loggers --entity-name 0").out()
                    .contains("root=INFO"));

        assertThat("Kafka pod should not roll", StatefulSetUtils.ssHasRolled(kafkaName, kafkaPods), is(false));
    }

    @Test
    void testDynamicallySetUnknownKafkaLogger() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 1).done();
        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        InlineLogging il = new InlineLogging();
        il.setLoggers(Collections.singletonMap("log4j.logger.paprika", "INFO"));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getKafka().setLogging(il);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaName, kafkaPods);
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                        "kafka", "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type broker-loggers --entity-name 0").out()
                        .contains("paprika=INFO"));
    }

    @Test
    void testDynamicallySetUnknownKafkaLoggerValue() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 1).done();
        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        InlineLogging il = new InlineLogging();
        il.setLoggers(Collections.singletonMap("kafka.root.logger.level", "PAPRIKA"));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getKafka().setLogging(il);
        });

        StatefulSetUtils.waitForNoRollingUpdate(kafkaName, kafkaPods);
        assertThat("Kafka pod should not roll", StatefulSetUtils.ssHasRolled(kafkaName, kafkaPods), is(false));
    }

    @Test
    void testDynamicallySetKafkaExternalLogging() {
        // this test changes dynamically unchangeable logging config and thus RU is expected
        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(NAMESPACE)
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

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMap);
        ExternalLogging el = new ExternalLogging();
        el.setName("external-cm");

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 1)
                .editOrNewSpec()
                    .editKafka()
                        .withExternalLogging(el)
                    .endKafka()
                .endSpec()
                .done();

        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(NAMESPACE)
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
                        "log4j.logger.kafka.authorizer.logger=ERROR"))
                .build();

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMap);
        StatefulSetUtils.waitForNoRollingUpdate(kafkaName, kafkaPods);
        assertThat("Kafka pod should not roll", StatefulSetUtils.ssHasRolled(kafkaName, kafkaPods), is(false));
        TestUtils.waitFor("Verify logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                        "kafka", "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type broker-loggers --entity-name 0").out()
                        .contains("kafka.authorizer.logger=ERROR"));

        // log4j.appender.CONSOLE.layout.ConversionPattern is changed and thus we need RU
        configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(NAMESPACE)
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
                        "log4j.logger.kafka.authorizer.logger=DEBUG"))
                .build();

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMap);
        StatefulSetUtils.waitTillSsHasRolled(kafkaName, kafkaPods);

        TestUtils.waitFor("Verify logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                        "kafka", "/bin/bash", "-c", "bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type broker-loggers --entity-name 0").out()
                        .contains("kafka.authorizer.logger=DEBUG"));
    }

    @Test
    @Tag(ROLLING_UPDATE)
    void testDynamicallySetMM2LoggingLevels() {
        InlineLogging ilOff = new InlineLogging();
        Map<String, String> loggers = new HashMap<>();
        loggers.put("connect.root.logger.level", "OFF");
        ilOff.setLoggers(loggers);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME + "-source", 3).done();
        KafkaResource.kafkaEphemeral(CLUSTER_NAME + "-target", 3).done();
        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, CLUSTER_NAME + "-target", CLUSTER_NAME + "-source", 1, false).done();
        String kafkaMM2PodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaMirrorMaker2.RESOURCE_KIND).get(0).getMetadata().getName();
        String mm2LogCheckCmd = "http://localhost:8083/admin/loggers/root";

        Map<String, String> mm2Snapshot = DeploymentUtils.depSnapshot(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME));

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");
        InlineLogging ilDebug = new InlineLogging();
        loggers.put("connect.root.logger.level", "DEBUG");
        ilDebug.setLoggers(loggers);

        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(CLUSTER_NAME, mm2 -> {
            mm2.getSpec().setLogging(ilDebug);
        });

        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPod(kafkaMM2PodName, "curl", mm2LogCheckCmd).out().contains("DEBUG")
        );

        String log4jConfig =
                "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                        "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                        "log4j.rootLogger=OFF, CONSOLE\n" +
                        "log4j.logger.org.apache.zookeeper=ERROR\n" +
                        "log4j.logger.org.I0Itec.zkclient=ERROR\n" +
                        "log4j.logger.org.reflections=ERROR";

        String externalCmName = "external-cm";

        ConfigMap mm2LoggingMap = new ConfigMapBuilder()
                .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(mm2LoggingMap);

        ExternalLogging mm2XternalLogging = new ExternalLoggingBuilder()
                .withName(externalCmName)
                .build();

        LOGGER.info("Setting log level of MM2 to OFF");
        // change to the external logging
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(CLUSTER_NAME, mm2 -> {
            mm2.getSpec().setLogging(mm2XternalLogging);
        });

        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPod(kafkaMM2PodName, "curl", mm2LogCheckCmd).out().contains("OFF")
        );

        assertThat("MirrorMaker2 pod should not roll", DeploymentUtils.depSnapshot(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME)), equalTo(mm2Snapshot));
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
    }

    @Override
    protected void tearDownEnvironmentAfterAll() {
        teardownEnvForOperator();
    }


    @Override
    protected void assertNoCoErrorsLogged(long sinceSeconds) {
        LOGGER.info("Skipping assertion if CO has some unexpected errors");
    }
}
