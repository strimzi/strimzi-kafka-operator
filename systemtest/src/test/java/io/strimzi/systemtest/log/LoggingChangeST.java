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
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.LOGGING_RELOADING_INTERVAL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;

@Tag(REGRESSION)
class LoggingChangeST extends AbstractST {
    static final String NAMESPACE = "logging-change-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LoggingChangeST.class);

    @Test
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testJSONFormatLogging() {
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

        KubernetesResource.clusterOperator(NAMESPACE)
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

        LOGGER.info("Changing rootLogger level to INFO with inline logging");
        InlineLogging ilInfo = new InlineLogging();
        ilInfo.setLoggers(Collections.singletonMap("rootLogger.level", "INFO"));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setLogging(ilInfo);
            k.getSpec().getEntityOperator().getUserOperator().setLogging(ilInfo);
        });

        LOGGER.info("The EO shouldn't roll - verifying pod stability");
        DeploymentUtils.waitForNoRollingUpdate(eoDeploymentName, eoPods);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=INFO")
                        && cmdKubeClient().execInPodContainer(eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=INFO")
        );

        LOGGER.info("Waiting {} ms for INFO log will appear", LOGGING_RELOADING_INTERVAL);
        //wait some time if TO and UO will log something
        Thread.sleep(LOGGING_RELOADING_INTERVAL);

        LOGGER.info("Asserting if log will contain some records");
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "user-operator", "1m"), is(not(emptyString())));
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "topic-operator", "1m"), is(not(emptyString())));

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
                        "appender.console.layout.pattern=[%d] %-5p <%-12.12c{1}:%L> [%-12.12t] %m%n\n" +
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
                        "appender.console.layout.pattern=[%d] %-5p <%-12.12c{1}:%L> [%-12.12t] %m%n\n" +
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

        LOGGER.info("The EO shouldn't roll - verifying pod stability");
        DeploymentUtils.waitForNoRollingUpdate(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), eoPods);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                        && cmdKubeClient().execInPodContainer(eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                        && cmdKubeClient().execInPodContainer(eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
                        && cmdKubeClient().execInPodContainer(eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
        );

        LOGGER.info("Waiting {} ms for INFO log will disappear", LOGGING_RELOADING_INTERVAL);
        Thread.sleep(LOGGING_RELOADING_INTERVAL);

        LOGGER.info("Asserting if log is without records");
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "topic-operator", "1m"), is(emptyString()));
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "user-operator", "1m"), is(emptyString()));


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
                        "appender.console.layout.pattern=[%d] %-5p <%-12.12c{1}:%L> [%-12.12t] %m%n\n" +
                        "rootLogger.level=INFO\n" +
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
                        "appender.console.layout.pattern=[%d] %-5p <%-12.12c{1}:%L> [%-12.12t] %m%n\n" +
                        "rootLogger.level=INFO\n" +
                        "rootLogger.appenderRefs=stdout\n" +
                        "rootLogger.appenderRef.console.ref=STDOUT\n" +
                        "rootLogger.additivity=false"))
                .build();

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapTo);
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapUo);

        LOGGER.info("The EO shouldn't roll - verifying pod stability");
        DeploymentUtils.waitForNoRollingUpdate(eoDeploymentName, eoPods);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPodContainer(eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=INFO")
                        && cmdKubeClient().execInPodContainer(eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=INFO")
        );

        LOGGER.info("Waiting {} ms for INFO log will appear", LOGGING_RELOADING_INTERVAL);
        //wait some time if TO and UO will log something
        Thread.sleep(LOGGING_RELOADING_INTERVAL);

        LOGGER.info("Asserting if log will contain some records");
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "user-operator", "1m"), is(not(emptyString())));
        assertThat(StUtils.getLogFromPodByTime(eoPodName, "topic-operator", "1m"), is(not(emptyString())));
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skip env recreation after failed tests!");
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
