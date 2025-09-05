/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.log;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.enums.LogLevel;
import io.skodjob.testframe.resources.KubeResourceManager;
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
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
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
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.TestConstants.STRIMZI_DEPLOYMENT_NAME;
import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROLLING_UPDATE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("This suite verifies logging behavior under various configurations and scenarios."),
    beforeTestSteps = {
        @Step(value = "Deploy the cluster operator.", expected = "Cluster operator is deployed successfully.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA),
        @Label(value = TestDocsLabels.LOGGING)
    }
)
class LoggingChangeST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(LoggingChangeST.class);

    private static final Pattern DEFAULT_LOG4J_PATTERN = Pattern.compile("^(?<date>[\\d-]+) (?<time>[\\d:,]+) (?<status>\\w+) (?<message>.+)");

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test verifying that the logging in JSON format works correctly across Kafka and operators using the JsonTemplateLayout."),
        steps = {
            @Step(value = "Assume Kafka version is 4.0.0 or greater.", expected = "Assumption holds true."),
            @Step(value = "Assume non-Helm and non-OLM installation.", expected = "Assumption holds true."),
            @Step(value = "Create ConfigMaps for Kafka and operators with JSON logging configuration.", expected = "ConfigMaps created and applied."),
            @Step(value = "Deploy Kafka cluster with the configured logging setup.", expected = "Kafka cluster deployed successfully."),
            @Step(value = "Perform pod snapshot for controllers, brokers, and entity operators.", expected = "Pod snapshots successfully captured."),
            @Step(value = "Verify logs are in JSON format for all components.", expected = "Logs are in JSON format."),
            @Step(value = "Restore original logging configuration.", expected = "Original logging is restored.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    @SuppressWarnings("checkstyle:MethodLength")
    void testJsonTemplateLayoutFormatLogging() {

        assumeTrue(TestKafkaVersion.compareDottedVersions(Environment.ST_KAFKA_VERSION, "4.0.0") >= 0,
            "Kafka version is lower than 4.0.0, JsonTemplateLayout is not supported");

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // In this test scenario we change configuration for CO and we have to be sure, that CO is installed via YAML bundle instead of helm or OLM
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        final String loggersConfigKafka = """
            name = KafkaConfig
            
            appender.console.type = Console
            appender.console.name = STDOUT
            appender.console.layout.type = JsonTemplateLayout
            appender.console.layout.eventTemplate = {"instant": {  "$resolver": "timestamp",  "pattern": {    "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",    "timeZone": "UTC"  }},"someConstant": 1,"message": {  "$resolver": "message",  "stringified": true}}
            
            rootLogger.level = INFO
            rootLogger.appenderRefs = console
            rootLogger.appenderRef.console.ref = STDOUT
            rootLogger.additivity = false
            
            logger.kafka.name = kafka
            logger.kafka.level = INFO
            logger.kafka.appenderRefs = console
            logger.kafka.appenderRef.console.ref = STDOUT
            logger.kafka.additivity = false
            
            logger.orgapachekafka.name = org.apache.kafka
            logger.orgapachekafka.level = INFO
            logger.orgapachekafka.appenderRefs = console
            logger.orgapachekafka.appenderRef.console.ref = STDOUT
            logger.orgapachekafka.additivity = false
            
            logger.requestlogger.name = kafka.request.logger
            logger.requestlogger.level = WARN
            logger.requestlogger.appenderRefs = console
            logger.requestlogger.appenderRef.console.ref = STDOUT
            logger.requestlogger.additivity = false
            
            logger.requestchannel.name = kafka.network.RequestChannel$
            logger.requestchannel.level = WARN
            logger.requestchannel.appenderRefs = console
            logger.requestchannel.appenderRef.console.ref = STDOUT
            logger.requestchannel.additivity = false
            
            logger.controller.name = org.apache.kafka.controller
            logger.controller.level = INFO
            logger.controller.appenderRefs = console
            logger.controller.appenderRef.console.ref = STDOUT
            logger.controller.additivity = false
            
            logger.logcleaner.name = kafka.log.LogCleaner
            logger.logcleaner.level = INFO
            logger.logcleaner.appenderRefs = console
            logger.logcleaner.appenderRef.console.ref = STDOUT
            logger.logcleaner.additivity = false
            
            logger.statechange.name = state.change.logger
            logger.statechange.level = INFO
            logger.statechange.appenderRefs = console
            logger.statechange.appenderRef.console.ref = STDOUT
            logger.statechange.additivity = false
            
            logger.authorizer.name = kafka.authorizer.logger
            logger.authorizer.level = INFO
            logger.authorizer.appenderRefs = console
            logger.authorizer.appenderRef.console.ref = STDOUT
            logger.authorizer.additivity = false
            """;

        final String loggersConfigOperators = """
            appender.console.type=Console
            appender.console.name=STDOUT
            appender.console.layout.type=JsonTemplateLayout
            appender.console.layout.eventTemplate={"instant": {  "$resolver": "timestamp",  "pattern": {    "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",    "timeZone": "UTC"  }},"someConstant": 1,"message": {  "$resolver": "message",  "stringified": true}}
            rootLogger.level=INFO
            rootLogger.appenderRefs=stdout
            rootLogger.appenderRef.console.ref=STDOUT
            rootLogger.additivity=false""";

        final String loggersConfigCO = """
            name = COConfig
            appender.console.type = Console
            appender.console.name = STDOUT
            appender.console.layout.type = JsonTemplateLayout
            appender.console.layout.eventTemplate = {"instant": {  "$resolver": "timestamp",  "pattern": {    "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",    "timeZone": "UTC"  }},"someConstant": 1,"message": {  "$resolver": "message",  "stringified": true}}
            rootLogger.level = ${env:STRIMZI_LOG_LEVEL:-INFO}
            rootLogger.appenderRefs = stdout
            rootLogger.appenderRef.console.ref = STDOUT
            rootLogger.additivity = false
            logger.kafka.name = org.apache.kafka
            logger.kafka.level = ${env:STRIMZI_AC_LOG_LEVEL:-WARN}
            logger.kafka.additivity = false""";

        final String loggersConfigCC = """
            name=CCConfig
            appender.console.type=Console
            appender.console.name=STDOUT
            appender.console.layout.type=JsonTemplateLayout
            appender.console.layout.eventTemplate = {"instant": {  "$resolver": "timestamp",  "pattern": {    "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",    "timeZone": "UTC"  }},"someConstant": 1,"message": {  "$resolver": "message",  "stringified": true}}
            rootLogger.level=INFO
            rootLogger.appenderRefs=stdout
            rootLogger.appenderRef.console.ref=STDOUT
            rootLogger.additivity=false""";

        final String configMapOpName = "json-layout-operators";
        final String configMapKafkaName = "json-layout-kafka";
        final String configMapCCName = "json-layout-cc";
        final String configMapCOName = TestConstants.STRIMZI_DEPLOYMENT_NAME;

        String originalCoLoggers = KubeResourceManager.get().kubeClient().getClient().configMaps()
            .inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).withName(configMapCOName).get().getData().get("log4j2.properties");

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

        ConfigMapKeySelector operatorsLoggingCMselector = new ConfigMapKeySelectorBuilder()
            .withName(configMapOpName)
            .withKey("log4j2.properties")
            .build();

        ConfigMap configMapCO = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapCOName)
                // we are using this namespace because CO is deployed @BeforeAll
                .withNamespace(SetupClusterOperator.getInstance().getOperatorNamespace())
            .endMetadata()
            .addToData("log4j2.properties", loggersConfigCO)
            .build();

        ConfigMap configMapCC = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapCCName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addToData("log4j2.properties", loggersConfigCC)
            .build();

        ConfigMapKeySelector ccLoggingCMselector = new ConfigMapKeySelectorBuilder()
            .withName(configMapCCName)
            .withKey("log4j2.properties")
            .build();

        KubeResourceManager.get().createResourceWithWait(
            configMapKafka,
            configMapOperators,
            configMapCC
        );
        KubeResourceManager.get().updateResource(configMapCO);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );

        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .editKafka()
                    .withLogging(new ExternalLoggingBuilder()
                        .withNewValueFrom()
                            .withConfigMapKeyRef(kafkaLoggingCMselector)
                        .endValueFrom()
                        .build())
                .endKafka()
                .editEntityOperator()
                    .editTopicOperator()
                        .withLogging(new ExternalLoggingBuilder()
                            .withNewValueFrom()
                                .withConfigMapKeyRef(operatorsLoggingCMselector)
                            .endValueFrom()
                            .build())
                    .endTopicOperator()
                    .editUserOperator()
                        .withLogging(new ExternalLoggingBuilder()
                            .withNewValueFrom()
                                .withConfigMapKeyRef(operatorsLoggingCMselector)
                            .endValueFrom()
                            .build())
                    .endUserOperator()
                .endEntityOperator()
                .editCruiseControl()
                    .withLogging(new ExternalLoggingBuilder()
                        .withNewValueFrom()
                            .withConfigMapKeyRef(ccLoggingCMselector)
                        .endValueFrom()
                        .build())
                .endCruiseControl()
            .endSpec()
            .build());

        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), testStorage.getEoDeploymentName());
        Map<String, String> ccPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), CruiseControlResources.componentName(testStorage.getClusterName()));
        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName());

        StUtils.checkLogForJSONFormat(SetupClusterOperator.getInstance().getOperatorNamespace(), operatorSnapshot, SetupClusterOperator.getInstance().getOperatorDeploymentName(), StUtils.JSON_TEMPLATE_LAYOUT_PATTERN);
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), brokerPods, "", StUtils.JSON_TEMPLATE_LAYOUT_PATTERN);
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), controllerPods, "", StUtils.JSON_TEMPLATE_LAYOUT_PATTERN);
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), eoPods, "topic-operator", StUtils.JSON_TEMPLATE_LAYOUT_PATTERN);
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), eoPods, "user-operator", StUtils.JSON_TEMPLATE_LAYOUT_PATTERN);
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), ccPods, "cruise-control", StUtils.JSON_TEMPLATE_LAYOUT_PATTERN);

        // set loggers of CO back to original
        configMapCO.getData().put("log4j2.properties", originalCoLoggers);
        KubeResourceManager.get().updateResource(configMapCO);
    }

    @ParallelNamespaceTest
    @TestDoc(
            description = @Desc("Test verifying that the logging in JSON format works correctly across Kafka and operators."),
            steps = {
                @Step(value = "Assume non-Helm and non-OLM installation.", expected = "Assumption holds true."),
                @Step(value = "Create ConfigMaps for Kafka and operators with JSON logging configuration.", expected = "ConfigMaps created and applied."),
                @Step(value = "Deploy Kafka cluster with the configured logging setup.", expected = "Kafka cluster deployed successfully."),
                @Step(value = "Perform pod snapshot for controllers, brokers, and entity operators.", expected = "Pod snapshots successfully captured."),
                @Step(value = "Verify logs are in JSON format for all components.", expected = "Logs are in JSON format."),
                @Step(value = "Restore original logging configuration.", expected = "Original logging is restored.")
            },
            labels = {
                @Label(value = TestDocsLabels.KAFKA),
                @Label(value = TestDocsLabels.LOGGING)
            }
    )
    @SuppressWarnings("checkstyle:MethodLength")
    void testJSONFormatLogging() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // In this test scenario we change configuration for CO and we have to be sure, that CO is installed via YAML bundle instead of helm or OLM
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());

        String loggersConfigKafka = """
                name = KafkaConfig

                appender.console.type = Console
                appender.console.name = STDOUT
                appender.console.layout.type=JsonLayout
                
                rootLogger.level = INFO
                rootLogger.appenderRefs = console
                rootLogger.appenderRef.console.ref = STDOUT
                rootLogger.additivity = false
                
                logger.kafka.name = kafka
                logger.kafka.level = INFO
                logger.kafka.appenderRefs = console
                logger.kafka.appenderRef.console.ref = STDOUT
                logger.kafka.additivity = false
                
                logger.orgapachekafka.name = org.apache.kafka
                logger.orgapachekafka.level = INFO
                logger.orgapachekafka.appenderRefs = console
                logger.orgapachekafka.appenderRef.console.ref = STDOUT
                logger.orgapachekafka.additivity = false
                
                logger.requestlogger.name = kafka.request.logger
                logger.requestlogger.level = WARN
                logger.requestlogger.appenderRefs = console
                logger.requestlogger.appenderRef.console.ref = STDOUT
                logger.requestlogger.additivity = false
                
                logger.requestchannel.name = kafka.network.RequestChannel$
                logger.requestchannel.level = WARN
                logger.requestchannel.appenderRefs = console
                logger.requestchannel.appenderRef.console.ref = STDOUT
                logger.requestchannel.additivity = false
                
                logger.controller.name = org.apache.kafka.controller
                logger.controller.level = INFO
                logger.controller.appenderRefs = console
                logger.controller.appenderRef.console.ref = STDOUT
                logger.controller.additivity = false
                
                logger.logcleaner.name = kafka.log.LogCleaner
                logger.logcleaner.level = INFO
                logger.logcleaner.appenderRefs = console
                logger.logcleaner.appenderRef.console.ref = STDOUT
                logger.logcleaner.additivity = false
                
                logger.statechange.name = state.change.logger
                logger.statechange.level = INFO
                logger.statechange.appenderRefs = console
                logger.statechange.appenderRef.console.ref = STDOUT
                logger.statechange.additivity = false
                
                logger.authorizer.name = kafka.authorizer.logger
                logger.authorizer.level = INFO
                logger.authorizer.appenderRefs = console
                logger.authorizer.appenderRef.console.ref = STDOUT
                logger.authorizer.additivity = false
                """;

        String loggersConfigOperators = "appender.console.type=Console\n" +
            "appender.console.name=STDOUT\n" +
            "appender.console.layout.type=JsonLayout\n" +
            "rootLogger.level=INFO\n" +
            "rootLogger.appenderRefs=stdout\n" +
            "rootLogger.appenderRef.console.ref=STDOUT\n" +
            "rootLogger.additivity=false";

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
        String configMapKafkaName = "json-layout-kafka";
        String configMapCOName = TestConstants.STRIMZI_DEPLOYMENT_NAME;

        String originalCoLoggers = KubeResourceManager.get().kubeClient().getClient().configMaps()
            .inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).withName(configMapCOName).get().getData().get("log4j2.properties");

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

        ConfigMap configMapCO = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapCOName)
                // we are using this namespace because CO is deployed @BeforeAll
                .withNamespace(SetupClusterOperator.getInstance().getOperatorNamespace())
            .endMetadata()
            .addToData("log4j2.properties", loggersConfigCO)
            .build();

        KubeResourceManager.get().createResourceWithWait(
            configMapKafka,
            configMapOperators
        );
        KubeResourceManager.get().updateResource(configMapCO);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        Kafka kafka = KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .editKafka()
                    .withLogging(new ExternalLoggingBuilder()
                            .withNewValueFrom()
                                .withConfigMapKeyRef(kafkaLoggingCMselector)
                            .endValueFrom()
                            .build())
                .endKafka()
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

        KubeResourceManager.get().createResourceWithWait(kafka);

        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(testStorage.getClusterName()));
        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName());

        StUtils.checkLogForJSONFormat(SetupClusterOperator.getInstance().getOperatorNamespace(), operatorSnapshot, SetupClusterOperator.getInstance().getOperatorDeploymentName(), null);
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), brokerPods, "", null);
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), controllerPods, "", null);
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), eoPods, "topic-operator", null);
        StUtils.checkLogForJSONFormat(testStorage.getNamespaceName(), eoPods, "user-operator", null);

        // set loggers of CO back to original
        configMapCO.getData().put("log4j2.properties", originalCoLoggers);
        KubeResourceManager.get().updateResource(configMapCO);
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    @TestDoc(
        description = @Desc("Test verifying the dynamic update of logging levels for Entity Operator components."),
        steps = {
            @Step(value = "Deploy Kafka with Inline logging set to OFF.", expected = "Kafka deployed with no logs from Entity Operator."),
            @Step(value = "Set logging level to DEBUG using Inline logging.", expected = "Logs appear for Entity Operator with level DEBUG."),
            @Step(value = "Switch to external logging configuration with level OFF.", expected = "Logs from Entity Operator components are suppressed."),
            @Step(value = "Update external logging configuration to level DEBUG.", expected = "Logs appear again with level DEBUG.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testDynamicallySetEOloggingLevels() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        InlineLogging ilOff = new InlineLogging();
        ilOff.setLoggers(Collections.singletonMap("rootLogger.level", "OFF"));

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
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
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setLogging(ilDebug);
            k.getSpec().getEntityOperator().getUserOperator().setLogging(ilDebug);
        });

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                        && KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
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

        KubeResourceManager.get().createResourceWithWait(configMapTo, configMapUo);

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
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setLogging(elTo);
            k.getSpec().getEntityOperator().getUserOperator().setLogging(elUo);
        });

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                    && KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=OFF")
                    && KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
                    && KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("monitorInterval=30")
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

        KubeResourceManager.get().updateResource(configMapTo, configMapUo);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, eoPodName, "topic-operator", "cat", "/opt/topic-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                        && KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, eoPodName, "user-operator", "cat", "/opt/user-operator/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
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
    @TestDoc(
        description = @Desc("This test dynamically changes the logging levels of the Kafka Bridge and verifies that it behaves correctly in response to these changes."),
        steps = {
            @Step(value = "Configure the initial logging levels of the Kafka Bridge to OFF using inline logging.", expected = "Kafka Bridge logging levels are set to OFF."),
            @Step(value = "Asynchronously deploy the Kafka Bridge with the initial logging configuration, along with the required Kafka cluster and Scraper Pod.", expected = "Kafka Bridge and all required resources become ready."),
            @Step(value = "Verify that the Kafka Bridge logs are empty due to logging level being OFF.", expected = "Kafka Bridge logs are confirmed to be empty."),
            @Step(value = "Change the Kafka Bridge's rootLogger level to DEBUG using inline logging and apply the changes.", expected = "Kafka Bridge logging level is updated to DEBUG and reflected in its log4j2.properties."),
            @Step(value = "Verify that the Kafka Bridge logs now contain records at the DEBUG level.", expected = "Kafka Bridge logs contain expected DEBUG level records."),
            @Step(value = "Switch the Kafka Bridge to use an external logging configuration from a ConfigMap with rootLogger level set to OFF.", expected = "Kafka Bridge logging levels are updated to OFF using the external ConfigMap."),
            @Step(value = "Verify that the Kafka Bridge logs are empty again after applying the external logging configuration.", expected = "Kafka Bridge logs are confirmed to be empty."),
            @Step(value = "Ensure that the Kafka Bridge Pod did not restart or roll during the logging level changes.", expected = "Kafka Bridge Pod maintains its original state without restarting.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testDynamicallySetBridgeLoggingLevels() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        InlineLogging ilOff = new InlineLogging();
        Map<String, String> loggers = new HashMap<>();
        loggers.put("rootLogger.level", "OFF");
        loggers.put("logger.bridge.level", "OFF");
        loggers.put("logger.healthy.level", "OFF");
        loggers.put("logger.ready.level", "OFF");
        ilOff.setLoggers(loggers);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        // create resources async
        KubeResourceManager.get().createResourceWithoutWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaBridgeTemplates.kafkaBridge(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()), 1)
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

        KafkaBridgeUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            bridz -> bridz.getSpec().setLogging(ilDebug)
        );

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("rootLogger.level=DEBUG")
                && KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("monitorInterval=30")
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

        KubeResourceManager.get().createResourceWithWait(configMapBridge);

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
        KafkaBridgeUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            bridz -> bridz.getSpec().setLogging(bridgeXternalLogging)
        );

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("rootLogger.level = OFF")
                && KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(LogLevel.DEBUG, bridgePodName, KafkaBridgeResources.componentName(testStorage.getClusterName()), "cat", "/opt/strimzi/custom-config/log4j2.properties").out().contains("monitorInterval=30")
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
    @TestDoc(
        description = @Desc("Tests dynamic reconfiguration of logging levels for the Cluster Operator and ensures changes are applied correctly."),
        steps = {
            @Step(value = "Capture current Cluster Operator pod snapshot.", expected = "Snapshot of the current Cluster Operator pod is captured."),
            @Step(value = "Verify initial logging configuration differs from new setting.", expected = "Initial log configuration is not equal to the new configuration."),
            @Step(value = "Update ConfigMap with the new logging settings.", expected = "ConfigMap is updated with the new log settings."),
            @Step(value = "Wait for the Cluster Operator to apply the new logging settings.", expected = "Log configuration is updated in the Cluster Operator pod."),
            @Step(value = "Verify the Cluster Operator pod is rolled to apply new settings.", expected = "Cluster Operator pod is rolled successfully with the new config."),
            @Step(value = "Change logging levels from OFF to INFO.", expected = "Log levels in log4j2.properties are updated to INFO."),
            @Step(value = "Verify new INFO settings are applied correctly.", expected = "Updated log level is applied and visible in logs."),
            @Step(value = "Check for Cluster Operator pod roll after level update.", expected = "Cluster Operator pod roll is verified after changing log levels.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testDynamicallySetClusterOperatorLoggingLevels() {
        final Map<String, String> coPod = DeploymentUtils.depSnapshot(SetupClusterOperator.getInstance().getOperatorNamespace(), STRIMZI_DEPLOYMENT_NAME);
        final String coPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(SetupClusterOperator.getInstance().getOperatorNamespace(), STRIMZI_DEPLOYMENT_NAME).get(0).getMetadata().getName();
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
            "    logger.kafka.additivity = false";

        ConfigMap coMap = new ConfigMapBuilder()
            .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(STRIMZI_DEPLOYMENT_NAME)
                .withNamespace(SetupClusterOperator.getInstance().getOperatorNamespace())
            .endMetadata()
            .withData(Collections.singletonMap("log4j2.properties", log4jConfig))
            .build();

        LOGGER.info("Checking that original logging config is different from the new one");
        assertThat(log4jConfig, not(equalTo(KubeResourceManager.get().kubeCmdClient().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).execInPod(LogLevel.DEBUG, coPodName, "/bin/bash", "-c", command).out().trim())));

        LOGGER.info("Changing logging for cluster-operator");
        KubeResourceManager.get().updateResource(coMap);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).execInPod(LogLevel.DEBUG, coPodName, "/bin/bash", "-c", command).out().contains("rootLogger.level = OFF")
        );

        LOGGER.info("Checking log4j2.properties in CO Pod");
        String podLogConfig = KubeResourceManager.get().kubeCmdClient().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).execInPod(LogLevel.DEBUG, coPodName, "/bin/bash", "-c", command).out().trim();
        assertThat(podLogConfig, equalTo(log4jConfig));

        LOGGER.info("Checking if CO rolled its Pod");
        assertThat(coPod, equalTo(DeploymentUtils.depSnapshot(SetupClusterOperator.getInstance().getOperatorNamespace(), STRIMZI_DEPLOYMENT_NAME)));

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String coLog = StUtils.getLogFromPodByTime(SetupClusterOperator.getInstance().getOperatorNamespace(), coPodName, STRIMZI_DEPLOYMENT_NAME, "30s");
                LOGGER.debug(coLog);
                return coLog != null && coLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(coLog).find();
            });

        LOGGER.info("Changing all levels from OFF to INFO/WARN");
        log4jConfig = log4jConfig.replaceAll("OFF", "INFO");
        coMap.setData(Collections.singletonMap("log4j2.properties", log4jConfig));

        LOGGER.info("Changing logging for cluster-operator");
        KubeResourceManager.get().updateResource(coMap);

        LOGGER.info("Waiting for log4j2.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).execInPod(LogLevel.DEBUG, coPodName, "/bin/bash", "-c", command).out().contains("rootLogger.level = INFO")
        );

        LOGGER.info("Checking log4j2.properties in CO Pod");
        podLogConfig = KubeResourceManager.get().kubeCmdClient().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).execInPod(LogLevel.DEBUG, coPodName, "/bin/bash", "-c", command).out().trim();
        assertThat(podLogConfig, equalTo(log4jConfig));

        LOGGER.info("Checking if CO rolled its Pod");
        assertThat(coPod, equalTo(DeploymentUtils.depSnapshot(SetupClusterOperator.getInstance().getOperatorNamespace(), STRIMZI_DEPLOYMENT_NAME)));

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String coLog = StUtils.getLogFromPodByTime(SetupClusterOperator.getInstance().getOperatorNamespace(), coPodName, STRIMZI_DEPLOYMENT_NAME, "30s");
                return coLog != null && !coLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(coLog).find();
            });
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @TestDoc(
        description = @Desc("This test dynamically changes the logging levels of the Kafka Bridge and verifies the application behaves correctly with respect to these changes."),
        steps = {
            @Step(value = "Deploy Kafka cluster and KafkaConnect cluster, the latter with Log level Off.", expected = "Clusters are deployed with KafkaConnect log level set to Off."),
            @Step(value = "Deploy all additional resources, Scraper Pod and network policies (in order to gather the stuff from the Scraper Pod).", expected = "Additional resources, Scraper Pod and network policies are deployed."),
            @Step(value = "Verify that no logs are present in KafkaConnect Pods.", expected = "KafkaConnect Pods contain no logs."),
            @Step(value = "Set inline log level to Debug in KafkaConnect CustomResource.", expected = "Log level is set to Debug, and pods provide logs on Debug level."),
            @Step(value = "Change inline log level from Debug to Info in KafkaConnect CustomResource.", expected = "Log level is changed to Info, and pods provide logs on Info level."),
            @Step(value = "Create ConfigMap with necessary data for external logging and modify KafkaConnect CustomResource to use external logging setting log level Off.", expected = "Log level is set to Off using external logging, and pods provide no logs."),
            @Step(value = "Disable the use of connector resources via annotations and verify KafkaConnect pod rolls.", expected = "KafkaConnect deployment rolls and pod changes are verified."),
            @Step(value = "Enable DEBUG logging via inline configuration.", expected = "Log lines contain DEBUG level messages after configuration changes."),
            @Step(value = "Check the propagation of logging level change to DEBUG under the condition where connector resources are not used.", expected = "DEBUG logs are present and correct according to the log4j pattern.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    @SuppressWarnings("checkstyle:MethodLength")
    void testDynamicallyAndNonDynamicSetConnectLoggingLevels() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final Pattern log4jPatternDebugLevel = Pattern.compile("^(?<date>[\\d-]+) (?<time>[\\d:,]+) DEBUG (?<message>.+)");
        final Pattern log4jPatternInfoLevel = Pattern.compile("^(?<date>[\\d-]+) (?<time>[\\d:,]+) INFO (?<message>.+)");

        final InlineLogging inlineLogging = new InlineLoggingBuilder().withLoggers(Map.of("rootLogger.level", "OFF")).build();

        final int connectReplicas = 3;
        final KafkaConnect connect = KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), connectReplicas)
            .editSpec()
                .withLogging(inlineLogging)
            .endSpec()
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build(),
            connect,
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyUtils.deployNetworkPolicyForResource(connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        final String scraperPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        Map<String, String> connectPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());

        LOGGER.info("Asserting if logs in connect Pods are without records in last 60 seconds");
        final Predicate<String> logsDisabled = pod -> !DEFAULT_LOG4J_PATTERN.matcher(StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), pod, "", "60s")).find();
        connectPods.keySet().forEach(pod -> assertTrue(logsDisabled.test(pod)));

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");

        final InlineLogging inlineLoggingDebug = new InlineLoggingBuilder().withLoggers(Map.of("rootLogger.level", "DEBUG")).build();

        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), conn -> {
            conn.getSpec().setLogging(inlineLoggingDebug);
        });

        // check if lines from Logs contain messages with log level DEBUG
        Predicate<String> hasLogLevelDebug = connectLogs -> connectLogs != null && !connectLogs.isEmpty() && log4jPatternDebugLevel.matcher(connectLogs).find();
        KafkaConnectUtils.waitForConnectLogLevelChangePropagation(testStorage, connectPods, scraperPodName, hasLogLevelDebug, "DEBUG");

        assertThat("Connect Pod should not roll", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()), equalTo(connectPods));

        LOGGER.info("Changing rootLogger level from DEBUG to INFO with inline logging");

        final InlineLogging inlineLoggingInfo = new InlineLoggingBuilder().withLoggers(Map.of("rootLogger.level", "INFO")).build();

        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), conn -> {
            conn.getSpec().setLogging(inlineLoggingInfo);
        });

        // check if lines from Logs contain log Level INFO but no longer DEBUG
        final Predicate<String> hasLogLevelInfo = connectLogs -> connectLogs != null && !connectLogs.isEmpty() &&
            !log4jPatternDebugLevel.matcher(connectLogs).find() &&
            log4jPatternInfoLevel.matcher(connectLogs).find();
        KafkaConnectUtils.waitForConnectLogLevelChangePropagation(testStorage, connectPods, scraperPodName, hasLogLevelInfo, "INFO");

        assertThat("Connect Pod should not roll", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()), equalTo(connectPods));

        // external logging
        final String log4jConfig = """
                name = KafkaConnectConfig

                appender.console.type = Console
                appender.console.name = STDOUT
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n
                
                rootLogger.level = OFF
                rootLogger.appenderRefs = console
                rootLogger.appenderRef.console.ref = STDOUT
                rootLogger.additivity = false
                
                logger.reflections.name = org.reflections
                logger.reflections.level = ERROR
                logger.reflections.additivity = false
                """;

        final String externalCmName = "external-cm";

        final ConfigMap connectLoggingMap = new ConfigMapBuilder()
            .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .addToData("log4j.properties", log4jConfig)
            .build();

        KubeResourceManager.get().createResourceWithWait(connectLoggingMap);

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
        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), conn -> {
            conn.getSpec().setLogging(connectXternalLogging);
        });

        // check if there are no more new lines in Logs
        final Predicate<String> hasLogLevelOff = connectLogs -> connectLogs != null && connectLogs.isEmpty();
        KafkaConnectUtils.waitForConnectLogLevelChangePropagation(testStorage, connectPods, scraperPodName, hasLogLevelOff, "OFF");

        assertThat("Connect Pod should not roll", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()), equalTo(connectPods));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test dynamically changing Kafka logging levels using inline and external logging."),
        steps = {
            @Step(value = "Set logging levels to OFF using inline logging.", expected = "All logging levels in Kafka are set to OFF."),
            @Step(value = "Deploy Kafka cluster with the logging configuration.", expected = "Kafka cluster is deployed successfully with logging set to OFF."),
            @Step(value = "Verify that the logs are not empty and do not match default log pattern.", expected = "Logs are verified as expected."),
            @Step(value = "Change rootLogger level to DEBUG using inline logging.", expected = "Root logger level is changed to DEBUG dynamically."),
            @Step(value = "Verify log change in Kafka Pod.", expected = "Logs reflect DEBUG level setting."),
            @Step(value = "Set external logging configuration with INFO level.", expected = "External logging is configured with rootLogger set to INFO."),
            @Step(value = "Verify log change with external logging.", expected = "Logs reflect INFO level setting through external logging."),
            @Step(value = "Assert no rolling update of Kafka Pods occurred.", expected = "Pods did not roll due to logging changes.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    @SuppressWarnings("checkstyle:MethodLength")
    void testDynamicallySetKafkaLoggingLevels() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        Map<String, String> log4j2Config = new HashMap<>();
        log4j2Config.put("rootLogger.leve", "OFF");
        log4j2Config.put("logger.kafka.level", "OFF");
        log4j2Config.put("logger.orgapachekafka.level", "OFF");
        log4j2Config.put("logger.requestlogger.level", "OFF");
        log4j2Config.put("logger.requestchannel.level", "OFF");
        log4j2Config.put("logger.controller.level", "OFF");
        log4j2Config.put("logger.logcleaner.level", "OFF");
        log4j2Config.put("logger.statechange.level", "OFF");
        log4j2Config.put("logger.authorizer.level", "OFF");

        final InlineLogging inlineLogging = new InlineLoggingBuilder().withLoggers(log4j2Config).build();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .editKafka()
                        .withLogging(inlineLogging)
                    .endKafka()
                .endSpec()
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        int podNum = KafkaComponents.getPodNumFromPodName(testStorage.getBrokerComponentName(), brokerPodName);
        String scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                boolean[] correctLogging = {true};

                brokerPods.keySet().forEach(podName -> {
                    String kafkaLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), podName, "kafka", "30s");
                    correctLogging[0] = kafkaLog != null && kafkaLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(kafkaLog).find();
                });

                return correctLogging[0];
            });

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");

        final InlineLogging inlineLoggingDebug = new InlineLoggingBuilder().withLoggers(Map.of("rootLogger.level", "DEBUG")).build();

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setLogging(inlineLoggingDebug);
        });

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

        final String externalLogging = """
                name = KafkaConfig
                
                appender.console.type = Console
                appender.console.name = STDOUT
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n
                
                rootLogger.level = INFO
                rootLogger.appenderRefs = console
                rootLogger.appenderRef.console.ref = STDOUT
                rootLogger.additivity = false
                
                logger.kafka.name = kafka
                logger.kafka.level = INFO
                logger.kafka.appenderRefs = console
                logger.kafka.appenderRef.console.ref = STDOUT
                logger.kafka.additivity = false
                
                logger.orgapachekafka.name = org.apache.kafka
                logger.orgapachekafka.level = INFO
                logger.orgapachekafka.appenderRefs = console
                logger.orgapachekafka.appenderRef.console.ref = STDOUT
                logger.orgapachekafka.additivity = false
                
                logger.requestlogger.name = kafka.request.logger
                logger.requestlogger.level = WARN
                logger.requestlogger.appenderRefs = console
                logger.requestlogger.appenderRef.console.ref = STDOUT
                logger.requestlogger.additivity = false
                
                logger.requestchannel.name = kafka.network.RequestChannel$
                logger.requestchannel.level = WARN
                logger.requestchannel.appenderRefs = console
                logger.requestchannel.appenderRef.console.ref = STDOUT
                logger.requestchannel.additivity = false
                
                logger.controller.name = org.apache.kafka.controller
                logger.controller.level = INFO
                logger.controller.appenderRefs = console
                logger.controller.appenderRef.console.ref = STDOUT
                logger.controller.additivity = false
                
                logger.logcleaner.name = kafka.log.LogCleaner
                logger.logcleaner.level = INFO
                logger.logcleaner.appenderRefs = console
                logger.logcleaner.appenderRef.console.ref = STDOUT
                logger.logcleaner.additivity = false
                
                logger.statechange.name = state.change.logger
                logger.statechange.level = INFO
                logger.statechange.appenderRefs = console
                logger.statechange.appenderRef.console.ref = STDOUT
                logger.statechange.additivity = false
                
                logger.authorizer.name = kafka.authorizer.logger
                logger.authorizer.level = INFO
                logger.authorizer.appenderRefs = console
                logger.authorizer.appenderRef.console.ref = STDOUT
                logger.authorizer.additivity = false
                """;

        ConfigMap configMap = new ConfigMapBuilder()
            .withNewMetadata()
              .withName("external-configmap")
              .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(Collections.singletonMap("log4j.properties", externalLogging))
            .build();

        KubeResourceManager.get().createResourceWithWait(configMap);

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
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setLogging(elKafka);
        });

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
    @TestDoc(
        description = @Desc("Test dynamically setting an unknown Kafka logger in the resource."),
        steps = {
            @Step(value = "Retrieve the name of a broker pod and Scraper Pod.", expected = "Pod names are retrieved successfully."),
            @Step(value = "Take a snapshot of broker pod states.", expected = "Broker pod states are captured."),
            @Step(value = "Set new logger configuration with InlineLogging.", expected = "Logger is configured to 'log4j.logger.paprika'='INFO'."),
            @Step(value = "Wait for rolling update of broker components to finish.", expected = "Brokers are updated and rolled successfully."),
            @Step(value = "Verify that logger configuration has changed.", expected = "Logger 'paprika=INFO' is configured correctly.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testDynamicallySetUnknownKafkaLogger() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        int podNum = KafkaComponents.getPodNumFromPodName(testStorage.getBrokerComponentName(), brokerPodName);

        String scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        final InlineLogging inlineLogging = new InlineLoggingBuilder().withLoggers(Map.of("logger.paprika.name", "paprika", "logger.paprika.level", "INFO")).build();

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setLogging(inlineLogging);
        });

        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("paprika=INFO"));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test the dynamic setting of an unknown Kafka logger value without triggering a rolling update."),
        steps = {
            @Step(value = "Take a snapshot of current broker pods.", expected = "Snapshot of broker pods is captured successfully."),
            @Step(value = "Set Kafka root logger level to an unknown value 'PAPRIKA'.", expected = "Logger level is updated in the Kafka resource spec."),
            @Step(value = "Wait to ensure no rolling update is triggered for broker pods.", expected = "Kafka brokers do not roll."),
            @Step(value = "Assert that Kafka Pod should not roll.", expected = "Assertion confirms no rolling update was triggered.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testDynamicallySetUnknownKafkaLoggerValue() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        final InlineLogging inlineLogging = new InlineLoggingBuilder().withLoggers(Map.of("rootLogger.level", "PAPRIKA")).build();

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> {
            k.getSpec().getKafka().setLogging(inlineLogging);
        });

        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods);
        assertThat("Kafka Pod should not roll", RollingUpdateUtils.componentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods), is(false));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test dynamic updates to Kafka's external logging configuration to ensure that logging updates are applied correctly, with or without triggering a rolling update based on the nature of the changes."),
        steps = {
            @Step(value = "Create a ConfigMap 'external-cm' with initial logging settings for Kafka.", expected = "ConfigMap 'external-cm' is created with initial logging settings."),
            @Step(value = "Deploy a Kafka cluster configured to use the external logging ConfigMap, along with a Scraper Pod.", expected = "Kafka cluster and Scraper Pod are successfully deployed and become ready."),
            @Step(value = "Update the ConfigMap 'external-cm' to change logger levels that can be updated dynamically.", expected = "ConfigMap is updated, and dynamic logging changes are applied without triggering a rolling update."),
            @Step(value = "Verify that no rolling update occurs and that the new logger levels are active in the Kafka brokers.", expected = "No rolling update occurs; Kafka brokers reflect the new logger levels."),
            @Step(value = "Update the ConfigMap 'external-cm' to change logger settings that require a rolling update.", expected = "ConfigMap is updated with changes that cannot be applied dynamically."),
            @Step(value = "Verify that a rolling update occurs and that the new logging settings are applied after the restart.", expected = "Rolling update completes successfully; Kafka brokers are updated with the new logging settings.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    @SuppressWarnings("checkstyle:MethodLength")
    void testDynamicallySetKafkaExternalLogging() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        String externalLogging = """
                name = KafkaConfig
                
                appender.console.type = Console
                appender.console.name = STDOUT
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n
                
                rootLogger.level = INFO
                rootLogger.appenderRefs = console
                rootLogger.appenderRef.console.ref = STDOUT
                rootLogger.additivity = false
                
                logger.kafka.name = kafka
                logger.kafka.level = INFO
                logger.kafka.appenderRefs = console
                logger.kafka.appenderRef.console.ref = STDOUT
                logger.kafka.additivity = false
                
                logger.orgapachekafka.name = org.apache.kafka
                logger.orgapachekafka.level = INFO
                logger.orgapachekafka.appenderRefs = console
                logger.orgapachekafka.appenderRef.console.ref = STDOUT
                logger.orgapachekafka.additivity = false
                
                logger.requestlogger.name = kafka.request.logger
                logger.requestlogger.level = WARN
                logger.requestlogger.appenderRefs = console
                logger.requestlogger.appenderRef.console.ref = STDOUT
                logger.requestlogger.additivity = false
                
                logger.requestchannel.name = kafka.network.RequestChannel$
                logger.requestchannel.level = WARN
                logger.requestchannel.appenderRefs = console
                logger.requestchannel.appenderRef.console.ref = STDOUT
                logger.requestchannel.additivity = false
                
                logger.controller.name = org.apache.kafka.controller
                logger.controller.level = INFO
                logger.controller.appenderRefs = console
                logger.controller.appenderRef.console.ref = STDOUT
                logger.controller.additivity = false
                
                logger.logcleaner.name = kafka.log.LogCleaner
                logger.logcleaner.level = INFO
                logger.logcleaner.appenderRefs = console
                logger.logcleaner.appenderRef.console.ref = STDOUT
                logger.logcleaner.additivity = false
                
                logger.statechange.name = state.change.logger
                logger.statechange.level = INFO
                logger.statechange.appenderRefs = console
                logger.statechange.appenderRef.console.ref = STDOUT
                logger.statechange.additivity = false
                
                logger.authorizer.name = kafka.authorizer.logger
                logger.authorizer.level = INFO
                logger.authorizer.appenderRefs = console
                logger.authorizer.appenderRef.console.ref = STDOUT
                logger.authorizer.additivity = false
                """;

        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", externalLogging))
                .build();

        KubeResourceManager.get().createResourceWithWait(configMap);

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

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );

        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .editKafka()
                    .withLogging(el)
                .endKafka()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        int podNum = KafkaComponents.getPodNumFromPodName(testStorage.getBrokerComponentName(), brokerPodName);
        String scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        externalLogging = """
                name = KafkaConfig
                
                appender.console.type = Console
                appender.console.name = STDOUT
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n
                
                rootLogger.level = ERROR
                rootLogger.appenderRefs = console
                rootLogger.appenderRef.console.ref = STDOUT
                rootLogger.additivity = false
                
                logger.kafka.name = kafka
                logger.kafka.level = ERROR
                logger.kafka.appenderRefs = console
                logger.kafka.appenderRef.console.ref = STDOUT
                logger.kafka.additivity = false
                
                logger.orgapachekafka.name = org.apache.kafka
                logger.orgapachekafka.level = ERROR
                logger.orgapachekafka.appenderRefs = console
                logger.orgapachekafka.appenderRef.console.ref = STDOUT
                logger.orgapachekafka.additivity = false
                
                logger.requestlogger.name = kafka.request.logger
                logger.requestlogger.level = WARN
                logger.requestlogger.appenderRefs = console
                logger.requestlogger.appenderRef.console.ref = STDOUT
                logger.requestlogger.additivity = false
                
                logger.requestchannel.name = kafka.network.RequestChannel$
                logger.requestchannel.level = WARN
                logger.requestchannel.appenderRefs = console
                logger.requestchannel.appenderRef.console.ref = STDOUT
                logger.requestchannel.additivity = false
                
                logger.controller.name = org.apache.kafka.controller
                logger.controller.level = ERROR
                logger.controller.appenderRefs = console
                logger.controller.appenderRef.console.ref = STDOUT
                logger.controller.additivity = false
                
                logger.logcleaner.name = kafka.log.LogCleaner
                logger.logcleaner.level = ERROR
                logger.logcleaner.appenderRefs = console
                logger.logcleaner.appenderRef.console.ref = STDOUT
                logger.logcleaner.additivity = false
                
                logger.statechange.name = state.change.logger
                logger.statechange.level = ERROR
                logger.statechange.appenderRefs = console
                logger.statechange.appenderRef.console.ref = STDOUT
                logger.statechange.additivity = false
                
                logger.authorizer.name = kafka.authorizer.logger
                logger.authorizer.level = ERROR
                logger.authorizer.appenderRefs = console
                logger.authorizer.appenderRef.console.ref = STDOUT
                logger.authorizer.additivity = false
                """;

        configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("external-cm")
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", externalLogging))
                .build();

        KubeResourceManager.get().updateResource(configMap);
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods);
        assertThat("Kafka Pod should not roll", RollingUpdateUtils.componentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods), is(false));
        TestUtils.waitFor("Verify logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.describeKafkaBrokerLoggersUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), podNum).contains("kafka.authorizer.logger=ERROR"));
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    @TestDoc(
        description = @Desc("Test to dynamically set logging levels for MirrorMaker2 and verify the changes are applied without pod rolling."),
        steps = {
            @Step(value = "Set initial logging level to OFF and deploy resources.", expected = "Resources are deployed"),
            @Step(value = "Verify the log is empty with level OFF.", expected = "Log is confirmed empty."),
            @Step(value = "Change logging level to DEBUG dynamically.", expected = "Logging level changes to DEBUG without pod roll."),
            @Step(value = "Verify the log is not empty with DEBUG level.", expected = "Log contains entries with DEBUG level."),
            @Step(value = "Create external ConfigMap for logging configuration.", expected = "ConfigMap created with log4j settings."),
            @Step(value = "Apply external logging configuration with level OFF.", expected = "External logging configuration applied."),
            @Step(value = "Verify changes to OFF level are applied and no pod roll occurs.", expected = "Logging changes to OFF correctly without rolling the pod.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testDynamicallySetMM2LoggingLevels() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final InlineLogging inlineLoggingOff = new InlineLoggingBuilder().withLoggers(Map.of("rootLogger.level", "OFF", "logger.reflections.level", "OFF")).build();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build());

        KubeResourceManager.get().createResourceWithWait(
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), 1, false)
                .editOrNewSpec()
                    .withLogging(inlineLoggingOff)
                .endSpec()
                .build());

        String kafkaMM2PodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getMM2Selector()).get(0).getMetadata().getName();
        String mm2LogCheckCmd = "http://localhost:8083/admin/loggers/root";

        Map<String, String> mm2Snapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector());

        TestUtils.waitFor("log to be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String mmLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), kafkaMM2PodName, "", "30s");
                return mmLog != null && mmLog.isEmpty() && !DEFAULT_LOG4J_PATTERN.matcher(mmLog).find();
            });

        LOGGER.info("Changing rootLogger level to DEBUG with inline logging");

        final InlineLogging inlineLoggingDebug = new InlineLoggingBuilder().withLoggers(Map.of("rootLogger.level", "DEBUG")).build();

        KafkaMirrorMaker2Utils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            mm2 -> mm2.getSpec().setLogging(inlineLoggingDebug)
        );

        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, kafkaMM2PodName, "curl", mm2LogCheckCmd).out().contains("DEBUG")
        );

        TestUtils.waitFor("log to not be empty", Duration.ofMillis(100).toMillis(), TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> {
                String mmLog = StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), kafkaMM2PodName, "", "30s");
                return mmLog != null && !mmLog.isEmpty() && DEFAULT_LOG4J_PATTERN.matcher(mmLog).find();
            });

        final String log4jConfig = """
                name = KafkaMirrorMaker2Config

                appender.console.type = Console
                appender.console.name = STDOUT
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n
                
                rootLogger.level = OFF
                rootLogger.appenderRefs = console
                rootLogger.appenderRef.console.ref = STDOUT
                rootLogger.additivity = false
                
                logger.reflections.name = org.reflections
                logger.reflections.level = ERROR
                logger.reflections.additivity = false
                """;

        String externalCmName = "external-cm";

        ConfigMap mm2LoggingMap = new ConfigMapBuilder()
                .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        KubeResourceManager.get().createResourceWithWait(mm2LoggingMap);

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
        KafkaMirrorMaker2Utils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(),
            mm2 -> mm2.getSpec().setLogging(mm2XternalLogging)
        );

        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, kafkaMM2PodName, "curl", mm2LogCheckCmd).out().contains("OFF")
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
    @TestDoc(
        description = @Desc("Test to ensure logging levels in Kafka MirrorMaker2 follow a specific hierarchy."),
        steps = {
            @Step(value = "Set up source and target Kafka cluster.", expected = "Kafka clusters are deployed and ready."),
            @Step(value = "Configure Kafka MirrorMaker2 with initial logging level set to `OFF`.", expected = "Logging configuration is applied with log level 'OFF'."),
            @Step(value = "Verify initial logging levels in Kafka MirrorMaker2 pod.", expected = "Log level 'OFF' is confirmed."),
            @Step(value = "Update logging configuration to change log levels.", expected = "New logging configuration is applied."),
            @Step(value = "Verify updated logging levels in Kafka MirrorMaker2 pod.", expected = "Log levels 'INFO' and 'WARN' are correctly set as per hierarchy."),
            @Step(value = "Ensure Kafka MirrorMaker2 pod remains stable (no roll).", expected = "No unexpected pod restarts detected.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    @SuppressWarnings("checkstyle:MethodLength")
    void testMM2LoggingLevelsHierarchy() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getSourceBrokerPoolName(), testStorage.getSourceClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getSourceControllerPoolName(), testStorage.getSourceClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getSourceClusterName(), 1).build());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getTargetClusterName(), 1).build());

        String log4jConfig = """
                name = KafkaMirrorMaker2Config

                appender.console.type = Console
                appender.console.name = STDOUT
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n
                
                rootLogger.level = OFF
                rootLogger.appenderRefs = console
                rootLogger.appenderRef.console.ref = STDOUT
                rootLogger.additivity = false
                
                logger.reflections.name = org.reflections
                logger.reflections.level = ERROR
                logger.reflections.additivity = false
                
                logger.jettythread.name = org.eclipse.jetty.util.thread
                logger.jettythread.level = FATAL
                
                logger.workertask.name = org.apache.kafka.connect.runtime.WorkerTask
                logger.workertask.level = OFF
                
                logger.jettythreadstrategy.name = org.eclipse.jetty.util.thread.strategy.EatWhatYouKill
                logger.jettythreadstrategy.level = OFF
                """;

        String externalCmName = "external-cm-hierarchy";

        ConfigMap mm2LoggingMap = new ConfigMapBuilder()
                .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        KubeResourceManager.get().createResourceWithWait(mm2LoggingMap);

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

        KubeResourceManager.get().createResourceWithWait(
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getSourceClusterName(), testStorage.getTargetClusterName(), 1, false)
                .editOrNewSpec()
                    .withLogging(mm2XternalLogging)
                .endSpec()
                .build());

        String kafkaMM2PodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getMM2Selector()).get(0).getMetadata().getName();

        Map<String, String> mm2Snapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector());

        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger init levels", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/root").out().contains("OFF")
        );

        LOGGER.info("Changing log levels");

        log4jConfig = """
                name = KafkaMirrorMaker2Config

                appender.console.type = Console
                appender.console.name = STDOUT
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n
                
                rootLogger.level = INFO
                rootLogger.appenderRefs = console
                rootLogger.appenderRef.console.ref = STDOUT
                rootLogger.additivity = false
                
                logger.reflections.name = org.reflections
                logger.reflections.level = ERROR
                logger.reflections.additivity = false
                
                logger.jettythread.name = org.eclipse.jetty.util.thread
                logger.jettythread.level = WARN
                
                # To test the hierarchy, we keep the loggers and remove the level
                logger.workertask.name = org.apache.kafka.connect.runtime.WorkerTask
                logger.jettythreadstrategy.name = org.eclipse.jetty.util.thread.strategy.EatWhatYouKill
                """;

        mm2LoggingMap = new ConfigMapBuilder()
                .withNewMetadata()
                .addToLabels("app", "strimzi")
                .withName(externalCmName)
                .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .withData(Collections.singletonMap("log4j.properties", log4jConfig))
                .build();

        KubeResourceManager.get().updateResource(mm2LoggingMap);

        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/root").out().contains("INFO")
                    // not set logger should inherit parent level (in this case 'org.eclipse.jetty.util.thread')
                    && KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/org.eclipse.jetty.util.thread.strategy.EatWhatYouKill").out().contains("WARN")
                    // logger with not set parent should inherit root
                    && KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, kafkaMM2PodName, "curl", "http://localhost:8083/admin/loggers/org.apache.kafka.connect.runtime.WorkerTask").out().contains("INFO")
        );

        assertThat("MirrorMaker2 pod should not roll", PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMM2Selector()), equalTo(mm2Snapshot));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test that verifies Kafka falls back to default logging configuration when a specified ConfigMap does not exist."),
        steps = {
            @Step(value = "Create an external ConfigMap with custom logging.", expected = "ConfigMap is successfully created."),
            @Step(value = "Deploy Kafka with the external logging ConfigMap.", expected = "Kafka is deployed with the custom logging configuration."),
            @Step(value = "Verify log4j.properties on Kafka broker contains custom ConfigMap data.", expected = "Kafka broker uses custom logging settings."),
            @Step(value = "Update Kafka to use a non-existing ConfigMap for logging.", expected = "Kafka is updated with new logging configuration."),
            @Step(value = "Ensure there is no rolling update and verify configuration falls back to default.", expected = "No rolling update occurs, and default logging configuration is used."),
            @Step(value = "Check Kafka status for error message regarding non-existing ConfigMap.", expected = "Kafka status indicates error about missing ConfigMap.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testNotExistingCMSetsDefaultLogging() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final String defaultProps = ReadWriteUtils.readFile(TestUtils.USER_PATH + "/../cluster-operator/src/main/resources/default-logging/KafkaCluster.properties");
        final String propertiesFilename = "log4j2.properties";
        final String loggingConfiguration = """
                name = KafkaConfig
                
                appender.console.type = Console
                appender.console.name = STDOUT
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n
                
                rootLogger.level = INFO
                rootLogger.appenderRefs = console
                rootLogger.appenderRef.console.ref = STDOUT
                rootLogger.additivity = false
                
                logger.kafka.name = kafka
                logger.kafka.level = INFO
                logger.kafka.appenderRefs = console
                logger.kafka.appenderRef.console.ref = STDOUT
                logger.kafka.additivity = false
                
                logger.orgapachekafka.name = org.apache.kafka
                logger.orgapachekafka.level = INFO
                logger.orgapachekafka.appenderRefs = console
                logger.orgapachekafka.appenderRef.console.ref = STDOUT
                logger.orgapachekafka.additivity = false
                """;

        String existingCmName = "external-cm";
        String nonExistingCmName = "non-existing-cm-name";

        ConfigMap configMap = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(existingCmName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(Collections.singletonMap("log4j.properties", loggingConfiguration))
            .build();

        KubeResourceManager.get().createResourceWithWait(configMap);

        LOGGER.info("Deploying Kafka with custom logging");
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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
        String brokerPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        
        String log4jFile =  KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(
            LogLevel.DEBUG,
            brokerPodName,
            "kafka", "/bin/bash", "-c", "cat custom-config/" + propertiesFilename).out();
        assertTrue(log4jFile.contains(loggingConfiguration));

        LOGGER.info("Changing external logging's CM to not existing one");
        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> kafka.getSpec().getKafka().setLogging(
            new ExternalLoggingBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                            .withKey("log4j.properties")
                            .withName(nonExistingCmName)
                            .withOptional(false)
                            .build())
                .endValueFrom()
                .build()));

        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerPods);

        LOGGER.info("Checking that log4j.properties in custom-config isn't empty and configuration is default");
        log4jFile = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPodContainer(
            LogLevel.DEBUG,
            brokerPodName,
            "kafka", "/bin/bash", "-c", "cat custom-config/" + propertiesFilename).out();

        assertFalse(log4jFile.isEmpty());
        assertTrue(log4jFile.contains(loggingConfiguration));
        assertFalse(log4jFile.contains(defaultProps));

        LOGGER.info("Checking if Kafka: {} contains error about non-existing CM", testStorage.getClusterName());
        Condition condition = CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().filter(con -> CustomResourceStatus.NotReady.toString().equals(con.getType())).findFirst().orElse(null);
        assertThat(condition, is(notNullValue()));
        assertTrue(condition.getMessage().matches("ConfigMap " + nonExistingCmName + " with external logging configuration does not exist .*"));
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test to validate the logging hierarchy in Kafka Connect and Kafka Connector."),
        steps = {
            @Step(value = "Create an ephemeral Kafka cluster.", expected = "Ephemeral Kafka cluster is deployed successfully."),
            @Step(value = "Deploy KafkaConnect with File Plugin and configure loggers.", expected = "KafkaConnect is configured and deployed with inline logging set to ERROR for FileStreamSourceConnector."),
            @Step(value = "Apply network policies for KafkaConnect.", expected = "Network policies are successfully applied."),
            @Step(value = "Change rootLogger level to ERROR for KafkaConnector.", expected = "Logging for KafkaConnector is updated to ERROR."),
            @Step(value = "Restart KafkaConnector and verify its status.", expected = "KafkaConnector is restarted and running."),
            @Step(value = "Validate that the logger level change is retained.", expected = "Logger level is consistent with ERROR."),
            @Step(value = "Update KafkaConnect's root logger to WARN and check if KafkaConnector inherits it.", expected = "KafkaConnector logging remains independent of KafkaConnect root logger.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testLoggingHierarchy() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
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

        KubeResourceManager.get().createResourceWithWait(
            connect,
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getClusterName(), 1).build()
        );

        LOGGER.info("Deploying network policies for KafkaConnect");
        NetworkPolicyUtils.deployNetworkPolicyForResource(connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        String connectorClassName = "org.apache.kafka.connect.file.FileStreamSourceConnector";
        final String scraperPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Changing rootLogger level in KafkaConnector to ERROR with inline logging");
        final InlineLogging inlineLogging = new InlineLoggingBuilder().withLoggers(Map.of("logger.connector.name", connectorClassName, "logger.connector.level", "ERROR")).build();

        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kc -> kc.getSpec().setLogging(inlineLogging));

        LOGGER.info("Waiting for Connect API loggers will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, scraperPodName, "curl",
                "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/admin/loggers/" + connectorClassName).out().contains("ERROR")
        );

        LOGGER.info("Restarting KafkaConnector {} with class name {}", testStorage.getClusterName(), connectorClassName);
        KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, scraperPodName,
            "curl", "-X", "POST",
            "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/connectors/" + testStorage.getClusterName() + "/restart");

        KafkaConnectorUtils.waitForConnectorWorkerStatus(testStorage.getNamespaceName(), scraperPodName, testStorage.getClusterName(), testStorage.getClusterName(), "RUNNING");

        LOGGER.info("Checking that logger is same for connector with class name {}", connectorClassName);
        String connectorLogger = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, scraperPodName, "curl",
            "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/admin/loggers/" + connectorClassName).out();

        assertTrue(connectorLogger.contains("ERROR"));

        LOGGER.info("Changing KafkaConnect's root logger to WARN, KafkaConnector: {} shouldn't inherit it", testStorage.getClusterName());

        final InlineLogging inlineLoggingWithConnect = new InlineLoggingBuilder().withLoggers(Map.of("rootLogger.level", "WARN", "logger.connector.name", connectorClassName, "logger.connector.level", "ERROR")).build();

        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kc -> kc.getSpec().setLogging(inlineLoggingWithConnect));

        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(LogLevel.DEBUG, scraperPodName, "curl",
                "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/admin/loggers/root").out().contains("WARN")
        );

        LOGGER.info("Checking if KafkaConnector {} doesn't inherit logger from KafkaConnect", connectorClassName);

        KafkaConnectorUtils.loggerStabilityWait(testStorage.getNamespaceName(), testStorage.getClusterName(), scraperPodName, "ERROR", connectorClassName);
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @TestDoc(
        description = @Desc("This test case checks that changing Logging configuration from internal to external triggers Rolling Update."),
        steps = {
            @Step(value = "Deploy Kafka cluster, without any logging related configuration.", expected = "Cluster is deployed."),
            @Step(value = "Modify Kafka by changing specification of logging to new external value.", expected = "Change in logging specification triggers Rolling Update."),
            @Step(value = "Modify ConfigMap to new logging format.", expected = "Change in logging specification triggers Rolling Update.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.LOGGING)
        }
    )
    void testChangingInternalToExternalLoggingTriggerRollingUpdate() {
        // This test would need some changes since Kafka 4.0.0 uses Log4j2 which provides dynamic logging changes for the brokers and is therefore disabled.
        // This test can be enabled again once we fix the issue: https://github.com/strimzi/strimzi-kafka-operator/issues/11312
        Assumptions.assumeTrue(TestKafkaVersion.compareDottedVersions(Environment.ST_KAFKA_VERSION, "4.0.0") < 0);

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // EO dynamic logging is tested in io.strimzi.systemtest.log.LoggingChangeST.testDynamicallySetEOloggingLevels
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());

        final String loggersConfig = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
            "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
            "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]\n" +
            "kafka.root.logger.level=INFO\n" +
            "log4j.rootLogger=${kafka.root.logger.level}, CONSOLE\n" +
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

        KubeResourceManager.get().createResourceWithWait(configMapLoggers);

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setLogging(new ExternalLoggingBuilder()
                .withNewValueFrom()
                .withConfigMapKeyRef(log4jLoggimgCMselector)
                .endValueFrom()
                .build());
        });

        LOGGER.info("Waiting for controller pods to roll after change in logging");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);

        LOGGER.info("Waiting for broker pods to roll after change in logging");
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        configMapLoggers.getData().put("log4j-custom.properties", loggersConfig.replace("%p %m (%c) [%t]", "%p %m (%c) [%t]%n"));
        KubeResourceManager.get().updateResource(configMapLoggers);

        LOGGER.info("Waiting for controller pods to roll after change in logging properties config map");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);

        LOGGER.info("Waiting for broker pods to roll after change in logging properties config map");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }
}
