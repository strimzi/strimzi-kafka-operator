/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.MavenArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.OtherArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.connect.build.TgzArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.ZipArtifactBuilder;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.KindNotSupported;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.SANITY;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@Tag(CONNECT_COMPONENTS)
@Tag(CONNECT)
@MicroShiftNotSupported
@SuiteDoc(
    description = @Desc("Testing Kafka Connect build and plugin management."),
    labels = {
        @Label(value = TestDocsLabels.CONNECT),
    }
)
class ConnectBuilderST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConnectBuilderST.class);

    private static final String CAMEL_CONNECTOR_HTTP_SINK_CLASS_NAME = "org.apache.camel.kafkaconnector.http.CamelHttpSinkConnector";
    private static final String CAMEL_CONNECTOR_TIMER_CLASS_NAME = "org.apache.camel.kafkaconnector.timer.CamelTimerSourceConnector";
    private static final String CAMEL_CONNECTOR_TGZ_URL = "https://repo.maven.apache.org/maven2/org/apache/camel/kafkaconnector/camel-http-kafka-connector/0.7.0/camel-http-kafka-connector-0.7.0-package.tar.gz";
    private static final String CAMEL_CONNECTOR_TGZ_CHECKSUM = "d0bb8c6a9e50b68eee3e4d70b6b7e5ae361373883ed3156bc11771330095b66195ac1c12480a0669712da4e5f38e64f004ffecabca4bf70d312f3f7ae0ad51b5";
    private static final String CAMEL_CONNECTOR_ZIP_URL = "https://repo.maven.apache.org/maven2/org/apache/camel/kafkaconnector/camel-http-kafka-connector/0.7.0/camel-http-kafka-connector-0.7.0-package.zip";
    private static final String CAMEL_CONNECTOR_ZIP_CHECKSUM = "bc15135b8ef7faccd073508da0510c023c0f6fa3ec7e48c98ad880dd112b53bf106ad0a47bcb353eed3ec03bb3d273da7de356f3f7f1766a13a234a6bc28d602";
    private static final String CAMEL_CONNECTOR_MAVEN_GROUP_ID = "org.apache.camel.kafkaconnector";
    private static final String CAMEL_CONNECTOR_MAVEN_ARTIFACT_ID = "camel-timer-kafka-connector";
    private static final String CAMEL_CONNECTOR_MAVEN_VERSION = "0.9.0";

    private static final String PLUGIN_WITH_TAR_AND_JAR_NAME = "connector-with-tar-and-jar";
    private static final String PLUGIN_WITH_ZIP_NAME = "connector-from-zip";
    private static final String PLUGIN_WITH_OTHER_TYPE_NAME = "plugin-with-other-type";
    private static final String PLUGIN_WITH_MAVEN_NAME = "connector-from-maven";

    private TestStorage suiteTestStorage;

    private static final Plugin PLUGIN_WITH_TAR_AND_JAR = new PluginBuilder()
        .withName(PLUGIN_WITH_TAR_AND_JAR_NAME)
        .withArtifacts(
            new JarArtifactBuilder()
                .withUrl(TestConstants.ECHO_SINK_JAR_URL)
                .withSha512sum(TestConstants.ECHO_SINK_JAR_CHECKSUM)
                .build(),
            new TgzArtifactBuilder()
                .withUrl(TestConstants.ECHO_SINK_TGZ_URL)
                .withSha512sum(TestConstants.ECHO_SINK_TGZ_CHECKSUM)
                .build())
        .build();

    private static final Plugin PLUGIN_WITH_ZIP = new PluginBuilder()
        .withName(PLUGIN_WITH_ZIP_NAME)
        .withArtifacts(
            new ZipArtifactBuilder()
                .withUrl(CAMEL_CONNECTOR_ZIP_URL)
                .withSha512sum(CAMEL_CONNECTOR_ZIP_CHECKSUM)
                .build())
        .build();

    private static final Plugin PLUGIN_WITH_OTHER_TYPE = new PluginBuilder()
        .withName(PLUGIN_WITH_OTHER_TYPE_NAME)
        .withArtifacts(
            new OtherArtifactBuilder()
                .withUrl(TestConstants.ECHO_SINK_JAR_URL)
                .withFileName(TestConstants.ECHO_SINK_FILE_NAME)
                .withSha512sum(TestConstants.ECHO_SINK_JAR_CHECKSUM)
                .build()
        )
        .build();

    private static final Plugin PLUGIN_WITH_MAVEN_TYPE = new PluginBuilder()
        .withName(PLUGIN_WITH_MAVEN_NAME)
        .withArtifacts(
            new MavenArtifactBuilder()
                .withVersion(CAMEL_CONNECTOR_MAVEN_VERSION)
                .withArtifact(CAMEL_CONNECTOR_MAVEN_ARTIFACT_ID)
                .withGroup(CAMEL_CONNECTOR_MAVEN_GROUP_ID)
                .build()
        )
        .build();

    @ParallelTest
    @TestDoc(
        description = @Desc("Test that ensures Kafka Connect build fails with wrong artifact checksum and recovers with correct checksum."),
        steps = {
            @Step(value = "Initialize TestStorage and get test image name.", expected = "TestStorage instance is created and the image name for the test case is retrieved."),
            @Step(value = "Create a Plugin with wrong checksum and build Kafka Connect resource with it.", expected = "Kafka Connect resource is created but the build fails due to wrong checksum."),
            @Step(value = "Deploy Scraper pod with specific configurations.", expected = "Kafka Scraper pod are successfully deployed."),
            @Step(value = "Wait for Kafka Connect status to indicate build failure.", expected = "Kafka Connect status contains message about build failure."),
            @Step(value = "Deploy network policies for Kafka Connect.", expected = "Network policies are successfully deployed for Kafka Connect."),
            @Step(value = "Replace the plugin checksum with the correct one and update Kafka Connect resource.", expected = "Kafka Connect resource is updated with the correct checksum."),
            @Step(value = "Wait for Kafka Connect to be ready.", expected = "Kafka Connect becomes ready."),
            @Step(value = "Verify that EchoSink KafkaConnector is available in Kafka Connect API.", expected = "EchoSink KafkaConnector is returned by Kafka Connect API."),
            @Step(value = "Verify that EchoSink KafkaConnector is listed in Kafka Connect resource status", expected = "EchoSink KafkaConnector is listed in the status of Kafka Connect resource")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testBuildFailsWithWrongChecksumOfArtifact() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final String imageName = getImageNameForTestCase();

        Plugin pluginWithWrongChecksum = new PluginBuilder()
            .withName("connector-with-wrong-checksum")
            .withArtifacts(new JarArtifactBuilder()
                .withUrl(TestConstants.ECHO_SINK_JAR_URL)
                .withSha512sum(TestConstants.ECHO_SINK_JAR_WRONG_CHECKSUM)
                .build())
            .build();

        KubeResourceManager.get().createResourceWithWait(ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        KubeResourceManager.get().createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), suiteTestStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .withNewBuild()
                    .withPlugins(pluginWithWrongChecksum)
                    .withOutput(KafkaConnectTemplates.dockerOutput(imageName))
                .endBuild()
            .endSpec()
            .build());

        KafkaConnectUtils.waitForConnectNotReady(testStorage.getNamespaceName(), testStorage.getClusterName());
        KafkaConnectUtils.waitUntilKafkaConnectStatusConditionContainsMessage(testStorage.getNamespaceName(), testStorage.getClusterName(), "The Kafka Connect build failed(.*)?");

        LOGGER.info("Checking if KafkaConnect status condition contains message about build failure");
        KafkaConnect kafkaConnect = CrdClients.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

        LOGGER.info("Deploying network policies for KafkaConnect");
        NetworkPolicyUtils.deployNetworkPolicyForResource(kafkaConnect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        Condition connectCondition = kafkaConnect.getStatus().getConditions().stream().findFirst().orElseThrow();

        assertTrue(connectCondition.getMessage().matches("The Kafka Connect build failed(.*)?"));
        assertThat(connectCondition.getType(), is(NotReady.toString()));

        LOGGER.info("Replacing plugin's checksum with right one");
        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kC -> {
            Plugin pluginWithRightChecksum = new PluginBuilder()
                .withName("connector-with-right-checksum")
                .withArtifacts(new JarArtifactBuilder()
                    .withUrl(TestConstants.ECHO_SINK_JAR_URL)
                    .withSha512sum(TestConstants.ECHO_SINK_JAR_CHECKSUM)
                    .build())
                .build();

            kC.getSpec().getBuild().getPlugins().remove(0);
            kC.getSpec().getBuild().getPlugins().add(pluginWithRightChecksum);
        });

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        String scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Checking if KafkaConnect API contains EchoSink KafkaConnector");
        String plugins = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl", "-X", "GET", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/connector-plugins").out();

        assertTrue(plugins.contains(TestConstants.ECHO_SINK_CLASS_NAME));

        LOGGER.info("Checking if KafkaConnect resource contains EchoSink KafkaConnector in status");
        kafkaConnect = CrdClients.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(TestConstants.ECHO_SINK_CLASS_NAME)));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test for building Kafka Connect image with combined jar, tar.gz, and zip plugins, and validating message send-receive functionality."),
        steps = {
            @Step(value = "Create TestStorage object", expected = "TestStorage instance is created with context"),
            @Step(value = "Get image name for test case", expected = "Image name is successfully retrieved"),
            @Step(value = "Create Kafka Topic resources", expected = "Kafka Topic resources are created with wait"),
            @Step(value = "Create Kafka Connect resources", expected = "Kafka Connect resources are created with wait"),
            @Step(value = "Configure Kafka Connector", expected = "Kafka Connector is configured and created with wait"),
            @Step(value = "Verify Kafka Connector class name", expected = "Connector class name matches expected ECHO_SINK_CLASS_NAME"),
            @Step(value = "Create Kafka Clients and send messages", expected = "Kafka Clients created and messages sent and verified"),
            @Step(value = "Check logs for received message", expected = "Logs contain the expected received message")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testBuildWithJarTgzAndZip() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // this test also testing push into Docker output
        final String imageName = getImageNameForTestCase();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());
        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), suiteTestStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewBuild()
                    .withPlugins(PLUGIN_WITH_TAR_AND_JAR, PLUGIN_WITH_ZIP)
                    .withOutput(KafkaConnectTemplates.dockerOutput(imageName))
                .endBuild()
                .withNewInlineLogging()
                    .addToLoggers("rootLogger.level", "INFO")
                .endInlineLogging()
            .endSpec()
            .build());

        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", testStorage.getTopicName());
        connectorConfig.put("level", "INFO");

        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), testStorage.getClusterName())
            .editOrNewSpec()
                .withClassName(TestConstants.ECHO_SINK_CLASS_NAME)
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        KafkaConnector kafkaConnector = CrdClients.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();
        assertThat(kafkaConnector.getSpec().getClassName(), is(TestConstants.ECHO_SINK_CLASS_NAME));

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        String connectPodName = PodUtils.listPodNames(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(testStorage.getNamespaceName(), connectPodName, "Received message with key 'null' and value 'Hello-world - 99'");
    }

    @OpenShiftOnly
    @ParallelTest
    @TestDoc(
        description = @Desc("Test verifying the successful push of a KafkaConnect build into an OpenShift ImageStream."),
        steps = {
            @Step(value = "Initialize test storage", expected = "Test storage is initialized with the test context"),
            @Step(value = "Create ImageStream", expected = "ImageStream is created in the specified namespace"),
            @Step(value = "Deploy KafkaConnect with the image stream output", expected = "KafkaConnect is deployed with the expected build configuration"),
            @Step(value = "Verify KafkaConnect build artifacts and status", expected = "KafkaConnect has two plugins, uses the image stream output and is in the 'Ready' state")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testPushIntoImageStream() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        String imageStreamName = "custom-image-stream";
        ImageStream imageStream = new ImageStreamBuilder()
            .editOrNewMetadata()
                .withName(imageStreamName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build();

        KubeResourceManager.get().kubeClient().getClient().adapt(OpenShiftClient.class).imageStreams().inNamespace(testStorage.getNamespaceName()).resource(imageStream).create();

        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), suiteTestStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .withNewBuild()
                    .withPlugins(PLUGIN_WITH_TAR_AND_JAR)
                    .withNewImageStreamOutput()
                        .withImage(imageStreamName + ":latest")
                    .endImageStreamOutput()
                .endBuild()
            .endSpec()
            .build());

        KafkaConnect kafkaConnect = CrdClients.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

        LOGGER.info("Checking, if KafkaConnect has all artifacts and if is successfully created");
        assertThat(kafkaConnect.getSpec().getBuild().getPlugins().get(0).getArtifacts().size(), is(2));
        assertThat(kafkaConnect.getSpec().getBuild().getOutput().getType(), is("imagestream"));
        assertThat(kafkaConnect.getSpec().getBuild().getOutput().getImage(), is(imageStreamName + ":latest"));
        assertThat(kafkaConnect.getStatus().getConditions().get(0).getType(), is(Ready.toString()));

        assertFalse(kafkaConnect.getStatus().getConnectorPlugins().isEmpty());
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(TestConstants.ECHO_SINK_CLASS_NAME)));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test updating and validating Kafka Connect with another plugin."),
        steps = {
            @Step(value = "Create TestStorage instance", expected = "Instance of TestStorage is created"),
            @Step(value = "Generate random topic name and create Kafka topic", expected = "Kafka topic is successfully created"),
            @Step(value = "Deploy network policies for KafkaConnect", expected = "Network policies are successfully deployed"),
            @Step(value = "Create EchoSink KafkaConnector", expected = "EchoSink KafkaConnector is successfully created and validated"),
            @Step(value = "Add a second plugin to Kafka Connect and perform rolling update", expected = "Second plugin is added and rolling update is performed"),
            @Step(value = "Create Camel-HTTP-Sink KafkaConnector", expected = "Camel-HTTP-Sink KafkaConnector is successfully created and validated"),
            @Step(value = "Verify that both connectors and plugins are present in Kafka Connect", expected = "Both connectors and plugins are verified successfully")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testUpdateConnectWithAnotherPlugin() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        String camelConnector = "camel-http-connector";
        final String imageName = getImageNameForTestCase();

        Plugin secondPlugin =  new PluginBuilder()
            .withName("camel-connector")
            .withArtifacts(
                new TgzArtifactBuilder()
                    .withUrl(CAMEL_CONNECTOR_TGZ_URL)
                    .withSha512sum(CAMEL_CONNECTOR_TGZ_CHECKSUM)
                    .build())
            .build();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), suiteTestStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewBuild()
                    .withPlugins(PLUGIN_WITH_TAR_AND_JAR)
                    .withOutput(KafkaConnectTemplates.dockerOutput(imageName))
                .endBuild()
            .endSpec()
            .build();

        KubeResourceManager.get().createResourceWithWait(connect, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyUtils.deployNetworkPolicyForResource(connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        Map<String, Object> echoSinkConfig = new HashMap<>();
        echoSinkConfig.put("topics", testStorage.getTopicName());
        echoSinkConfig.put("level", "INFO");

        LOGGER.info("Creating EchoSink KafkaConnector");
        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), TestConstants.ECHO_SINK_CONNECTOR_NAME, testStorage.getClusterName())
            .editOrNewSpec()
                .withClassName(TestConstants.ECHO_SINK_CLASS_NAME)
                .withConfig(echoSinkConfig)
            .endSpec()
            .build());

        Map<String, String> connectSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());
        String scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Checking that KafkaConnect API contains EchoSink KafkaConnector and not Camel-Telegram Connector class name");
        String plugins = KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl", "-X", "GET", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/connector-plugins").out();

        assertFalse(plugins.contains(CAMEL_CONNECTOR_HTTP_SINK_CLASS_NAME));
        assertTrue(plugins.contains(TestConstants.ECHO_SINK_CLASS_NAME));

        LOGGER.info("Adding one more connector to the KafkaConnect");
        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaConnect -> {
            kafkaConnect.getSpec().getBuild().getPlugins().add(secondPlugin);
        });

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 1, connectSnapshot);

        Map<String, Object> camelHttpConfig = new HashMap<>();
        camelHttpConfig.put("camel.sink.path.httpUri", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083");
        camelHttpConfig.put("topics", testStorage.getTopicName());

        LOGGER.info("Creating Camel-HTTP-Sink KafkaConnector");
        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), camelConnector, testStorage.getClusterName())
            .editOrNewSpec()
                .withClassName(CAMEL_CONNECTOR_HTTP_SINK_CLASS_NAME)
                .withConfig(camelHttpConfig)
            .endSpec()
            .build());

        KafkaConnectUtils.waitForConnectStatusContainsPlugins(testStorage.getNamespaceName(), testStorage.getClusterName());

        KafkaConnect kafkaConnect = CrdClients.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

        LOGGER.info("Checking if both Connectors were created and Connect contains both plugins");
        assertThat(kafkaConnect.getSpec().getBuild().getPlugins().size(), is(2));

        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(TestConstants.ECHO_SINK_CLASS_NAME)));
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(CAMEL_CONNECTOR_HTTP_SINK_CLASS_NAME)));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test verifying Kafka Connect plugin behavior with and without file names for different plugin types."),
        steps = {
            @Step(value = "Initialize test storage and topic", expected = "Namespace and topic are created successfully"),
            @Step(value = "Create and set up Kafka Connect with specified plugin and build configurations", expected = "Kafka Connect is deployed and configured correctly"),
            @Step(value = "Take a snapshot of current Kafka Connect pods and verify plugin file name", expected = "Plugin file name matches the expected file name"),
            @Step(value = "Modify Kafka Connect to use a plugin without a file name and trigger a rolling update", expected = "Kafka Connect plugin is updated without the file name successfully"),
            @Step(value = "Verify plugin file name after update using the plugin's hash", expected = "Plugin file name is different from the previous name and matches the hash")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testBuildOtherPluginTypeWithAndWithoutFileName() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final String imageName = getImageNameForTestCase();

        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicName, suiteTestStorage.getClusterName()).build());

        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), suiteTestStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editOrNewSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewBuild()
                        .withPlugins(PLUGIN_WITH_OTHER_TYPE)
                        .withOutput(KafkaConnectTemplates.dockerOutput(imageName))
                .endBuild()
            .endSpec()
            .build());

        Map<String, String> connectSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());
        String connectPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()).get(0).getMetadata().getName();

        LOGGER.info("Checking that plugin has correct file name: {}", TestConstants.ECHO_SINK_FILE_NAME);
        assertEquals(TestConstants.ECHO_SINK_FILE_NAME, getPluginFileNameFromConnectPod(testStorage.getNamespaceName(), connectPodName));

        final Plugin pluginWithoutFileName = new PluginBuilder()
            .withName(PLUGIN_WITH_OTHER_TYPE_NAME)
            .withArtifacts(
                new OtherArtifactBuilder()
                    .withUrl(TestConstants.ECHO_SINK_JAR_URL)
                    .withSha512sum(TestConstants.ECHO_SINK_JAR_CHECKSUM)
                    .build()
            )
            .build();

        LOGGER.info("Removing file name from the plugin, hash should be used");
        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), connect -> {
            connect.getSpec().getBuild().setPlugins(Collections.singletonList(pluginWithoutFileName));
        });

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 1, connectSnapshot);

        LOGGER.info("Checking that plugin has different name than before");
        connectPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()).get(0).getMetadata().getName();
        String fileName = getPluginFileNameFromConnectPod(testStorage.getNamespaceName(), connectPodName);
        assertNotEquals(TestConstants.ECHO_SINK_FILE_NAME, fileName);
        assertEquals(fileName, Util.hashStub(TestConstants.ECHO_SINK_JAR_URL));
    }

    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    @KindNotSupported("using kind we encounter (error building image: deleting file system after stage 0: unlinkat //product_uuid: device or resource busy)")
    @ParallelTest
    @TestDoc(
        description = @Desc("Test building a plugin using Maven coordinates artifacts."),
        steps = {
            @Step(value = "Create a test storage object", expected = "Test storage object is created"),
            @Step(value = "Generate image name for the test case", expected = "Image name is generated successfully"),
            @Step(value = "Create Kafka topic and Kafka Connect resources with the configuration for plugin using mvn coordinates", expected = "Resources are created and available"),
            @Step(value = "Configure Kafka Connector and deploy it", expected = "Connector is deployed with correct configuration"),
            @Step(value = "Create Kafka consumer and start consuming messages", expected = "Consumer starts consuming messages successfully"),
            @Step(value = "Verify that consumer receives messages", expected = "Consumer receives the expected messages")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testBuildPluginUsingMavenCoordinatesArtifacts() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final String imageName = getImageNameForTestCase();
        final String connectorName = testStorage.getClusterName() + "-camel-connector";

        KubeResourceManager.get().createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), suiteTestStorage.getClusterName()).build(),
            KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), suiteTestStorage.getClusterName(), 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editOrNewSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewBuild()
                        .withPlugins(PLUGIN_WITH_MAVEN_TYPE)
                        .withOutput(KafkaConnectTemplates.dockerOutput(imageName))
                    .endBuild()
                .endSpec()
                .build());

        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", testStorage.getTopicName());
        connectorConfig.put("camel.source.path.timerName", "timer");

        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), connectorName, testStorage.getClusterName())
            .editOrNewSpec()
                .withClassName(CAMEL_CONNECTOR_TIMER_CLASS_NAME)
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        final KafkaClients kafkaClient = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        KubeResourceManager.get().createResourceWithWait(kafkaClient.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    private String getPluginFileNameFromConnectPod(final String namespaceName, final String connectPodName) {
        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(connectPodName,
            "/bin/bash", "-c", "ls plugins/plugin-with-other-type/*").out().trim();
    }

    private String getImageNameForTestCase() {
        int randomNum = new Random().nextInt(Integer.MAX_VALUE);
        return Environment.getImageOutputRegistry(Environment.TEST_SUITE_NAMESPACE, TestConstants.ST_CONNECT_BUILD_IMAGE_NAME, String.valueOf(randomNum));

    }

    @BeforeAll
    void setup() {
        suiteTestStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_SHORT)
                .build()
            )
            .install();

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 3).build());
    }
}
