/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
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
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.KindNotSupported;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
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
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.SANITY;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
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
    void testBuildFailsWithWrongChecksumOfArtifact() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String imageName = getImageNameForTestCase();

        Plugin pluginWithWrongChecksum = new PluginBuilder()
            .withName("connector-with-wrong-checksum")
            .withArtifacts(new JarArtifactBuilder()
                .withUrl(TestConstants.ECHO_SINK_JAR_URL)
                .withSha512sum(TestConstants.ECHO_SINK_JAR_WRONG_CHECKSUM)
                .build())
            .build();

        resourceManager.createResourceWithWait(ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1)
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
        KafkaConnectUtils.waitUntilKafkaConnectStatusConditionContainsMessage(testStorage.getClusterName(), testStorage.getNamespaceName(), "The Kafka Connect build failed(.*)?");

        LOGGER.info("Checking if KafkaConnect status condition contains message about build failure");
        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

        LOGGER.info("Deploying network policies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(kafkaConnect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        Condition connectCondition = kafkaConnect.getStatus().getConditions().stream().findFirst().orElseThrow();

        assertTrue(connectCondition.getMessage().matches("The Kafka Connect build failed(.*)?"));
        assertThat(connectCondition.getType(), is(NotReady.toString()));

        LOGGER.info("Replacing plugin's checksum with right one");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kC -> {
            Plugin pluginWithRightChecksum = new PluginBuilder()
                .withName("connector-with-right-checksum")
                .withArtifacts(new JarArtifactBuilder()
                    .withUrl(TestConstants.ECHO_SINK_JAR_URL)
                    .withSha512sum(TestConstants.ECHO_SINK_JAR_CHECKSUM)
                    .build())
                .build();

            kC.getSpec().getBuild().getPlugins().remove(0);
            kC.getSpec().getBuild().getPlugins().add(pluginWithRightChecksum);
        }, testStorage.getNamespaceName());

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        String scraperPodName = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Checking if KafkaConnect API contains EchoSink KafkaConnector");
        String plugins = cmdKubeClient(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl", "-X", "GET", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/connector-plugins").out();

        assertTrue(plugins.contains(TestConstants.ECHO_SINK_CLASS_NAME));

        LOGGER.info("Checking if KafkaConnect resource contains EchoSink KafkaConnector in status");
        kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(TestConstants.ECHO_SINK_CLASS_NAME)));
    }

    @ParallelTest
    void testBuildWithJarTgzAndZip() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // this test also testing push into Docker output
        final String imageName = getImageNameForTestCase();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());
        resourceManager.createResourceWithWait(KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1)
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
                    .addToLoggers("connect.root.logger.level", "INFO")
                .endInlineLogging()
            .endSpec()
            .build());

        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", testStorage.getTopicName());
        connectorConfig.put("level", "INFO");

        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName())
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .withClassName(TestConstants.ECHO_SINK_CLASS_NAME)
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        KafkaConnector kafkaConnector = KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();
        assertThat(kafkaConnector.getSpec().getClassName(), is(TestConstants.ECHO_SINK_CLASS_NAME));

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        resourceManager.createResourceWithWait(kafkaClients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        String connectPodName = kubeClient(testStorage.getNamespaceName()).listPodNamesInSpecificNamespace(testStorage.getNamespaceName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).stream()
                .filter(it -> it.contains(testStorage.getClusterName())).toList().get(0);
        PodUtils.waitUntilMessageIsInPodLogs(testStorage.getNamespaceName(), connectPodName, "Received message with key 'null' and value '\"Hello-world - 99\"'");
    }

    @OpenShiftOnly
    @ParallelTest
    void testPushIntoImageStream() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        String imageStreamName = "custom-image-stream";
        ImageStream imageStream = new ImageStreamBuilder()
            .editOrNewMetadata()
                .withName(imageStreamName)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build();

        kubeClient().getClient().adapt(OpenShiftClient.class).imageStreams().inNamespace(testStorage.getNamespaceName()).resource(imageStream).create();

        resourceManager.createResourceWithWait(KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1)
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

        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

        LOGGER.info("Checking, if KafkaConnect has all artifacts and if is successfully created");
        assertThat(kafkaConnect.getSpec().getBuild().getPlugins().get(0).getArtifacts().size(), is(2));
        assertThat(kafkaConnect.getSpec().getBuild().getOutput().getType(), is("imagestream"));
        assertThat(kafkaConnect.getSpec().getBuild().getOutput().getImage(), is(imageStreamName + ":latest"));
        assertThat(kafkaConnect.getStatus().getConditions().get(0).getType(), is(Ready.toString()));

        assertTrue(!kafkaConnect.getStatus().getConnectorPlugins().isEmpty());
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(TestConstants.ECHO_SINK_CLASS_NAME)));
    }

    @ParallelTest
    void testUpdateConnectWithAnotherPlugin() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

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

        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicName, testStorage.getNamespaceName()).build());

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1)
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
                .withNewInlineLogging()
                    .addToLoggers("connect.root.logger.level", "INFO")
                .endInlineLogging()
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(connect, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyResource.deployNetworkPolicyForResource(connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        Map<String, Object> echoSinkConfig = new HashMap<>();
        echoSinkConfig.put("topics", topicName);
        echoSinkConfig.put("level", "INFO");

        LOGGER.info("Creating EchoSink KafkaConnector");
        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(TestConstants.ECHO_SINK_CONNECTOR_NAME, testStorage.getClusterName())
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .withClassName(TestConstants.ECHO_SINK_CLASS_NAME)
                .withConfig(echoSinkConfig)
            .endSpec()
            .build());

        Map<String, String> connectSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());
        String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Checking that KafkaConnect API contains EchoSink KafkaConnector and not Camel-Telegram Connector class name");
        String plugins = cmdKubeClient(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl", "-X", "GET", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083/connector-plugins").out();

        assertFalse(plugins.contains(CAMEL_CONNECTOR_HTTP_SINK_CLASS_NAME));
        assertTrue(plugins.contains(TestConstants.ECHO_SINK_CLASS_NAME));

        LOGGER.info("Adding one more connector to the KafkaConnect");
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), kafkaConnect -> {
            kafkaConnect.getSpec().getBuild().getPlugins().add(secondPlugin);
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 1, connectSnapshot);

        Map<String, Object> camelHttpConfig = new HashMap<>();
        camelHttpConfig.put("camel.sink.path.httpUri", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName()) + ":8083");
        camelHttpConfig.put("topics", topicName);

        LOGGER.info("Creating Camel-HTTP-Sink KafkaConnector");
        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(camelConnector, testStorage.getClusterName())
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editOrNewSpec()
                .withClassName(CAMEL_CONNECTOR_HTTP_SINK_CLASS_NAME)
                .withConfig(camelHttpConfig)
            .endSpec()
            .build());

        KafkaConnectUtils.waitForConnectStatusContainsPlugins(testStorage.getNamespaceName(), testStorage.getClusterName());

        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName()).get();

        LOGGER.info("Checking if both Connectors were created and Connect contains both plugins");
        assertThat(kafkaConnect.getSpec().getBuild().getPlugins().size(), is(2));

        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(TestConstants.ECHO_SINK_CLASS_NAME)));
        assertTrue(kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(CAMEL_CONNECTOR_HTTP_SINK_CLASS_NAME)));
    }

    @ParallelTest
    void testBuildOtherPluginTypeWithAndWithoutFileName() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String imageName = getImageNameForTestCase();

        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topicName, testStorage.getNamespaceName()).build());

        resourceManager.createResourceWithWait(KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1)
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
        String connectPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();

        LOGGER.info("Checking that plugin has correct file name: {}", TestConstants.ECHO_SINK_FILE_NAME);
        assertEquals(getPluginFileNameFromConnectPod(testStorage.getNamespaceName(), connectPodName), TestConstants.ECHO_SINK_FILE_NAME);

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
        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(testStorage.getClusterName(), connect -> {
            connect.getSpec().getBuild().setPlugins(Collections.singletonList(pluginWithoutFileName));
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 1, connectSnapshot);

        LOGGER.info("Checking that plugin has different name than before");
        connectPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getClusterName(), Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        String fileName = getPluginFileNameFromConnectPod(testStorage.getNamespaceName(), connectPodName);
        assertNotEquals(fileName, TestConstants.ECHO_SINK_FILE_NAME);
        assertEquals(fileName, Util.hashStub(TestConstants.ECHO_SINK_JAR_URL));
    }

    @Tag(SANITY)
    @Tag(ACCEPTANCE)
    @KindNotSupported("using kind we encounter (error building image: deleting file system after stage 0: unlinkat //product_uuid: device or resource busy)")
    @ParallelTest
    void testBuildPluginUsingMavenCoordinatesArtifacts() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String imageName = getImageNameForTestCase();
        final String connectorName = testStorage.getClusterName() + "-camel-connector";

        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build(),
            KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), testStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1)
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

        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(connectorName, testStorage.getClusterName())
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editOrNewSpec()
                .withClassName(CAMEL_CONNECTOR_TIMER_CLASS_NAME)
                .withConfig(connectorConfig)
            .endSpec()
            .build());

        final KafkaClients kafkaClient = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()));
        resourceManager.createResourceWithWait(kafkaClient.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    private String getPluginFileNameFromConnectPod(final String namespaceName, final String connectPodName) {
        return cmdKubeClient(namespaceName).execInPod(connectPodName,
            "/bin/bash", "-c", "ls plugins/plugin-with-other-type/*").out().trim();
    }

    private String getImageNameForTestCase() {
        int randomNum = new Random().nextInt(Integer.MAX_VALUE);
        return Environment.getImageOutputRegistry(Environment.TEST_SUITE_NAMESPACE, TestConstants.ST_CONNECT_BUILD_IMAGE_NAME, String.valueOf(randomNum));

    }

    @BeforeAll
    void setup() {
        suiteTestStorage = new TestStorage(ResourceManager.getTestContext());

        clusterOperator = clusterOperator.defaultInstallation()
            .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(suiteTestStorage.getClusterName(), 3)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());
    }
}
