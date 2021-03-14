/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaConnectTlsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaConnectS2IStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectS2ITemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectS2IUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentConfigUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CONNECT_S2I;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.systemtest.Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource.kafkaConnectS2IClient;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@OpenShiftOnly
@Tag(REGRESSION)
@Tag(CONNECT_S2I)
@Tag(CONNECT_COMPONENTS)
class ConnectS2IST extends AbstractST {

    public static final String NAMESPACE = "connect-s2i-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(ConnectS2IST.class);
    private static final String CONNECT_S2I_TOPIC_NAME = "connect-s2i-topic-example";
    private String connectS2IClusterName;
    private String secondClusterName;
    private final String connectS2iClientsName = "connect-s2i-clients-name";
    private String kafkaClientsPodName;

    @ParallelTest
    void testDeployS2IWithCamelTimerPlugin(ExtensionContext extensionContext) throws InterruptedException, IOException {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";
        String timerTopicName = "timer-topic";
        // Calls to Connect API are executed from kafka-0 pod
        deployConnectS2IWithCamelTimer(extensionContext, clusterName, connectS2IClusterName, false);

        String timerConnectorConfig = "{" +
                "\"name\": \"" + connectS2IClusterName + "\"," +
                "\"config\": {" +
                "   \"connector.class\" : \"org.apache.camel.kafkaconnector.timer.CamelTimerSourceConnector\"," +
                "   \"tasks.max\" : \"1\"," +
                "   \"camel.source.path.timerName\" : \"timer\"," +
                "   \"topics\" : \"" + timerTopicName + "\"," +
                "   \"value.converter.schemas.enable\" : \"false\"," +
                "   \"transforms\" : \"HoistField,InsertField,ReplaceField\"," +
                "   \"transforms.HoistField.type\" : \"org.apache.kafka.connect.transforms.HoistField$Value\"," +
                "   \"transforms.HoistField.field\" : \"originalValue\"," +
                "   \"transforms.InsertField.type\" : \"org.apache.kafka.connect.transforms.InsertField$Value\"," +
                "   \"transforms.InsertField.timestamp.field\" : \"timestamp\"," +
                "   \"transforms.InsertField.static.field\" : \"message\"," +
                "   \"transforms.InsertField.static.value\" : \"'Hello World'\"," +
                "   \"transforms.ReplaceField.type\" : \"org.apache.kafka.connect.transforms.ReplaceField$Value\"," +
                "   \"transforms.ReplaceField.blacklist\" : \"originalValue\"}" +
                "}";

        KafkaConnectS2IUtils.waitForConnectS2IReady(connectS2IClusterName);

        TestUtils.waitFor("ConnectS2I will be ready and POST will be executed", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            String createConnectorOutput = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "POST", "-H", "Accept:application/json", "-H", "Content-Type:application/json",
                    "http://" + KafkaConnectS2IResources.serviceName(connectS2IClusterName) + ":8083/connectors/", "-d", timerConnectorConfig).out();
            LOGGER.info("Create Connector result: {}", createConnectorOutput);
            return !createConnectorOutput.contains("error_code");
        });

        // Make sure that connector is really created
        Thread.sleep(10_000);

        String connectorStatus = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", "http://" + KafkaConnectS2IResources.serviceName(connectS2IClusterName) + ":8083/connectors/" + connectS2IClusterName + "/status").out();

        assertThat(connectorStatus, containsString("RUNNING"));

        consumerTimerMessages(extensionContext, clusterName, timerTopicName);
    }

    @ParallelTest
    @Tag(CONNECTOR_OPERATOR)
    void testDeployS2IAndKafkaConnectorWithCamelTimerPlugin(ExtensionContext extensionContext) throws IOException {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";

        String timerTopicName1 = "timer-topic";
        String timerTopicName2 = "timer-topic-2";
        String consumerName = "timer-consumer";
        // Calls to Connect API are executed from kafka-0 pod
        deployConnectS2IWithCamelTimer(extensionContext, clusterName, connectS2IClusterName, true);

        // Make sure that Connect API is ready
        KafkaConnectS2IUtils.waitForConnectS2IReady(connectS2IClusterName);

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(connectS2IClusterName)
            .withNewSpec()
                .withClassName("org.apache.camel.kafkaconnector.timer.CamelTimerSourceConnector")
                .withTasksMax(2)
                .addToConfig("camel.source.path.timerName", "timer")
                .addToConfig("topics", timerTopicName1)
                .addToConfig("value.converter.schemas.enable", "false")
                .addToConfig("transforms", "HoistField,InsertField,ReplaceField")
                .addToConfig("transforms.HoistField.type", "org.apache.kafka.connect.transforms.HoistField$Value")
                .addToConfig("transforms.HoistField.field", "originalValue")
                .addToConfig("transforms.InsertField.type", "org.apache.kafka.connect.transforms.InsertField$Value")
                .addToConfig("transforms.InsertField.timestamp.field", "timestamp")
                .addToConfig("transforms.InsertField.static.field", "message")
                .addToConfig("transforms.InsertField.static.value", "'Hello World'")
                .addToConfig("transforms.ReplaceField.type", "org.apache.kafka.connect.transforms.ReplaceField$Value")
                .addToConfig("transforms.ReplaceField.blacklist", "originalValue")
            .endSpec()
            .build());

        checkConnectorInStatus(NAMESPACE, connectS2IClusterName);
        String apiUrl = KafkaConnectS2IResources.serviceName(connectS2IClusterName);

        String connectorStatus = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", "http://" + apiUrl + ":8083/connectors/" + connectS2IClusterName + "/status").out();
        assertThat(connectorStatus, containsString("RUNNING"));

        consumerTimerMessages(extensionContext, clusterName, timerTopicName1);

        String connectorConfig = KafkaConnectorUtils.getConnectorConfig(kafkaClientsPodName, connectS2IClusterName, apiUrl);
        KafkaConnectorResource.replaceKafkaConnectorResource(connectS2IClusterName, kC -> {
            Map<String, Object> config = kC.getSpec().getConfig();
            config.put("topics", timerTopicName2);
            kC.getSpec().setConfig(config);
            kC.getSpec().setTasksMax(8);
        });

        connectorConfig = KafkaConnectorUtils.waitForConnectorConfigUpdate(kafkaClientsPodName, connectS2IClusterName, connectorConfig, apiUrl);
        assertThat(connectorConfig.contains("tasks.max\":\"8"), is(true));
        assertThat(connectorConfig.contains("topics\":\"timer-topic-2"), is(true));

        consumerTimerMessages(extensionContext, clusterName, timerTopicName2);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectS2IWithTlsAndScramShaAuthentication(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaUser user  = KafkaUserTemplates.scramShaUser(clusterName, userName).build();
        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-tls-" + Constants.KAFKA_CLIENTS, user).build());
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, clusterName, 1)
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(clusterName))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(userName)
                        .withNewPasswordSecret()
                            .withSecretName(userName)
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .build());

        final String tlsKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-tls-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(tlsKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        String kafkaConnectS2IPodName = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName).get(0).getMetadata().getName();
        String kafkaConnectS2ILogs = kubeClient().logs(kafkaConnectS2IPodName);

        LOGGER.info("Verifying that in KafkaConnect logs not contain ERRORs");
        assertThat(kafkaConnectS2ILogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", tlsKafkaClientsPodName, topicName);
        KafkaConnectorUtils.createFileSinkConnector(tlsKafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(connectS2IClusterName, NAMESPACE, 8083));

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectS2IPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        assertThat(cmdKubeClient().execInPod(kafkaConnectS2IPodName, "/bin/bash", "-c", "cat " + Constants.DEFAULT_SINK_FILE_PATH).out(),
                containsString("99"));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCustomAndUpdatedValues(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1).build());

        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");

        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, clusterName, 1)
            .editSpec()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .build());

        String depConfName = KafkaConnectS2IResources.deploymentName(connectS2IClusterName);

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        Map<String, String> connectSnapshot = DeploymentConfigUtils.depConfigSnapshot(KafkaConnectS2IResources.deploymentName(connectS2IClusterName));

        LOGGER.info("Verify values before update");

        LabelSelector deploymentConfigSelector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(KafkaConnectS2IResources.deploymentName(connectS2IClusterName))).build();
        String connectPodName = kubeClient().listPods(deploymentConfigSelector).get(0).getMetadata().getName();

        checkSpecificVariablesInContainer(connectPodName, KafkaConnectS2IResources.deploymentName(connectS2IClusterName), envVarGeneral);

        LOGGER.info("Updating values in ConnectS2I container");
        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, kc -> {
            kc.getSpec().getTemplate().getConnectContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
        });

        DeploymentConfigUtils.waitTillDepConfigHasRolled(depConfName, connectSnapshot);

        deploymentConfigSelector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(KafkaConnectS2IResources.deploymentName(connectS2IClusterName))).build();
        connectPodName = kubeClient().listPods(deploymentConfigSelector).get(0).getMetadata().getName();

        LOGGER.info("Verify values after update");
        checkSpecificVariablesInContainer(connectPodName, depConfName, envVarUpdated);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testJvmAndResources(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resourceManager.createResource(extensionContext, false, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, clusterName, 1)
            .editSpec()
                .withResources(
                    new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("400M"))
                        .addToLimits("cpu", new Quantity("2"))
                        .addToRequests("memory", new Quantity("300M"))
                        .addToRequests("cpu", new Quantity("1"))
                        .build())
                .withBuildResources(
                    new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("1000M"))
                        .addToLimits("cpu", new Quantity("1000"))
                        .addToRequests("memory", new Quantity("400M"))
                        .addToRequests("cpu", new Quantity("1000"))
                        .build())
                .withNewJvmOptions()
                    .withXmx("200m")
                    .withXms("200m")
                    .withXx(jvmOptionsXX)
                .endJvmOptions()
            .endSpec()
            .build());

        KafkaConnectS2IUtils.waitForConnectS2INotReady(connectS2IClusterName);

        TestUtils.waitFor("build status: Pending", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_AVAILABILITY_TEST,
            () -> kubeClient().getClient().adapt(OpenShiftClient.class).builds().inNamespace(NAMESPACE).withName(connectS2IClusterName + "-connect-1").get().getStatus().getPhase().equals("Pending"));

        kubeClient().getClient().adapt(OpenShiftClient.class).builds().inNamespace(NAMESPACE).withName(connectS2IClusterName + "-connect-1").withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, kc -> {
            kc.getSpec().setBuildResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity("1000M"))
                    .addToLimits("cpu", new Quantity("1"))
                    .addToRequests("memory", new Quantity("400M"))
                    .addToRequests("cpu", new Quantity("1"))
                    .build());
        });

        TestUtils.waitFor("KafkaConnect change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> kubeClient().getClient().adapt(OpenShiftClient.class).buildConfigs().inNamespace(NAMESPACE).withName(connectS2IClusterName + "-connect").get().getSpec().getResources().getRequests().get("cpu").equals(new Quantity("1")));

        cmdKubeClient().exec("start-build", KafkaConnectS2IResources.deploymentName(connectS2IClusterName), "-n", NAMESPACE);

        KafkaConnectS2IUtils.waitForConnectS2IReady(connectS2IClusterName);

        String podName = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName).get(0).getMetadata().getName();

        assertResources(NAMESPACE, podName, connectS2IClusterName + "-connect",
            "400M", "2", "300M", "1");
        assertExpectedJavaOpts(podName, connectS2IClusterName + "-connect",
            "-Xmx200m", "-Xms200m", "-XX:+UseG1GC");

        kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(connectS2IClusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        DeploymentConfigUtils.waitForDeploymentConfigDeletion(KafkaConnectS2IResources.deploymentName(connectS2IClusterName));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(CONNECTOR_OPERATOR)
    @Tag(ACCEPTANCE)
    void testKafkaConnectorWithConnectS2IAndConnectWithSameName(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String connectClusterName = clusterName + "-connect";
        String connectS2IClusterName = clusterName + "-connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        // Create different connect cluster via S2I resources
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, clusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
                .editSpec()
                .addToConfig("group.id", connectS2IClusterName)
                .addToConfig("offset.storage.topic", connectS2IClusterName + "-offsets")
                .addToConfig("config.storage.topic", connectS2IClusterName + "-config")
                .addToConfig("status.storage.topic", connectS2IClusterName + "-status")
            .endSpec()
            .build());

        // Create connect cluster with default connect image
        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(extensionContext, connectClusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("group.id", connectClusterName)
                .addToConfig("offset.storage.topic", connectClusterName + "-offsets")
                .addToConfig("config.storage.topic", connectClusterName + "-config")
                .addToConfig("status.storage.topic", connectClusterName + "-status")
            .endSpec()
            .build());

        KafkaConnectUtils.waitForConnectNotReady(connectClusterName);

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(connectS2IClusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", "/tmp/test-file-sink.txt")
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        // Check that KafkaConnectS2I contains created connector
        String connectS2IPodName = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName).get(0).getMetadata().getName();
        KafkaConnectorUtils.waitForConnectorCreation(connectS2IPodName, clusterName);

        KafkaConnectUtils.waitForConnectNotReady(connectClusterName);

        String connectorConfig = KafkaConnectorUtils.getConnectorConfig(connectS2IPodName, clusterName, "localhost");

        String newTopic = "new-topic";
        KafkaConnectorResource.replaceKafkaConnectorResource(connectS2IClusterName, kc -> {
            kc.getSpec().getConfig().put("topics", newTopic);
            kc.getSpec().setTasksMax(8);
        });

        connectorConfig = KafkaConnectorUtils.waitForConnectorConfigUpdate(connectS2IPodName, connectS2IClusterName, connectorConfig, "localhost");
        assertThat(connectorConfig.contains("tasks.max\":\"8"), is(true));
        assertThat(connectorConfig.contains("topics\":\"" + newTopic), is(true));

        // Now delete KafkaConnector resource and create connector manually
        KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(clusterName).delete();

        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, kc -> {
            kc.getMetadata().getAnnotations().remove(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES);
        });

        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectS2IResources.url(connectS2IClusterName, NAMESPACE, 8083));
        final String connectorName = "sink-test";
        KafkaConnectorUtils.waitForConnectorCreation(connectS2IPodName, connectorName);
        KafkaConnectorUtils.waitForConnectorStability(connectorName, connectS2IPodName);
        KafkaConnectUtils.waitForConnectNotReady(connectClusterName);

        KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(connectClusterName).delete();
        DeploymentUtils.waitForDeploymentDeletion(KafkaConnectResources.deploymentName(connectClusterName));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    void testMultiNodeKafkaConnectS2IWithConnectorCreation(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        // Crate connect cluster with default connect image
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, clusterName, 3)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editSpec()
                    .addToConfig("group.id", connectS2IClusterName)
                    .addToConfig("offset.storage.topic", connectS2IClusterName + "-offsets")
                    .addToConfig("config.storage.topic", connectS2IClusterName + "-config")
                    .addToConfig("status.storage.topic", connectS2IClusterName + "-status")
                .endSpec()
                .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(connectS2IClusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        String execConnectPod =  kubeClient().listKafkaConnectS2IPods(connectS2IClusterName).get(0).getMetadata().getName();

        JsonObject connectStatus = new JsonObject(cmdKubeClient().execInPod(
                execConnectPod,
                "curl", "-X", "GET", "http://localhost:8083/connectors/" + connectS2IClusterName + "/status").out()
        );

        String podIP = connectStatus.getJsonObject("connector").getString("worker_id").split(":")[0];

        String connectorPodName = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName).stream().filter(pod ->
                pod.getStatus().getPodIP().equals(podIP)).findFirst().get().getMetadata().getName();

        internalKafkaClient.assertSentAndReceivedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(connectorPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testChangeConnectS2IConfig(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String secondClusterName = "second-" + clusterName;
        String connectS2IClusterName = clusterName + "-connect-s2i";
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(secondClusterName, 3, 1).build());

        String bootstrapAddress = KafkaResources.tlsBootstrapAddress(secondClusterName);

        resourceManager.createResource(extensionContext,
                KafkaClientsTemplates.kafkaClients(true, kafkaClientsName).build());

        resourceManager.createResource(extensionContext,
            KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, clusterName, 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editSpec()
                    .withVersion(TestKafkaVersion.getKafkaVersions().get(0).version())
                .endSpec()
                .build());

        String deploymentConfigName = KafkaConnectS2IResources.deploymentName(connectS2IClusterName);

        List<Pod> connectPods = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName);

        LOGGER.info("===== SCALING UP AND DOWN =====");

        final int initialReplicas = connectPods.size();
        assertThat(initialReplicas, is(1));

        final int scaleReplicasTo = initialReplicas + 3;

        //scale up
        LOGGER.info("Scaling up to {}", scaleReplicasTo);
        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, cs2i ->
                cs2i.getSpec().setReplicas(scaleReplicasTo));
        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(deploymentConfigName, scaleReplicasTo);

        connectPods = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName);
        assertThat(connectPods.size(), Matchers.is(scaleReplicasTo));
        LOGGER.info("Scaling to {} finished", scaleReplicasTo);

        //scale down
        LOGGER.info("Scaling down to {}", initialReplicas);
        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, cs2i ->
                cs2i.getSpec().setReplicas(initialReplicas));
        Map<String, String> depConfSnapshot = DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(deploymentConfigName, initialReplicas);

        connectPods = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName);
        assertThat(connectPods.size(), is(initialReplicas));

        LOGGER.info("Scaling to {} finished", initialReplicas);

        LOGGER.info("===== UPDATE BOOTSTRAP SERVER ADDRESS =====");

        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, kafkaConnectS2I -> {
            kafkaConnectS2I.getSpec().setBootstrapServers(bootstrapAddress);
            kafkaConnectS2I.getSpec().setTls(new KafkaConnectTlsBuilder()
                    .addNewTrustedCertificate()
                        .withNewSecretName(secondClusterName + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .withNewSecretName(secondClusterName + "-cluster-ca-cert")
                    .withCertificate("ca.crt")
                    .endTrustedCertificate()
                    .build());
        });

        depConfSnapshot = DeploymentConfigUtils.waitTillDepConfigHasRolled(deploymentConfigName, depConfSnapshot);
        assertThat(kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(connectS2IClusterName).get().getSpec().getBootstrapServers(), is(bootstrapAddress));

        LOGGER.info("===== CONNECTS2I VERSION CHANGE =====");

        LOGGER.info("Setting version from {} to {}", TestKafkaVersion.getKafkaVersions().get(0).version(), TestKafkaVersion.getKafkaVersions().get(1).version());
        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName,
            kafkaConnectS2I -> kafkaConnectS2I.getSpec().setVersion(TestKafkaVersion.getKafkaVersions().get(1).version()));

        depConfSnapshot = DeploymentConfigUtils.waitTillDepConfigHasRolled(deploymentConfigName, depConfSnapshot);

        String versionCommand = "ls libs | grep -Po 'connect-api-\\K(\\d+.\\d+.\\d+)(?=.*jar)' | head -1";

        String actualVersion = cmdKubeClient().execInPodContainer(kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0),
                "", "/bin/bash", "-c", versionCommand).out().trim();

        assertThat(actualVersion, is(TestKafkaVersion.getKafkaVersions().get(1).version()));

        LOGGER.info("===== CONNECTS2I CERT CHANGE =====");
        InputStream secretInputStream = getClass().getClassLoader().getResourceAsStream("security-st-certs/expired-cluster-ca.crt");
        String clusterCaCert = TestUtils.readResource(secretInputStream);
        SecretUtils.createSecret("my-secret", "ca.crt", new String(Base64.getEncoder().encode(clusterCaCert.getBytes()), StandardCharsets.US_ASCII));

        CertSecretSource certSecretSource = new CertSecretSourceBuilder()
            .withSecretName("my-secret")
            .withCertificate("ca.crt")
            .build();

        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, kafkaConnectS2I -> {
            kafkaConnectS2I.getSpec().getTls().getTrustedCertificates().add(certSecretSource);
        });

        DeploymentConfigUtils.waitTillDepConfigHasRolled(deploymentConfigName, depConfSnapshot);

        List<CertSecretSource> trustedCertificates = kafkaConnectS2IClient().inNamespace(NAMESPACE)
            .withName(connectS2IClusterName).get().getSpec().getTls().getTrustedCertificates();

        assertThat(trustedCertificates.stream().anyMatch(cert -> cert.getSecretName().equals("my-secret")), is(true));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SCALABILITY)
    void testScaleConnectS2IWithoutConnectorToZero(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, clusterName, 2).build());

        String deploymentConfigName = KafkaConnectS2IResources.deploymentName(connectS2IClusterName);
        List<Pod> connectS2IPods = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName);

        assertThat(connectS2IPods.size(), is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, kafkaConnectS2I -> kafkaConnectS2I.getSpec().setReplicas(0));

        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(deploymentConfigName, 0);

        connectS2IPods = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName);
        KafkaConnectS2IStatus connectS2IStatus = kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(connectS2IClusterName).get().getStatus();

        assertThat(connectS2IPods.size(), Matchers.is(0));
        assertThat(connectS2IStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(CONNECTOR_OPERATOR)
    @Tag(SCALABILITY)
    void testScaleConnectS2IWithConnectorToZero(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, clusterName, 2)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(connectS2IClusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", TOPIC_NAME)
            .endSpec()
            .build());

        String deploymentConfigName = KafkaConnectS2IResources.deploymentName(connectS2IClusterName);
        List<Pod> connectS2IPods = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName);

        assertThat(connectS2IPods.size(), Matchers.is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectS2IResource.replaceConnectS2IResource(connectS2IClusterName, kafkaConnectS2I -> kafkaConnectS2I.getSpec().setReplicas(0));

        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(deploymentConfigName, 0);

        connectS2IPods = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName);
        KafkaConnectS2IStatus connectS2IStatus = kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(connectS2IClusterName).get().getStatus();
        KafkaConnectorStatus connectorStatus = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(connectS2IClusterName).get().getStatus();

        assertThat(connectS2IPods.size(), Matchers.is(0));
        assertThat(connectS2IStatus.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getType().equals(NotReady.toString())), is(true));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getMessage().contains("has 0 replicas")), is(true));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SCALABILITY)
    void testScaleConnectS2ISubresource(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "-connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, connectS2IClusterName, clusterName, 1).build());

        int scaleTo = 4;
        long connectS2IObsGen = kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(connectS2IClusterName).get().getStatus().getObservedGeneration();
        String connectS2IGenName = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaConnectS2I subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient().scaleByName(KafkaConnectS2I.RESOURCE_KIND, connectS2IClusterName, scaleTo);
        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(KafkaConnectS2IResources.deploymentName(connectS2IClusterName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<Pod> connectS2IPods = kubeClient().listKafkaConnectS2IPods(connectS2IClusterName);
        assertThat(connectS2IPods.size(), is(4));
        assertThat(kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(connectS2IClusterName).get().getSpec().getReplicas(), is(4));
        assertThat(kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(connectS2IClusterName).get().getStatus().getReplicas(), is(4));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(connectS2IObsGen < kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(connectS2IClusterName).get().getStatus().getObservedGeneration(), is(true));
        for (Pod pod : connectS2IPods) {
            assertThat(pod.getMetadata().getName().contains(connectS2IGenName), is(true));
        }
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testHostAliases(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        HostAlias hostAlias = new HostAliasBuilder()
            .withIp(aliasIp)
            .withHostnames(aliasHostname)
            .build();

        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, clusterName, clusterName, 1)
            .editSpec()
                .withNewTemplate()
                    .withNewPod()
                        .withHostAliases(hostAlias)
                    .endPod()
                .endTemplate()
            .endSpec()
            .build());

        String connectS2IPodName = kubeClient().listKafkaConnectS2IPods(clusterName).get(0).getMetadata().getName();

        LOGGER.info("Checking the /etc/hosts file");
        String output = cmdKubeClient().execInPod(connectS2IPodName, "cat", "/etc/hosts").out();
        assertThat(output, containsString(etcHostsData));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMountingSecretAndConfigMapAsVolumesAndEnvVars(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "connect-s2i";

        String secretPassword = "password";
        String encodedPassword = Base64.getEncoder().encodeToString(secretPassword.getBytes());

        String secretEnv = "MY_CONNECTS2I_SECRET";
        String configMapEnv = "MY_CONNECTS2I_CONFIG_MAP";

        String dotedSecretEnv = "MY_DOTED_CONNECTS2I_SECRET";
        String dotedConfigMapEnv = "MY_DOTED_CONNECTS2I_CONFIG_MAP";

        String configMapName = "connect-config-map";
        String secretName = "connect-secret";

        String dotedConfigMapName = "connect.config.map";
        String dotedSecretName = "connect.secret";

        String configMapKey = "my-key";
        String secretKey = "my-secret-key";

        String configMapValue = "my-value";

        Secret connectSecret = new SecretBuilder()
            .withNewMetadata()
                .withName(secretName)
            .endMetadata()
            .withType("Opaque")
            .addToData(secretKey, encodedPassword)
            .build();

        ConfigMap configMap = new ConfigMapBuilder()
            .editOrNewMetadata()
                .withName(configMapName)
            .endMetadata()
            .addToData(configMapKey, configMapValue)
            .build();

        Secret dotedConnectSecret = new SecretBuilder()
            .withNewMetadata()
                .withName(dotedSecretName)
            .endMetadata()
            .withType("Opaque")
            .addToData(secretKey, encodedPassword)
            .build();

        ConfigMap dotedConfigMap = new ConfigMapBuilder()
            .editOrNewMetadata()
                .withName(dotedConfigMapName)
            .endMetadata()
            .addToData(configMapKey, configMapValue)
            .build();

        kubeClient().createSecret(connectSecret);
        kubeClient().createSecret(dotedConnectSecret);
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMap);
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(dotedConfigMap);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, clusterName, clusterName, 1)
            .editSpec()
                .withNewExternalConfiguration()
                    .addNewVolume()
                        .withNewName(secretName)
                        .withSecret(new SecretVolumeSourceBuilder().withSecretName(secretName).build())
                    .endVolume()
                    .addNewVolume()
                        .withNewName(configMapName)
                        .withConfigMap(new ConfigMapVolumeSourceBuilder().withName(configMapName).build())
                    .endVolume()
                    .addNewVolume()
                        .withNewName(dotedSecretName)
                        .withSecret(new SecretVolumeSourceBuilder().withSecretName(dotedSecretName).build())
                    .endVolume()
                    .addNewVolume()
                        .withNewName(dotedConfigMapName)
                        .withConfigMap(new ConfigMapVolumeSourceBuilder().withName(dotedConfigMapName).build())
                    .endVolume()
                    .addNewEnv()
                        .withNewName(secretEnv)
                        .withNewValueFrom()
                            .withSecretKeyRef(
                                new SecretKeySelectorBuilder()
                                    .withKey(secretKey)
                                    .withName(connectSecret.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                    .addNewEnv()
                        .withNewName(configMapEnv)
                        .withNewValueFrom()
                            .withConfigMapKeyRef(
                                new ConfigMapKeySelectorBuilder()
                                    .withKey(configMapKey)
                                    .withName(configMap.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                    .addNewEnv()
                        .withNewName(dotedSecretEnv)
                        .withNewValueFrom()
                            .withSecretKeyRef(
                                new SecretKeySelectorBuilder()
                                    .withKey(secretKey)
                                    .withName(dotedConnectSecret.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                    .addNewEnv()
                        .withNewName(dotedConfigMapEnv)
                        .withNewValueFrom()
                            .withConfigMapKeyRef(
                                new ConfigMapKeySelectorBuilder()
                                    .withKey(configMapKey)
                                    .withName(dotedConfigMap.getMetadata().getName())
                                    .withOptional(false)
                                    .build())
                        .endValueFrom()
                    .endEnv()
                .endExternalConfiguration()
            .endSpec()
            .build());

        String connectS2IPodName = kubeClient().listKafkaConnectS2IPods(clusterName).get(0).getMetadata().getName();

        LOGGER.info("Check if the ENVs contains desired values");
        assertThat(cmdKubeClient().execInPod(connectS2IPodName, "/bin/bash", "-c", "printenv " + secretEnv).out().trim(), equalTo(secretPassword));
        assertThat(cmdKubeClient().execInPod(connectS2IPodName, "/bin/bash", "-c", "printenv " + configMapEnv).out().trim(), equalTo(configMapValue));
        assertThat(cmdKubeClient().execInPod(connectS2IPodName, "/bin/bash", "-c", "printenv " + dotedSecretEnv).out().trim(), equalTo(secretPassword));
        assertThat(cmdKubeClient().execInPod(connectS2IPodName, "/bin/bash", "-c", "printenv " + dotedConfigMapEnv).out().trim(), equalTo(configMapValue));

        LOGGER.info("Check if volumes contains desired values");
        assertThat(
            cmdKubeClient().execInPod(connectS2IPodName, "/bin/bash", "-c", "cat external-configuration/" + configMapName + "/" + configMapKey).out().trim(),
            equalTo(configMapValue)
        );
        assertThat(
            cmdKubeClient().execInPod(connectS2IPodName, "/bin/bash", "-c", "cat external-configuration/" + secretName + "/" + secretKey).out().trim(),
            equalTo(secretPassword)
        );
        assertThat(
            cmdKubeClient().execInPod(connectS2IPodName, "/bin/bash", "-c", "cat external-configuration/" + dotedConfigMapName + "/" + configMapKey).out().trim(),
            equalTo(configMapValue)
        );
        assertThat(
            cmdKubeClient().execInPod(connectS2IPodName, "/bin/bash", "-c", "cat external-configuration/" + dotedSecretName + "/" + secretKey).out().trim(),
            equalTo(secretPassword)
        );
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testConfigureDeploymentStrategy(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String connectS2IClusterName = clusterName + "connect-s2i";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, clusterName, clusterName, 1)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String connectS2IDepName = KafkaConnectS2IResources.deploymentName(clusterName);

        LOGGER.info("Adding label to ConnectS2I resource, the CR should be recreated");
        KafkaConnectS2IResource.replaceConnectS2IResource(clusterName,
            kcs2i -> kcs2i.getMetadata().setLabels(Collections.singletonMap("some", "label")));
        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(connectS2IDepName, 1);

        KafkaConnectS2I kafkaConnectS2I = kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(clusterName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kafkaConnectS2I.getStatus().getObservedGeneration(), is(1L));
        assertThat(kafkaConnectS2I.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kafkaConnectS2I.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaConnectS2IResource.replaceConnectS2IResource(clusterName,
            kcs2i -> kcs2i.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE));
        KafkaConnectS2IUtils.waitForConnectS2IReady(clusterName);

        LOGGER.info("Adding another label to ConnectS2I resource, pods should be rolled");
        KafkaConnectS2IResource.replaceConnectS2IResource(clusterName, kcs2i -> kcs2i.getMetadata().getLabels().put("another", "label"));
        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(connectS2IDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        kafkaConnectS2I = kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(clusterName).get();
        assertThat(kafkaConnectS2I.getStatus().getObservedGeneration(), is(2L));
        assertThat(kafkaConnectS2I.getMetadata().getLabels().toString(), containsString("another=label"));
        assertThat(kafkaConnectS2I.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.ROLLING_UPDATE));
    }

    private void deployConnectS2IWithCamelTimer(ExtensionContext extensionContext, String clusterName, String kafkaConnectS2IName, boolean useConnectorOperator) throws IOException {
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, kafkaConnectS2IName, clusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, Boolean.toString(useConnectorOperator))
            .endMetadata()
            .build());

        String depConfName = KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName);
        Map<String, String> connectSnapshot = DeploymentConfigUtils.depConfigSnapshot(depConfName);

        File dir = FileUtils.downloadAndUnzip("https://repo1.maven.org/maven2/org/apache/camel/kafkaconnector/camel-timer-kafka-connector/0.7.0/camel-timer-kafka-connector-0.7.0-package.zip");

        // Start a new image build using the plugins directory
        cmdKubeClient().execInCurrentNamespace("start-build", depConfName, "--from-dir", dir.getAbsolutePath());
        // Wait for rolling update connect pods
        DeploymentConfigUtils.waitTillDepConfigHasRolled(depConfName, connectSnapshot);

        LOGGER.info("Collect plugins information from connect s2i pod");
        String plugins = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", "http://" + KafkaConnectS2IResources.serviceName(kafkaConnectS2IName) + ":8083/connector-plugins").out();

        assertThat(plugins, containsString("org.apache.camel.kafkaconnector.timer.CamelTimerSourceConnector"));
    }

    private void checkConnectorInStatus(String namespace, String kafkaConnectS2IName) {
        KafkaConnectS2IStatus kafkaConnectS2IStatus = kafkaConnectS2IClient().inNamespace(namespace).withName(kafkaConnectS2IName).get().getStatus();
        List<ConnectorPlugin> pluginsList = kafkaConnectS2IStatus.getConnectorPlugins();
        assertThat(pluginsList, notNullValue());
        List<String> pluginsClasses = pluginsList.stream().map(p -> p.getConnectorClass()).collect(Collectors.toList());
        assertThat(pluginsClasses, hasItems("org.apache.kafka.connect.file.FileStreamSinkConnector",
                "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
                "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
                "org.apache.kafka.connect.mirror.MirrorSourceConnector",
                "org.apache.camel.kafkaconnector.timer.CamelTimerSourceConnector"));
    }

    private void consumerTimerMessages(ExtensionContext extensionContext, String clusterName, String topicName) {
        String consumerName = "timer-consumer-" + rng.nextInt(Integer.MAX_VALUE);
        KafkaBasicExampleClients kafkaBasicClientResource = new KafkaBasicExampleClients.Builder()
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withTopicName(topicName)
                .withMessageCount(10)
                .withDelayMs(0)
                .build();

        resourceManager.createResource(extensionContext, kafkaBasicClientResource.consumerStrimzi().build());
        ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, 10);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT, Constants.RECONCILIATION_INTERVAL);

        connectS2IClusterName = NAMESPACE + "-s2i";
        secondClusterName = "second-" + NAMESPACE;

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, connectS2iClientsName).build());
        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(connectS2iClientsName).get(0).getMetadata().getName();

        if (SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {

            LOGGER.info("Checking if secret {} is in the default namespace", SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET);

            if (kubeClient("default").getSecret(SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET) == null) {
                throw new RuntimeException(SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET + " is not in the default namespace!");
            }

            Secret pullSecret = kubeClient("default").getSecret(SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET);

            kubeClient(NAMESPACE).createSecret(new SecretBuilder()
                .withNewApiVersion("v1")
                .withNewKind("Secret")
                .withNewMetadata()
                .withName(SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET)
                .endMetadata()
                .withNewType("kubernetes.io/dockerconfigjson")
                .withData(Collections.singletonMap(".dockerconfigjson", pullSecret.getData().get(".dockerconfigjson")))
                .build());

            LOGGER.info("Link existing pull-secret {} with associate builder service account", SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET);

            ResourceManager.cmdKubeClient().exec("secrets", "link", "builder", SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET);
        }
    }
}
