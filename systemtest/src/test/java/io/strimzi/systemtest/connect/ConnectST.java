/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.connect;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSourceBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectS2ITemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectS2IUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentConfigUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.systemtest.Constants.SMOKE;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(CONNECT_COMPONENTS)
class ConnectST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConnectST.class);
    public static final String NAMESPACE = "connect-cluster-test";

    private static final String CONNECT_TOPIC_NAME = "connect-topic-example";
    private static final String KAFKA_CLIENTS_NAME = "shared-" +  Constants.KAFKA_CLIENTS;

    private String kafkaClientsPodName;

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testDeployUndeploy(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        Map<String, Object> exceptedConfig = StUtils.loadProperties("group.id=" + KafkaConnectResources.deploymentName(clusterName) + "\n" +
                "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "config.storage.topic=" + KafkaConnectResources.metricsAndLogConfigMapName(clusterName) + "\n" +
                "status.storage.topic=" + KafkaConnectResources.configStorageTopicStatus(clusterName) + "\n" +
                "offset.storage.topic=" + KafkaConnectResources.configStorageTopicOffsets(clusterName) + "\n");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1, true).build());

        LOGGER.info("Looks like the connect cluster my-cluster deployed OK");

        String podName = PodUtils.getPodNameByPrefix(KafkaConnectResources.deploymentName(clusterName));
        String kafkaPodJson = TestUtils.toJsonString(kubeClient().getPod(podName));

        assertThat(kafkaPodJson, hasJsonPath(StUtils.globalVariableJsonPathBuilder(0, "KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.tlsBootstrapAddress(clusterName))));
        assertThat(StUtils.getPropertiesFromJson(0, kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(exceptedConfig));
        testDockerImagesForKafkaConnect(clusterName);

        verifyLabelsOnPods(clusterName, "connect", null, "KafkaConnect");
        verifyLabelsForService(clusterName, "connect-api", "KafkaConnect");
        verifyLabelsForConfigMaps(clusterName, null, "");
        verifyLabelsForServiceAccounts(clusterName, null);
    }

    private void testDockerImagesForKafkaConnect(String clusterName) {
        LOGGER.info("Verifying docker image names");
        Map<String, String> imgFromDeplConf = getImagesFromConfig();
        //Verifying docker image for kafka connect
        String connectImageName = PodUtils.getFirstContainerImageNameFromPod(kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).
                get(0).getMetadata().getName());

        String connectVersion = Crds.kafkaConnectOperation(kubeClient().getClient()).inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getVersion();
        if (connectVersion == null) {
            connectVersion = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_CONNECT_IMAGE_MAP)).get(connectVersion), is(connectImageName));
        LOGGER.info("Docker images verified");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SMOKE)
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectWithFileSinkPlugin(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        String kafkaConnectPodName = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(clusterName, NAMESPACE, 8083));

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectWithPlainAndScramShaAuthentication(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        // Use a Kafka with plain listener disabled

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaUser kafkaUser =  KafkaUserTemplates.scramShaUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(clusterName, userName).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .withNewSpec()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(clusterName))
                .withNewKafkaClientAuthenticationScramSha512()
                    .withNewUsername(userName)
                    .withPasswordSecret(new PasswordSecretSourceBuilder()
                        .withSecretName(userName)
                        .withPassword("password")
                        .build())
                .endKafkaClientAuthenticationScramSha512()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withReplicas(1)
            .endSpec()
            .build());

        String kafkaConnectPodName = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        String kafkaConnectLogs = kubeClient().logs(kafkaConnectPodName);

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", kafkaClientsPodName, topicName);
        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(clusterName, NAMESPACE, 8083));

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName + "-second", kafkaUser).build());

        final String kafkaClientsSecondPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName + "-second").get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsSecondPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectAndConnectorFileSinkPlugin(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                .endSpec()
            .build());

        String connectorName = "license-source";

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(connectorName, clusterName, 2)
            .editSpec()
                .addToConfig("topic", topicName)
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

        int received = internalKafkaClient.receiveMessagesPlain();
        assertThat(received, greaterThanOrEqualTo(MESSAGE_COUNT));

        String service = KafkaConnectResources.url(clusterName, NAMESPACE, 8083);
        String output = cmdKubeClient().execInPod(kafkaClientsPodName, "/bin/bash", "-c", "curl " + service + "/connectors/" + connectorName).out();
        assertThat(output, containsString("\"name\":\"license-source\""));
        assertThat(output, containsString("\"connector.class\":\"org.apache.kafka.connect.file.FileStreamSourceConnector\""));
        assertThat(output, containsString("\"tasks.max\":\"2\""));
        assertThat(output, containsString("\"topic\":\"" + topicName + "\""));
    }


    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testJvmAndResources(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editSpec()
                .withResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity("400M"))
                    .addToLimits("cpu", new Quantity("2"))
                    .addToRequests("memory", new Quantity("300M"))
                    .addToRequests("cpu", new Quantity("1"))
                    .build())
                .withNewJvmOptions()
                    .withXmx("200m")
                    .withXms("200m")
                    .withXx(jvmOptionsXX)
                .endJvmOptions()
            .endSpec()
            .build());

        String podName = PodUtils.getPodNameByPrefix(KafkaConnectResources.deploymentName(clusterName));
        assertResources(NAMESPACE, podName, KafkaConnectResources.deploymentName(clusterName),
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(podName, KafkaConnectResources.deploymentName(clusterName),
                "-Xmx200m", "-Xms200m", "-XX:+UseG1GC");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SCALABILITY)
    void testKafkaConnectScaleUpScaleDown(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        LOGGER.info("Running kafkaConnectScaleUP {} in namespace", NAMESPACE);

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1).build());

        String deploymentName = KafkaConnectResources.deploymentName(clusterName);

        // kafka cluster Connect already deployed
        List<Pod> connectPods = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        int initialReplicas = connectPods.size();
        assertThat(initialReplicas, is(1));
        final int scaleTo = initialReplicas + 3;

        LOGGER.info("Scaling up to {}", scaleTo);
        KafkaConnectResource.replaceKafkaConnectResource(clusterName, c -> c.getSpec().setReplicas(scaleTo));

        DeploymentUtils.waitForDeploymentAndPodsReady(deploymentName, scaleTo);
        connectPods = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        assertThat(connectPods.size(), is(scaleTo));

        LOGGER.info("Scaling down to {}", initialReplicas);
        KafkaConnectResource.replaceKafkaConnectResource(clusterName, c -> c.getSpec().setReplicas(initialReplicas));

        DeploymentUtils.waitForDeploymentAndPodsReady(deploymentName, initialReplicas);
        connectPods = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        assertThat(connectPods.size(), is(initialReplicas));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectWithTlsAndTlsClientAuthentication(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaUser kafkaUser = KafkaUserTemplates.tlsUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, kafkaUser);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewTls()
                .addNewTrustedCertificate()
                    .withSecretName(clusterName + "-cluster-ca-cert")
                    .withCertificate("ca.crt")
                .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(clusterName + "-kafka-bootstrap:9093")
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(userName)
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                .endSpec()
                .build());

        String kafkaConnectPodName = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        String kafkaConnectLogs = kubeClient().logs(kafkaConnectPodName);

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", kafkaClientsPodName, topicName);
        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(clusterName, NAMESPACE, 8083));

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName + "-second", kafkaUser).build());

        final String kafkaClientsSecondPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName + "-second").get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsSecondPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesTls(),
                internalKafkaClient.receiveMessagesTls()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectWithTlsAndScramShaAuthentication(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        KafkaUser kafkaUser = KafkaUserTemplates.scramShaUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, kafkaUser);
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewTls()
                    .addNewTrustedCertificate()
                        .withSecretName(clusterName + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(clusterName + "-kafka-bootstrap:9093")
                .withNewKafkaClientAuthenticationScramSha512()
                    .withUsername(userName)
                    .withNewPasswordSecret()
                        .withSecretName(userName)
                        .withPassword("password")
                    .endPasswordSecret()
                .endKafkaClientAuthenticationScramSha512()
            .endSpec()
            .build());

        String kafkaConnectPodName = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        String kafkaConnectLogs = kubeClient().logs(kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", kafkaClientsPodName, topicName);
        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(clusterName, NAMESPACE, 8083));

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName + "-second", kafkaUser).build());

        final String kafkaClientsSecondPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName + "-second").get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsSecondPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCustomAndUpdatedValues(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        String usedVariable = "KAFKA_CONNECT_CONFIGURATION";

        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");
        envVarGeneral.put(usedVariable, "test.value");

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        Map<String, Object> connectConfig = new HashMap<>();
        connectConfig.put("config.storage.replication.factor", "1");
        connectConfig.put("offset.storage.replication.factor", "1");
        connectConfig.put("status.storage.replication.factor", "1");

        int initialDelaySeconds = 30;
        int timeoutSeconds = 10;
        int updatedInitialDelaySeconds = 31;
        int updatedTimeoutSeconds = 11;
        int periodSeconds = 10;
        int successThreshold = 1;
        int failureThreshold = 3;
        int updatedPeriodSeconds = 5;
        int updatedFailureThreshold = 1;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editSpec()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                    .endConnectContainer()
                .endTemplate()
                .withNewReadinessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endReadinessProbe()
                .withNewLivenessProbe()
                    .withInitialDelaySeconds(initialDelaySeconds)
                    .withTimeoutSeconds(timeoutSeconds)
                    .withPeriodSeconds(periodSeconds)
                    .withSuccessThreshold(successThreshold)
                    .withFailureThreshold(failureThreshold)
                .endLivenessProbe()
            .endSpec()
            .build());

        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(KafkaConnectResources.deploymentName(clusterName));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), envVarGeneral);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(
                StUtils.checkEnvVarInPod(kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName(), usedVariable),
                is(not("test.value"))
        );

        LOGGER.info("Updating values in MirrorMaker container");

        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kc -> {
            kc.getSpec().getTemplate().getConnectContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            kc.getSpec().setConfig(connectConfig);
            kc.getSpec().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kc.getSpec().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kc.getSpec().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kc.getSpec().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kc.getSpec().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kc.getSpec().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kc.getSpec().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            kc.getSpec().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
        });

        DeploymentUtils.waitTillDepHasRolled(KafkaConnectResources.deploymentName(clusterName), 1, connectSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), envVarUpdated);
        checkComponentConfiguration(KafkaConnectResources.deploymentName(clusterName), KafkaConnectResources.deploymentName(clusterName), "KAFKA_CONNECT_CONFIGURATION", connectConfig);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(CONNECTOR_OPERATOR)
    @OpenShiftOnly
    void testKafkaConnectorWithConnectAndConnectS2IWithSameName(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        String connectClusterName = "connect-cluster-1";
        String connectS2IClusterName = "connect-s2i-cluster-1";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        // Crate connect cluster with default connect image
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
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

        // Create different connect cluster via S2I resources
        resourceManager.createResource(extensionContext, false, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, clusterName, clusterName, 1)
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

        KafkaConnectS2IUtils.waitForConnectS2INotReady(clusterName);

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", "/tmp/test-file-sink.txt")
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        // Check that KafkaConnect contains created connector
        String connectPodName = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        KafkaConnectorUtils.waitForConnectorCreation(connectPodName, clusterName);

        KafkaConnectS2IUtils.waitForConnectS2INotReady(clusterName);

        String newTopic = "new-topic";
        String connectorConfig = KafkaConnectorUtils.getConnectorConfig(connectPodName, clusterName, "localhost");

        KafkaConnectorResource.replaceKafkaConnectorResource(clusterName, kc -> {
            kc.getSpec().getConfig().put("topics", newTopic);
            kc.getSpec().setTasksMax(8);
        });

        connectorConfig = KafkaConnectorUtils.waitForConnectorConfigUpdate(connectPodName, clusterName, connectorConfig, "localhost");
        assertThat(connectorConfig.contains("tasks.max\":\"8"), is(true));
        assertThat(connectorConfig.contains("topics\":\"" + newTopic), is(true));

        // Now delete KafkaConnector resource and create connector manually
        KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(clusterName).delete();

        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kc -> {
            kc.getMetadata().getAnnotations().remove(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES);
        });

        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(clusterName, NAMESPACE, 8083));
        final String connectorName = "sink-test";
        KafkaConnectorUtils.waitForConnectorCreation(connectPodName, connectorName);
        KafkaConnectorUtils.waitForConnectorStability(connectorName, connectPodName);
        KafkaConnectS2IUtils.waitForConnectS2INotReady(clusterName);

        KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(clusterName).delete();
        DeploymentConfigUtils.waitForDeploymentConfigDeletion(KafkaConnectS2IResources.deploymentName(clusterName));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ACCEPTANCE)
    void testMultiNodeKafkaConnectWithConnectorCreation(ExtensionContext extensionContext) {
        String connectClusterName = "connect-cluster-2";
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        // Crate connect cluster with default connect image
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 3)
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

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
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

        String execConnectPod =  kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();
        JsonObject connectStatus = new JsonObject(cmdKubeClient().execInPod(
                execConnectPod,
                "curl", "-X", "GET", "http://localhost:8083/connectors/" + clusterName + "/status").out()
        );
        String podIP = connectStatus.getJsonObject("connector").getString("worker_id").split(":")[0];
        String connectorPodName = kubeClient().listPods().stream().filter(pod ->
                pod.getStatus().getPodIP().equals(podIP)).findFirst().get().getMetadata().getName();

        internalKafkaClient.assertSentAndReceivedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(connectorPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(CONNECTOR_OPERATOR)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testConnectTlsAuthWithWeirdUserName(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        // Create weird named user with . and maximum of 64 chars -> TLS
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationTls())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, weirdUserName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .withNewTls()
                    .withTrustedCertificates(new CertSecretSourceBuilder()
                        .withCertificate("ca.crt")
                        .withNewSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                        .build())
                .endTls()
                .withNewKafkaClientAuthenticationTls()
                    .withNewCertificateAndKey()
                        .withSecretName(weirdUserName)
                        .withCertificate("user.crt")
                        .withKey("user.key")
                    .endCertificateAndKey()
                .endKafkaClientAuthenticationTls()
                .withBootstrapServers(KafkaResources.tlsBootstrapAddress(clusterName))
            .endSpec()
            .build());

        testConnectAuthorizationWithWeirdUserName(extensionContext, clusterName, weirdUserName, SecurityProtocol.SSL, topicName);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(CONNECTOR_OPERATOR)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testConnectScramShaAuthWithWeirdUserName(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasdsadasdasdasdasdgasgadfasdad";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .endGenericKafkaListener()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(true)
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(clusterName, weirdUserName).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editOrNewSpec()
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(clusterName))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withNewUsername(weirdUserName)
                        .withPasswordSecret(new PasswordSecretSourceBuilder()
                            .withSecretName(weirdUserName)
                            .withPassword("password")
                            .build())
                    .endKafkaClientAuthenticationScramSha512()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewTls()
                        .withTrustedCertificates(new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withNewSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                                .build())
                    .endTls()
                .endSpec()
                .build());

        testConnectAuthorizationWithWeirdUserName(extensionContext, clusterName, weirdUserName, SecurityProtocol.SASL_SSL, topicName);
    }

    void testConnectAuthorizationWithWeirdUserName(ExtensionContext extensionContext, String clusterName, String userName, SecurityProtocol securityProtocol, String topicName) {
        String connectorPodName = kubeClient().listPodsByPrefixInName(clusterName + "-connect").get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
            .endSpec()
            .build());

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(securityProtocol)
            .withTopicName(topicName)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(connectorPodName, Constants.DEFAULT_SINK_FILE_PATH);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SCALABILITY)
    void testScaleConnectWithoutConnectorToZero(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 2).build());

        String connectDeploymentName = KafkaConnectResources.deploymentName(clusterName);
        List<Pod> connectPods = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));

        assertThat(connectPods.size(), is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kafkaConnect -> kafkaConnect.getSpec().setReplicas(0));

        KafkaConnectUtils.waitForConnectReady(clusterName);
        PodUtils.waitForPodsReady(kubeClient().getDeploymentSelectors(connectDeploymentName), 0, true);

        connectPods = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        KafkaConnectStatus connectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus();

        assertThat(connectPods.size(), is(0));
        assertThat(connectStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SCALABILITY)
    @Tag(CONNECTOR_OPERATOR)
    void testScaleConnectWithConnectorToZero(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 2)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", topicName)
            .endSpec()
            .build());

        String connectDeploymentName = KafkaConnectResources.deploymentName(clusterName);
        List<Pod> connectPods = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));

        assertThat(connectPods.size(), is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kafkaConnect -> kafkaConnect.getSpec().setReplicas(0));

        KafkaConnectUtils.waitForConnectReady(clusterName);
        PodUtils.waitForPodsReady(kubeClient().getDeploymentSelectors(connectDeploymentName), 0, true);

        connectPods = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        KafkaConnectStatus connectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus();
        KafkaConnectorStatus connectorStatus = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus();

        assertThat(connectPods.size(), is(0));
        assertThat(connectStatus.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getType().equals(NotReady.toString())), is(true));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getMessage().contains("has 0 replicas")), is(true));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(SCALABILITY)
    @Tag(CONNECTOR_OPERATOR)
    void testScaleConnectAndConnectorSubresource(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", topicName)
            .endSpec()
            .build());

        int scaleTo = 4;
        long connectObsGen = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getObservedGeneration();
        String connectGenName = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaConnect subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient().scaleByName(KafkaConnect.RESOURCE_KIND, clusterName, scaleTo);
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaConnectResources.deploymentName(clusterName), scaleTo);

        LOGGER.info("Check if replicas is set to {}, observed generation is higher - for spec and status - naming prefix should be same", scaleTo);
        List<Pod> connectPods = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName));
        assertThat(connectPods.size(), is(4));
        assertThat(KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getReplicas(), is(4));
        assertThat(KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getReplicas(), is(4));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(connectObsGen < KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getObservedGeneration(), is(true));
        for (Pod pod : connectPods) {
            assertThat(pod.getMetadata().getName().contains(connectGenName), is(true));
        }

        LOGGER.info("-------> Scaling KafkaConnector subresource <-------");
        LOGGER.info("Scaling subresource task max to {}", scaleTo);
        cmdKubeClient().scaleByName(KafkaConnector.RESOURCE_KIND, clusterName, scaleTo);
        KafkaConnectorUtils.waitForConnectorsTaskMaxChange(clusterName, scaleTo);

        LOGGER.info("Check if taskMax is set to {}", scaleTo);
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getTasksMax(), is(scaleTo));
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getTasksMax(), is(scaleTo));

        LOGGER.info("Check taskMax on Connect pods API");
        for (Pod pod : connectPods) {
            JsonObject json = new JsonObject(KafkaConnectorUtils.getConnectorSpecFromConnectAPI(pod.getMetadata().getName(), clusterName));
            assertThat(Integer.parseInt(json.getJsonObject("config").getString("tasks.max")), is(scaleTo));
        }
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMountingSecretAndConfigMapAsVolumesAndEnvVars(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        String secretPassword = "password";
        String encodedPassword = Base64.getEncoder().encodeToString(secretPassword.getBytes());

        String secretEnv = "MY_CONNECT_SECRET";
        String configMapEnv = "MY_CONNECT_CONFIG_MAP";

        String dotedSecretEnv = "MY_DOTED_CONNECT_SECRET";
        String dotedConfigMapEnv = "MY_DOTED_CONNECT_CONFIG_MAP";

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

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
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

        String connectPodName = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();

        LOGGER.info("Check if the ENVs contains desired values");
        assertThat(cmdKubeClient().execInPod(connectPodName, "/bin/bash", "-c", "printenv " + secretEnv).out().trim(), equalTo(secretPassword));
        assertThat(cmdKubeClient().execInPod(connectPodName, "/bin/bash", "-c", "printenv " + configMapEnv).out().trim(), equalTo(configMapValue));
        assertThat(cmdKubeClient().execInPod(connectPodName, "/bin/bash", "-c", "printenv " + dotedSecretEnv).out().trim(), equalTo(secretPassword));
        assertThat(cmdKubeClient().execInPod(connectPodName, "/bin/bash", "-c", "printenv " + dotedConfigMapEnv).out().trim(), equalTo(configMapValue));

        LOGGER.info("Check if volumes contains desired values");
        assertThat(
            cmdKubeClient().execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + configMapName + "/" + configMapKey).out().trim(),
            equalTo(configMapValue)
        );
        assertThat(
            cmdKubeClient().execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + secretName + "/" + secretKey).out().trim(),
            equalTo(secretPassword)
        );
        assertThat(
            cmdKubeClient().execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + dotedConfigMapName + "/" + configMapKey).out().trim(),
            equalTo(configMapValue)
        );
        assertThat(
            cmdKubeClient().execInPod(connectPodName, "/bin/bash", "-c", "cat external-configuration/" + dotedSecretName + "/" + secretKey).out().trim(),
            equalTo(secretPassword)
        );
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testHostAliases(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        HostAlias hostAlias = new HostAliasBuilder()
            .withIp(aliasIp)
            .withHostnames(aliasHostname)
            .build();

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editSpec()
                .withNewTemplate()
                    .withNewPod()
                        .withHostAliases(hostAlias)
                    .endPod()
                .endTemplate()
            .endSpec()
            .build());

        String connectPodName = kubeClient().listPodsByPrefixInName(KafkaConnectResources.deploymentName(clusterName)).get(0).getMetadata().getName();

        LOGGER.info("Checking the /etc/hosts file");
        String output = cmdKubeClient().execInPod(connectPodName, "cat", "/etc/hosts").out();
        assertThat(output, containsString(etcHostsData));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testConfigureDeploymentStrategy(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, clusterName, 1)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .build());

        String connectDepName = KafkaConnectResources.deploymentName(clusterName);

        LOGGER.info("Adding label to Connect resource, the CR should be recreated");
        KafkaConnectResource.replaceKafkaConnectResource(clusterName,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap("some", "label")));
        DeploymentUtils.waitForDeploymentAndPodsReady(connectDepName, 1);

        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kafkaConnect.getStatus().getObservedGeneration(), is(1L));
        assertThat(kafkaConnect.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kafkaConnect.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaConnectResource.replaceKafkaConnectResource(clusterName,
            kc -> kc.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE));
        KafkaConnectUtils.waitForConnectReady(clusterName);

        LOGGER.info("Adding another label to Connect resource, pods should be rolled");
        KafkaConnectResource.replaceKafkaConnectResource(clusterName, kc -> kc.getMetadata().getLabels().put("another", "label"));
        DeploymentUtils.waitForDeploymentAndPodsReady(connectDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get();
        assertThat(kafkaConnect.getStatus().getObservedGeneration(), is(2L));
        assertThat(kafkaConnect.getMetadata().getLabels().toString(), containsString("another=label"));
        assertThat(kafkaConnect.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.ROLLING_UPDATE));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT);

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, KAFKA_CLIENTS_NAME).build());

        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();
    }
}
