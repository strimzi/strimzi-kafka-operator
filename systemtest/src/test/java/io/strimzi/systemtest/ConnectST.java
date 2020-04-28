/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSourceBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectS2IUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TRAVIS;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(CONNECT_COMPONENTS)
class ConnectST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(ConnectST.class);
    public static final String NAMESPACE = "connect-cluster-test";

    private static final String CONNECT_TOPIC_NAME = "connect-topic-example";

    private String kafkaClientsPodName;

    @Test
    void testDeployUndeploy() {
        Map<String, Object> exceptedConfig = StUtils.loadProperties("group.id=" + KafkaConnectResources.deploymentName(CLUSTER_NAME) + "\n" +
                "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "config.storage.topic=" + KafkaConnectResources.metricsAndLogConfigMapName(CLUSTER_NAME) + "\n" +
                "status.storage.topic=" + KafkaConnectResources.configStorageTopicStatus(CLUSTER_NAME) + "\n" +
                "offset.storage.topic=" + KafkaConnectResources.configStorageTopicOffsets(CLUSTER_NAME) + "\n");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1).done();
        LOGGER.info("Looks like the connect cluster my-cluster deployed OK");

        String podName = PodUtils.getPodNameByPrefix(KafkaConnectResources.deploymentName(CLUSTER_NAME));
        String kafkaPodJson = TestUtils.toJsonString(kubeClient().getPod(podName));

        assertThat(kafkaPodJson, hasJsonPath(StUtils.globalVariableJsonPathBuilder(0, "KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))));
        assertThat(StUtils.getPropertiesFromJson(0, kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(exceptedConfig));
        testDockerImagesForKafkaConnect();

        verifyLabelsOnPods(CLUSTER_NAME, "connect", null, "KafkaConnect");
        verifyLabelsForService(CLUSTER_NAME, "connect-api", "KafkaConnect");
        verifyLabelsForConfigMaps(CLUSTER_NAME, null, "");
        verifyLabelsForServiceAccounts(CLUSTER_NAME, null);
    }

    private void testDockerImagesForKafkaConnect() {
        LOGGER.info("Verifying docker image names");
        Map<String, String> imgFromDeplConf = getImagesFromConfig();
        //Verifying docker image for kafka connect
        String connectImageName = PodUtils.getFirstContainerImageNameFromPod(kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, "KafkaConnect").
                get(0).getMetadata().getName());

        String connectVersion = Crds.kafkaConnectOperation(kubeClient().getClient()).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getVersion();
        if (connectVersion == null) {
            connectVersion = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_CONNECT_IMAGE_MAP)).get(connectVersion), is(connectImageName));
        LOGGER.info("Docker images verified");
    }

    @Test
    @Tag(TRAVIS)
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectWithFileSinkPlugin() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec().done();
        KafkaTopicResource.topic(CLUSTER_NAME, CONNECT_TOPIC_NAME).done();

        String kafkaConnectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, CONNECT_TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083));

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(CONNECT_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        LOGGER.info("Deleting topic {} from CR", CONNECT_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", CONNECT_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(CONNECT_TOPIC_NAME);
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectWithPlainAndScramShaAuthentication() {
        // Use a Kafka with plain listener disabled
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewPlain()
                                .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .endPlain()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        KafkaUser kafkaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, USER_NAME).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withNewUsername(USER_NAME)
                        .withPasswordSecret(new PasswordSecretSourceBuilder()
                            .withSecretName(USER_NAME)
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
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, CONNECT_TOPIC_NAME).done();

        String kafkaConnectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();
        String kafkaConnectLogs = kubeClient().logs(kafkaConnectPodName);

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", kafkaClientsPodName, CONNECT_TOPIC_NAME);
        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, CONNECT_TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083));

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME + "-second", kafkaUser).done();

        final String kafkaClientsSecondPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME + "-second").get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsSecondPodName)
            .withTopicName(CONNECT_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(USER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesPlain(),
                internalKafkaClient.receiveMessagesPlain()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(CONNECT_TOPIC_NAME).cascading(true).delete();
        LOGGER.info("Topic {} deleted", CONNECT_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(CONNECT_TOPIC_NAME);
    }

    @Test
    @Tag(ACCEPTANCE)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaConnectAndConnectorFileSinkPlugin() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                .endSpec().done();

        String connectorName = "license-source";
        KafkaConnectorResource.kafkaConnector(connectorName, CLUSTER_NAME, 2)
            .editSpec()
                .addToConfig("topic", TOPIC_NAME)
            .endSpec().done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        int received = internalKafkaClient.receiveMessagesPlain();
        assertThat(received, greaterThanOrEqualTo(MESSAGE_COUNT));

        String service = KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083);
        String output = cmdKubeClient().execInPod(kafkaClientsPodName, "/bin/bash", "-c", "curl " + service + "/connectors/" + connectorName).out();
        assertThat(output, containsString("\"name\":\"license-source\""));
        assertThat(output, containsString("\"connector.class\":\"org.apache.kafka.connect.file.FileStreamSourceConnector\""));
        assertThat(output, containsString("\"tasks.max\":\"2\""));
        assertThat(output, containsString("\"topic\":\"" + TOPIC_NAME + "\""));

        LOGGER.info("Deleting connector {} CR", connectorName);
        cmdKubeClient().deleteByName("kafkaconnector", connectorName);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }


    @Test
    void testJvmAndResources() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect")
            .endMetadata()
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
                        .withServer(true)
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endSpec()
                .done();

        String podName = PodUtils.getPodNameByPrefix(KafkaConnectResources.deploymentName(CLUSTER_NAME));
        assertResources(NAMESPACE, podName, KafkaConnectResources.deploymentName(CLUSTER_NAME),
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(podName, KafkaConnectResources.deploymentName(CLUSTER_NAME),
                "-Xmx200m", "-Xms200m", "-server", "-XX:+UseG1GC");
    }

    @Test
    void testKafkaConnectScaleUpScaleDown() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        LOGGER.info("Running kafkaConnectScaleUP {} in namespace", NAMESPACE);
        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1).done();

        String deploymentName = KafkaConnectResources.deploymentName(CLUSTER_NAME);

        // kafka cluster Connect already deployed
        List<String> connectPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, "KafkaConnect");
        int initialReplicas = connectPods.size();
        assertThat(initialReplicas, is(1));
        final int scaleTo = initialReplicas + 1;

        LOGGER.info("Scaling up to {}", scaleTo);
        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, c -> c.getSpec().setReplicas(scaleTo));

        DeploymentUtils.waitForDeploymentAndPodsReady(deploymentName, scaleTo);
        connectPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, "KafkaConnect");
        assertThat(connectPods.size(), is(scaleTo));

        LOGGER.info("Scaling down to {}", initialReplicas);
        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, c -> c.getSpec().setReplicas(initialReplicas));

        DeploymentUtils.waitForDeploymentAndPodsReady(deploymentName, initialReplicas);
        connectPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, "KafkaConnect");
        assertThat(connectPods.size(), is(initialReplicas));
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectWithTlsAndTlsClientAuthentication() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewTls()
                                .withAuth(new KafkaListenerAuthenticationTls())
                            .endTls()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        KafkaUser kafkaUser = KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(CLUSTER_NAME + "-cluster-ca-cert")
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(CLUSTER_NAME + "-kafka-bootstrap:9093")
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName(USER_NAME)
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, CONNECT_TOPIC_NAME).done();

        String kafkaConnectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();
        String kafkaConnectLogs = kubeClient().logs(kafkaConnectPodName);

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", kafkaClientsPodName, CONNECT_TOPIC_NAME);
        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, CONNECT_TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083));

        KafkaClientsResource.deployKafkaClients(true, KAFKA_CLIENTS_NAME + "-second", kafkaUser).done();

        final String kafkaClientsSecondPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME + "-second").get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsSecondPodName)
            .withTopicName(CONNECT_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(USER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesTls(),
                internalKafkaClient.receiveMessagesTls()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        LOGGER.info("Deleting topic {} from CR", CONNECT_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", CONNECT_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(CONNECT_TOPIC_NAME);
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectWithTlsAndScramShaAuthentication() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                        .endTls()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        KafkaUser kafkaUser = KafkaUserResource.scramShaUser(CLUSTER_NAME, USER_NAME).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(CLUSTER_NAME + "-cluster-ca-cert")
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(CLUSTER_NAME + "-kafka-bootstrap:9093")
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(USER_NAME)
                        .withNewPasswordSecret()
                            .withSecretName(USER_NAME)
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, CONNECT_TOPIC_NAME).done();

        String kafkaConnectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();
        String kafkaConnectLogs = kubeClient().logs(kafkaConnectPodName);

        LOGGER.info("Verifying that KafkaConnect pod logs don't contain ERRORs");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", kafkaClientsPodName, CONNECT_TOPIC_NAME);
        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, CONNECT_TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083));

        KafkaClientsResource.deployKafkaClients(true, KAFKA_CLIENTS_NAME + "-second", kafkaUser).done();

        final String kafkaClientsSecondPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME + "-second").get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsSecondPodName)
            .withTopicName(CONNECT_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(USER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesTls(),
                internalKafkaClient.receiveMessagesTls()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        LOGGER.info("Deleting topic {} from CR", CONNECT_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", CONNECT_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(CONNECT_TOPIC_NAME);
    }

    @Test
    void testCustomAndUpdatedValues() {
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

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
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
            .endSpec().done();

        Map<String, String> connectSnapshot = DeploymentUtils.depSnapshot(KafkaConnectResources.deploymentName(CLUSTER_NAME));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), envVarGeneral);

        LOGGER.info("Check if actual env variable {} has different value than {}", usedVariable, "test.value");
        assertThat(
                StUtils.checkEnvVarInPod(kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName(), usedVariable),
                CoreMatchers.is(CoreMatchers.not("test.value"))
        );

        LOGGER.info("Updating values in MirrorMaker container");
        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kc -> {
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

        DeploymentUtils.waitTillDepHasRolled(KafkaConnectResources.deploymentName(CLUSTER_NAME), 1, connectSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), envVarUpdated);
        checkComponentConfiguration(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), "KAFKA_CONNECT_CONFIGURATION", connectConfig);
    }

    @Test
    @Tag(CONNECTOR_OPERATOR)
    void testKafkaConnectorWithConnectAndConnectS2IWithSameName() {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        String connectClusterName = "connect-cluster";
        String connectS2IClusterName = "connect-s2i-cluster";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        // Crate connect cluster with default connect image
        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect")
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("group.id", connectClusterName)
                .addToConfig("offset.storage.topic", connectClusterName + "-offsets")
                .addToConfig("config.storage.topic", connectClusterName + "-config")
                .addToConfig("status.storage.topic", connectClusterName + "-status")
            .endSpec().done();

        // Create different connect cluster via S2I resources
        KafkaConnectS2IResource.kafkaConnectS2IWithoutWait(KafkaConnectS2IResource.defaultKafkaConnectS2I(CLUSTER_NAME, CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect-s2i")
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("group.id", connectS2IClusterName)
                .addToConfig("offset.storage.topic", connectS2IClusterName + "-offsets")
                .addToConfig("config.storage.topic", connectS2IClusterName + "-config")
                .addToConfig("status.storage.topic", connectS2IClusterName + "-status")
            .endSpec().build());

        KafkaConnectS2IUtils.waitForConnectS2INotReady(CLUSTER_NAME);

        KafkaConnectorResource.kafkaConnector(CLUSTER_NAME)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", "/tmp/test-file-sink.txt")
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec().done();

        // Check that KafkaConnect contains created connector
        String connectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();
        KafkaConnectorUtils.waitForConnectorCreation(connectPodName, CLUSTER_NAME);

        KafkaConnectS2IUtils.waitForConnectS2INotReady(CLUSTER_NAME);

        String newTopic = "new-topic";
        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME, kc -> {
            kc.getSpec().getConfig().put("topics", newTopic);
            kc.getSpec().setTasksMax(8);
        });

        TestUtils.waitFor("mongodb.user and tasks.max upgrade in S2I connector", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.TIMEOUT_AVAILABILITY_TEST,
            () -> {
                String connectorConfig = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083) + "/connectors/" + CLUSTER_NAME + "/config").out();
                return connectorConfig.contains("tasks.max\":\"8") && connectorConfig.contains("topics\":\"" + newTopic);
            });

        // Now delete KafkaConnector resource and create connector manually
        KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kc -> {
            kc.getMetadata().getAnnotations().remove(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES);
        });

        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083));
        final String connectorName = "sink-test";
        KafkaConnectorUtils.waitForConnectorCreation(connectPodName, connectorName);
        KafkaConnectorUtils.waitForConnectorStability(connectorName, connectPodName);
        KafkaConnectS2IUtils.waitForConnectS2INotReady(CLUSTER_NAME);
        KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
    }

    @Test
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(ACCEPTANCE)
    void testMultiNodeKafkaConnectWithConnectorCreation() {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        String connectClusterName = "connect-cluster";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        // Crate connect cluster with default connect image
        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 3)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editSpec()
                    .addToConfig("group.id", connectClusterName)
                    .addToConfig("offset.storage.topic", connectClusterName + "-offsets")
                    .addToConfig("config.storage.topic", connectClusterName + "-config")
                    .addToConfig("status.storage.topic", connectClusterName + "-status")
                .endSpec().done();

        KafkaConnectorResource.kafkaConnector(CLUSTER_NAME)
                .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .endSpec().done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        String execConnectPod =  kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();
        JsonObject connectStatus = new JsonObject(cmdKubeClient().execInPod(
                execConnectPod,
                "curl", "-X", "GET", "http://localhost:8083/connectors/" + CLUSTER_NAME + "/status").out()
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
    @Test
    void testConnectTlsAuthWithWeirdUserName() {
        // Create weird named user with . and maximum of 64 chars -> TLS
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withAuth(new KafkaListenerAuthenticationTls())
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        KafkaUserResource.tlsUser(CLUSTER_NAME, weirdUserName).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
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
                            .withNewSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME))
                            .build())
                    .endTls()
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
                .endSpec()
                .done();

        testConnectAuthorizationWithWeirdUserName(weirdUserName, SecurityProtocol.SSL);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(CONNECTOR_OPERATOR)
    @Test
    void testConnectScramShaAuthWithWeirdUserName() {
        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasdsadasdasdasdasdgasgadfasdad";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .editOrNewListeners()
                            .withNewTls()
                                .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .endTls()
                            .withNewKafkaListenerExternalNodePort()
                                .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        KafkaUserResource.scramShaUser(CLUSTER_NAME, weirdUserName).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editOrNewSpec()
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
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
                                .withNewSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME))
                                .build())
                    .endTls()
                .endSpec()
                .done();

        testConnectAuthorizationWithWeirdUserName(weirdUserName, SecurityProtocol.SASL_SSL);
    }

    void testConnectAuthorizationWithWeirdUserName(String userName, SecurityProtocol securityProtocol) {
        String topicName = TOPIC_NAME + rng.nextInt(Integer.MAX_VALUE);
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        String connectorPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-connect").get(0).getMetadata().getName();

        KafkaConnectorResource.kafkaConnector(CLUSTER_NAME)
                .editSpec()
                    .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                    .addToConfig("topics", topicName)
                    .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .endSpec().done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withKafkaUsername(userName)
            .withMessageCount(MESSAGE_COUNT)
            .withSecurityProtocol(securityProtocol)
            .withTopicName(topicName)
            .build();

        assertThat(basicExternalKafkaClient.sendMessagesTls(), is(MESSAGE_COUNT));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(connectorPodName, Constants.DEFAULT_SINK_FILE_PATH);
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) throws InterruptedException {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
        deployKafkaClients();
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        cluster.setNamespace(NAMESPACE);
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT).done();

        deployKafkaClients();
    }

    private void deployKafkaClients() {
        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();
        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();
    }
}