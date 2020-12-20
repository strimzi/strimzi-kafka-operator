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
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectS2IUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
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
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.ResourceManager;

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
    private static final String CONNECT_S2I_CLUSTER_NAME = CLUSTER_NAME + "-s2i";
    private static final String SECOND_CLUSTER_NAME = "second-" + CLUSTER_NAME;

    private String kafkaClientsPodName;

    @Test
    void testDeployS2IWithMongoDBPlugin() throws InterruptedException, IOException {
        final String kafkaConnectS2IName = "kafka-connect-s2i-name-1";
        // Calls to Connect API are executed from kafka-0 pod
        deployConnectS2IWithMongoDb(kafkaConnectS2IName, false);

        String mongoDbConfig = "{" +
                "\"name\": \"" + kafkaConnectS2IName + "\"," +
                "\"config\": {" +
                "   \"connector.class\" : \"io.debezium.connector.mongodb.MongoDbConnector\"," +
                "   \"tasks.max\" : \"1\"," +
                "   \"mongodb.hosts\" : \"debezium/localhost:27017\"," +
                "   \"mongodb.name\" : \"dbserver1\"," +
                "   \"mongodb.user\" : \"debezium\"," +
                "   \"mongodb.password\" : \"dbz\"," +
                "   \"database.history.kafka.bootstrap.servers\" : \"localhost:9092\"}" +
                "}";

        KafkaConnectS2IUtils.waitForConnectS2IReady(kafkaConnectS2IName);

        TestUtils.waitFor("ConnectS2I will be ready and POST will be executed", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            String createConnectorOutput = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "POST", "-H", "Accept:application/json", "-H", "Content-Type:application/json",
                    "http://" + KafkaConnectS2IResources.serviceName(kafkaConnectS2IName) + ":8083/connectors/", "-d", mongoDbConfig).out();
            LOGGER.info("Create Connector result: {}", createConnectorOutput);
            return !createConnectorOutput.contains("error_code");
        });

        // Make sure that connector is really created
        Thread.sleep(10_000);

        String connectorStatus = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", "http://" + KafkaConnectS2IResources.serviceName(kafkaConnectS2IName) + ":8083/connectors/" + kafkaConnectS2IName + "/status").out();

        assertThat(connectorStatus, containsString("RUNNING"));
    }

    @Test
    @Tag(CONNECTOR_OPERATOR)
    void testDeployS2IAndKafkaConnectorWithMongoDBPlugin() throws IOException {
        final String kafkaConnectS2IName = "kafka-connect-s2i-name-11";
        // Calls to Connect API are executed from kafka-0 pod
        deployConnectS2IWithMongoDb(kafkaConnectS2IName, true);

        // Make sure that Connect API is ready
        KafkaConnectS2IUtils.waitForConnectS2IReady(kafkaConnectS2IName);

        KafkaConnectorResource.kafkaConnector(kafkaConnectS2IName)
            .withNewSpec()
                .withClassName("io.debezium.connector.mongodb.MongoDbConnector")
                .withTasksMax(2)
                .addToConfig("mongodb.hosts", "debezium/localhost:27017")
                .addToConfig("mongodb.name", "dbserver1")
                .addToConfig("mongodb.user", "debezium")
                .addToConfig("mongodb.password", "dbz")
                .addToConfig("database.history.kafka.bootstrap.servers", "localhost:9092")
            .endSpec().done();

        checkConnectorInStatus(NAMESPACE, kafkaConnectS2IName);
        String apiUrl = KafkaConnectS2IResources.serviceName(kafkaConnectS2IName);

        String connectorStatus = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", "http://" + apiUrl + ":8083/connectors/" + kafkaConnectS2IName + "/status").out();
        assertThat(connectorStatus, containsString("RUNNING"));

        String connectorConfig = KafkaConnectorUtils.getConnectorConfig(kafkaClientsPodName, kafkaConnectS2IName, apiUrl);
        KafkaConnectorResource.replaceKafkaConnectorResource(kafkaConnectS2IName, kC -> {
            Map<String, Object> config = kC.getSpec().getConfig();
            config.put("mongodb.user", "test-user");
            kC.getSpec().setConfig(config);
            kC.getSpec().setTasksMax(8);
        });

        connectorConfig = KafkaConnectorUtils.waitForConnectorConfigUpdate(kafkaClientsPodName, kafkaConnectS2IName, connectorConfig, apiUrl);
        assertThat(connectorConfig.contains("tasks.max\":\"8"), is(true));
        assertThat(connectorConfig.contains("mongodb.user\":\"test-user"), is(true));
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testSecretsWithKafkaConnectS2IWithTlsAndScramShaAuthentication() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
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
            .done();

        final String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();
        final String kafkaConnectS2IName = "kafka-connect-s2i-name-2";

        KafkaTopicResource.topic(CLUSTER_NAME, CONNECT_S2I_TOPIC_NAME).done();
        KafkaUser user = KafkaUserResource.scramShaUser(CLUSTER_NAME, userName).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-tls-" + Constants.KAFKA_CLIENTS, user).done();

        KafkaConnectS2IResource.kafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME))
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(userName)
                        .withNewPasswordSecret()
                            .withSecretName(userName)
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .done();

        final String tlsKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-tls-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(tlsKafkaClientsPodName)
            .withTopicName(CONNECT_S2I_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        String kafkaConnectS2IPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getName();
        String kafkaConnectS2ILogs = kubeClient().logs(kafkaConnectS2IPodName);

        LOGGER.info("Verifying that in KafkaConnect logs not contain ERRORs");
        assertThat(kafkaConnectS2ILogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector via pod {} with topic {}", tlsKafkaClientsPodName, CONNECT_S2I_TOPIC_NAME);
        KafkaConnectorUtils.createFileSinkConnector(tlsKafkaClientsPodName, CONNECT_S2I_TOPIC_NAME, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(kafkaConnectS2IName, NAMESPACE, 8083));

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesTls(),
            internalKafkaClient.receiveMessagesTls()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectS2IPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");

        assertThat(cmdKubeClient().execInPod(kafkaConnectS2IPodName, "/bin/bash", "-c", "cat " + Constants.DEFAULT_SINK_FILE_PATH).out(),
                containsString("99"));
    }

    @Test
    void testCustomAndUpdatedValues() {
        final String kafkaConnectS2IName = "kafka-connect-s2i-name-3";
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");

        KafkaConnectS2IResource.kafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
            .editSpec()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .done();

        String depConfName = KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName);

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        Map<String, String> connectSnapshot = DeploymentConfigUtils.depConfigSnapshot(KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName));

        LOGGER.info("Verify values before update");

        LabelSelector deploymentConfigSelector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName))).build();
        String connectPodName = kubeClient().listPods(deploymentConfigSelector).get(0).getMetadata().getName();

        checkSpecificVariablesInContainer(connectPodName, KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName), envVarGeneral);

        LOGGER.info("Updating values in ConnectS2I container");
        KafkaConnectS2IResource.replaceConnectS2IResource(kafkaConnectS2IName, kc -> {
            kc.getSpec().getTemplate().getConnectContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
        });

        DeploymentConfigUtils.waitTillDepConfigHasRolled(depConfName, connectSnapshot);

        deploymentConfigSelector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName))).build();
        connectPodName = kubeClient().listPods(deploymentConfigSelector).get(0).getMetadata().getName();

        LOGGER.info("Verify values after update");
        checkSpecificVariablesInContainer(connectPodName, depConfName, envVarUpdated);
    }

    @Test
    void testJvmAndResources() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        final String kafkaConnectS2IName = "kafka-connect-s2i-name-4";

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        KafkaConnectS2I kafkaConnectS2I = KafkaConnectS2IResource.kafkaConnectS2IWithoutWait(KafkaConnectS2IResource.defaultKafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
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
            .endSpec().build());

        KafkaConnectS2IUtils.waitForConnectS2INotReady(kafkaConnectS2IName);

        TestUtils.waitFor("build status: Pending", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_AVAILABILITY_TEST,
            () -> kubeClient().getClient().adapt(OpenShiftClient.class).builds().inNamespace(NAMESPACE).withName(kafkaConnectS2IName + "-connect-1").get().getStatus().getPhase().equals("Pending"));

        kubeClient().getClient().adapt(OpenShiftClient.class).builds().inNamespace(NAMESPACE).withName(kafkaConnectS2IName + "-connect-1").withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

        KafkaConnectS2IResource.replaceConnectS2IResource(kafkaConnectS2IName, kc -> {
            kc.getSpec().setBuildResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity("1000M"))
                    .addToLimits("cpu", new Quantity("1"))
                    .addToRequests("memory", new Quantity("400M"))
                    .addToRequests("cpu", new Quantity("1"))
                    .build());
        });

        TestUtils.waitFor("KafkaConnect change", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> kubeClient().getClient().adapt(OpenShiftClient.class).buildConfigs().inNamespace(NAMESPACE).withName(kafkaConnectS2IName + "-connect").get().getSpec().getResources().getRequests().get("cpu").equals(new Quantity("1")));

        cmdKubeClient().exec("start-build", KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName), "-n", NAMESPACE);

        KafkaConnectS2IUtils.waitForConnectS2IReady(kafkaConnectS2IName);

        String podName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getName();

        assertResources(NAMESPACE, podName, kafkaConnectS2IName + "-connect",
            "400M", "2", "300M", "1");
        assertExpectedJavaOpts(podName, kafkaConnectS2IName + "-connect",
            "-Xmx200m", "-Xms200m", "-XX:+UseG1GC");

        KafkaConnectS2IResource.deleteKafkaConnectS2IWithoutWait(kafkaConnectS2IName);
        DeploymentConfigUtils.waitForDeploymentConfigDeletion(KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName));
    }

    @Test
    @Tag(CONNECTOR_OPERATOR)
    @Tag(ACCEPTANCE)
    void testKafkaConnectorWithConnectS2IAndConnectWithSameName() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        String connectClusterName = "connect-cluster";
        String connectS2IClusterName = "connect-s2i-cluster";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        // Create different connect cluster via S2I resources
        KafkaConnectS2IResource.kafkaConnectS2I(CLUSTER_NAME, CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
                .editSpec()
                .addToConfig("group.id", connectS2IClusterName)
                .addToConfig("offset.storage.topic", connectS2IClusterName + "-offsets")
                .addToConfig("config.storage.topic", connectS2IClusterName + "-config")
                .addToConfig("status.storage.topic", connectS2IClusterName + "-status")
            .endSpec().done();

        // Create connect cluster with default connect image
        KafkaConnectResource.kafkaConnectWithoutWait(KafkaConnectResource.defaultKafkaConnect(CLUSTER_NAME, CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .addToConfig("group.id", connectClusterName)
                .addToConfig("offset.storage.topic", connectClusterName + "-offsets")
                .addToConfig("config.storage.topic", connectClusterName + "-config")
                .addToConfig("status.storage.topic", connectClusterName + "-status")
            .endSpec().build());

        KafkaConnectUtils.waitForConnectNotReady(CLUSTER_NAME);

        KafkaConnectorResource.kafkaConnector(CLUSTER_NAME)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", "/tmp/test-file-sink.txt")
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec().done();

        // Check that KafkaConnectS2I contains created connector
        String connectS2IPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getName();
        KafkaConnectorUtils.waitForConnectorCreation(connectS2IPodName, CLUSTER_NAME);

        KafkaConnectUtils.waitForConnectNotReady(CLUSTER_NAME);

        String connectorConfig = KafkaConnectorUtils.getConnectorConfig(connectS2IPodName, CLUSTER_NAME, "localhost");

        String newTopic = "new-topic";
        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME, kc -> {
            kc.getSpec().getConfig().put("topics", newTopic);
            kc.getSpec().setTasksMax(8);
        });

        connectorConfig = KafkaConnectorUtils.waitForConnectorConfigUpdate(connectS2IPodName, CLUSTER_NAME, connectorConfig, "localhost");
        assertThat(connectorConfig.contains("tasks.max\":\"8"), is(true));
        assertThat(connectorConfig.contains("topics\":\"" + newTopic), is(true));

        // Now delete KafkaConnector resource and create connector manually
        KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();

        KafkaConnectS2IResource.replaceConnectS2IResource(CLUSTER_NAME, kc -> {
            kc.getMetadata().getAnnotations().remove(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES);
        });

        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083));
        final String connectorName = "sink-test";
        KafkaConnectorUtils.waitForConnectorCreation(connectS2IPodName, connectorName);
        KafkaConnectorUtils.waitForConnectorStability(connectorName, connectS2IPodName);
        KafkaConnectUtils.waitForConnectNotReady(CLUSTER_NAME);

        KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
        DeploymentUtils.waitForDeploymentDeletion(KafkaConnectResources.deploymentName(CLUSTER_NAME));
    }

    @Test
    @Tag(CONNECTOR_OPERATOR)
    @Tag(INTERNAL_CLIENTS_USED)
    void testMultiNodeKafkaConnectS2IWithConnectorCreation() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        String connectS2IClusterName = "connect-s2i-cluster";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        // Crate connect cluster with default connect image
        KafkaConnectS2IResource.kafkaConnectS2I(CLUSTER_NAME, CLUSTER_NAME, 3)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editSpec()
                    .addToConfig("group.id", connectS2IClusterName)
                    .addToConfig("offset.storage.topic", connectS2IClusterName + "-offsets")
                    .addToConfig("config.storage.topic", connectS2IClusterName + "-config")
                    .addToConfig("status.storage.topic", connectS2IClusterName + "-status")
                .endSpec().done();

        KafkaConnectorResource.kafkaConnector(CLUSTER_NAME)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", topicName)
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .done();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        String execConnectPod =  kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getName();
        JsonObject connectStatus = new JsonObject(cmdKubeClient().execInPod(
                execConnectPod,
                "curl", "-X", "GET", "http://localhost:8083/connectors/" + CLUSTER_NAME + "/status").out()
        );
        String podIP = connectStatus.getJsonObject("connector").getString("worker_id").split(":")[0];
        String connectorPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).stream().filter(pod ->
                pod.getStatus().getPodIP().equals(podIP)).findFirst().get().getMetadata().getName();

        internalKafkaClient.assertSentAndReceivedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(connectorPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }

    @Test
    void testChangeConnectS2IConfig() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
        KafkaResource.kafkaEphemeral(SECOND_CLUSTER_NAME, 3, 1).done();

        String bootstrapAddress = KafkaResources.tlsBootstrapAddress(SECOND_CLUSTER_NAME);

        KafkaConnectS2IResource.kafkaConnectS2I(CONNECT_S2I_CLUSTER_NAME, CLUSTER_NAME, 1)
                .editMetadata()
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .editSpec()
                    .withVersion(TestKafkaVersion.getKafkaVersions().get(0).version())
                .endSpec()
                .done();

        String deploymentConfigName = KafkaConnectS2IResources.deploymentName(CONNECT_S2I_CLUSTER_NAME);

        List<String> connectPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND);

        LOGGER.info("===== SCALING UP AND DOWN =====");

        final int initialReplicas = connectPods.size();
        assertThat(initialReplicas, is(1));

        final int scaleReplicasTo = initialReplicas + 3;

        //scale up
        LOGGER.info("Scaling up to {}", scaleReplicasTo);
        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECT_S2I_CLUSTER_NAME, cs2i ->
                cs2i.getSpec().setReplicas(scaleReplicasTo));
        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(deploymentConfigName, scaleReplicasTo);

        connectPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND);
        assertThat(connectPods.size(), Matchers.is(scaleReplicasTo));
        LOGGER.info("Scaling to {} finished", scaleReplicasTo);

        //scale down
        LOGGER.info("Scaling down to {}", initialReplicas);
        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECT_S2I_CLUSTER_NAME, cs2i ->
                cs2i.getSpec().setReplicas(initialReplicas));
        Map<String, String> depConfSnapshot = DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(deploymentConfigName, initialReplicas);

        connectPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND);
        assertThat(connectPods.size(), is(initialReplicas));

        LOGGER.info("Scaling to {} finished", initialReplicas);

        LOGGER.info("===== UPDATE BOOTSTRAP SERVER ADDRESS =====");

        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECT_S2I_CLUSTER_NAME, kafkaConnectS2I -> {
            kafkaConnectS2I.getSpec().setBootstrapServers(bootstrapAddress);
            kafkaConnectS2I.getSpec().setTls(new KafkaConnectTlsBuilder()
                    .addNewTrustedCertificate()
                        .withNewSecretName(SECOND_CLUSTER_NAME + "-cluster-ca-cert")
                        .withCertificate("ca.crt")
                    .withNewSecretName(SECOND_CLUSTER_NAME + "-cluster-ca-cert")
                    .withCertificate("ca.crt")
                    .endTrustedCertificate()
                    .build());
        });

        depConfSnapshot = DeploymentConfigUtils.waitTillDepConfigHasRolled(deploymentConfigName, depConfSnapshot);
        assertThat(KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CONNECT_S2I_CLUSTER_NAME).get().getSpec().getBootstrapServers(), is(bootstrapAddress));

        LOGGER.info("===== CONNECTS2I VERSION CHANGE =====");

        LOGGER.info("Setting version from {} to {}", TestKafkaVersion.getKafkaVersions().get(0).version(), TestKafkaVersion.getKafkaVersions().get(1).version());
        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECT_S2I_CLUSTER_NAME,
            kafkaConnectS2I -> kafkaConnectS2I.getSpec().setVersion(TestKafkaVersion.getKafkaVersions().get(1).version()));

        depConfSnapshot = DeploymentConfigUtils.waitTillDepConfigHasRolled(deploymentConfigName, depConfSnapshot);

        String versionCommand = "ls libs | grep -Po 'connect-api-\\K(\\d+.\\d+.\\d+)(?=.*jar)' | head -1";

        String actualVersion = cmdKubeClient().execInPodContainer(kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0),
                "", "/bin/bash", "-c", versionCommand).out().trim();

        assertThat(actualVersion, is(TestKafkaVersion.getKafkaVersions().get(1).version()));

        LOGGER.info("===== CONNECTS2I CERT CHANGE =====");
        InputStream secretInputStream = getClass().getClassLoader().getResourceAsStream("security-st-certs/cluster-ca.crt");
        String clusterCaCert = TestUtils.readResource(secretInputStream);
        SecretUtils.createSecret("my-secret", "ca.crt", new String(Base64.getEncoder().encode(clusterCaCert.getBytes()), StandardCharsets.US_ASCII));

        CertSecretSource certSecretSource = new CertSecretSourceBuilder()
            .withSecretName("my-secret")
            .withCertificate("ca.crt")
            .build();

        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECT_S2I_CLUSTER_NAME, kafkaConnectS2I -> {
            kafkaConnectS2I.getSpec().getTls().getTrustedCertificates().add(certSecretSource);
        });

        DeploymentConfigUtils.waitTillDepConfigHasRolled(deploymentConfigName, depConfSnapshot);

        List<CertSecretSource> trustedCertificates = KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE)
            .withName(CONNECT_S2I_CLUSTER_NAME).get().getSpec().getTls().getTrustedCertificates();

        assertThat(trustedCertificates.stream().anyMatch(cert -> cert.getSecretName().equals("my-secret")), is(true));
    }

    @Test
    @Tag(SCALABILITY)
    void testScaleConnectS2IWithoutConnectorToZero() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        KafkaConnectS2IResource.kafkaConnectS2I(CONNECT_S2I_CLUSTER_NAME, CLUSTER_NAME, 2).done();

        String deploymentConfigName = KafkaConnectS2IResources.deploymentName(CONNECT_S2I_CLUSTER_NAME);
        List<String> connectS2IPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND);

        assertThat(connectS2IPods.size(), Matchers.is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECT_S2I_CLUSTER_NAME, kafkaConnectS2I -> kafkaConnectS2I.getSpec().setReplicas(0));

        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(deploymentConfigName, 0);

        connectS2IPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND);
        KafkaConnectS2IStatus connectS2IStatus = KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CONNECT_S2I_CLUSTER_NAME).get().getStatus();

        assertThat(connectS2IPods.size(), Matchers.is(0));
        assertThat(connectS2IStatus.getConditions().get(0).getType(), is(Ready.toString()));
    }

    @Test
    @Tag(CONNECTOR_OPERATOR)
    @Tag(SCALABILITY)
    void testScaleConnectS2IWithConnectorToZero() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        KafkaConnectS2IResource.kafkaConnectS2I(CONNECT_S2I_CLUSTER_NAME, CLUSTER_NAME, 2)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .done();

        KafkaConnectorResource.kafkaConnector(CONNECT_S2I_CLUSTER_NAME)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("topics", TOPIC_NAME)
            .endSpec()
            .done();

        String deploymentConfigName = KafkaConnectS2IResources.deploymentName(CONNECT_S2I_CLUSTER_NAME);
        List<String> connectS2IPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND);

        assertThat(connectS2IPods.size(), Matchers.is(2));
        //scale down
        LOGGER.info("Scaling KafkaConnect down to zero");
        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECT_S2I_CLUSTER_NAME, kafkaConnectS2I -> kafkaConnectS2I.getSpec().setReplicas(0));

        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(deploymentConfigName, 0);

        connectS2IPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND);
        KafkaConnectS2IStatus connectS2IStatus = KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CONNECT_S2I_CLUSTER_NAME).get().getStatus();
        KafkaConnectorStatus connectorStatus = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CONNECT_S2I_CLUSTER_NAME).get().getStatus();

        assertThat(connectS2IPods.size(), Matchers.is(0));
        assertThat(connectS2IStatus.getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getType().equals(NotReady.toString())), is(true));
        assertThat(connectorStatus.getConditions().stream().anyMatch(condition -> condition.getMessage().contains("has 0 replicas")), is(true));
    }

    @Test
    @Tag(SCALABILITY)
    void testScaleConnectS2ISubresource() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        KafkaConnectS2IResource.kafkaConnectS2I(CLUSTER_NAME, CLUSTER_NAME, 1).done();

        int scaleTo = 4;
        long connectS2IObsGen = KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getObservedGeneration();
        String connectS2IGenName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getGenerateName();

        LOGGER.info("-------> Scaling KafkaConnectS2I subresource <-------");
        LOGGER.info("Scaling subresource replicas to {}", scaleTo);
        cmdKubeClient().scaleByName(KafkaConnectS2I.RESOURCE_KIND, CLUSTER_NAME, scaleTo);
        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(KafkaConnectS2IResources.deploymentName(CLUSTER_NAME), scaleTo);

        LOGGER.info("Check if replicas is set to {}, naming prefix should be same and observed generation higher", scaleTo);
        List<String> connectS2IPods = kubeClient().listPodNames(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND);
        assertThat(connectS2IPods.size(), is(4));
        assertThat(KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getReplicas(), is(4));
        assertThat(KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getReplicas(), is(4));
        /*
        observed generation should be higher than before scaling -> after change of spec and successful reconciliation,
        the observed generation is increased
        */
        assertThat(connectS2IObsGen < KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getObservedGeneration(), is(true));
        for (String pod : connectS2IPods) {
            assertThat(pod.contains(connectS2IGenName), is(true));
        }
    }

    @Test
    void testHostAliases() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        HostAlias hostAlias = new HostAliasBuilder()
            .withIp(aliasIp)
            .withHostnames(aliasHostname)
            .build();

        KafkaConnectS2IResource.kafkaConnectS2I(CLUSTER_NAME, CLUSTER_NAME, 1)
            .editSpec()
                .withNewTemplate()
                    .withNewPod()
                        .withHostAliases(hostAlias)
                    .endPod()
                .endTemplate()
            .endSpec()
            .done();

        String connectS2IPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getName();

        LOGGER.info("Checking the /etc/hosts file");
        String output = cmdKubeClient().execInPod(connectS2IPodName, "cat", "/etc/hosts").out();
        assertThat(output, containsString(etcHostsData));
    }

    @Test
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testMountingSecretAndConfigMapAsVolumesAndEnvVars() {
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

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        KafkaConnectS2IResource.kafkaConnectS2I(CLUSTER_NAME, CLUSTER_NAME, 1)
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
            .done();

        String connectS2IPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, KafkaConnectS2I.RESOURCE_KIND).get(0).getMetadata().getName();

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

    @Test
    void testConfigureDeploymentStrategy() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        KafkaConnectS2IResource.kafkaConnectS2I(CLUSTER_NAME, CLUSTER_NAME, 1)
            .editSpec()
                .editOrNewTemplate()
                    .editOrNewDeployment()
                        .withDeploymentStrategy(DeploymentStrategy.RECREATE)
                    .endDeployment()
                .endTemplate()
            .endSpec()
            .done();

        String connectS2IDepName = KafkaConnectS2IResources.deploymentName(CLUSTER_NAME);

        LOGGER.info("Adding label to ConnectS2I resource, the CR should be recreated");
        KafkaConnectS2IResource.replaceConnectS2IResource(CLUSTER_NAME,
            kcs2i -> kcs2i.getMetadata().setLabels(Collections.singletonMap("some", "label")));
        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(connectS2IDepName, 1);

        KafkaConnectS2I kafkaConnectS2I = KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get();

        LOGGER.info("Checking that observed gen. is still on 1 (recreation) and new label is present");
        assertThat(kafkaConnectS2I.getStatus().getObservedGeneration(), is(1L));
        assertThat(kafkaConnectS2I.getMetadata().getLabels().toString(), containsString("some=label"));
        assertThat(kafkaConnectS2I.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.RECREATE));

        LOGGER.info("Changing deployment strategy to {}", DeploymentStrategy.ROLLING_UPDATE);
        KafkaConnectS2IResource.replaceConnectS2IResource(CLUSTER_NAME,
            kcs2i -> kcs2i.getSpec().getTemplate().getDeployment().setDeploymentStrategy(DeploymentStrategy.ROLLING_UPDATE));
        KafkaConnectS2IUtils.waitForConnectS2IReady(CLUSTER_NAME);

        LOGGER.info("Adding another label to ConnectS2I resource, pods should be rolled");
        KafkaConnectS2IResource.replaceConnectS2IResource(CLUSTER_NAME, kcs2i -> kcs2i.getMetadata().getLabels().put("another", "label"));
        DeploymentConfigUtils.waitForDeploymentConfigAndPodsReady(connectS2IDepName, 1);

        LOGGER.info("Checking that observed gen. higher (rolling update) and label is changed");
        kafkaConnectS2I = KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get();
        assertThat(kafkaConnectS2I.getStatus().getObservedGeneration(), is(2L));
        assertThat(kafkaConnectS2I.getMetadata().getLabels().toString(), containsString("another=label"));
        assertThat(kafkaConnectS2I.getSpec().getTemplate().getDeployment().getDeploymentStrategy(), is(DeploymentStrategy.ROLLING_UPDATE));
    }

    private void deployConnectS2IWithMongoDb(String kafkaConnectS2IName, boolean useConnectorOperator) throws IOException {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        KafkaConnectS2IResource.kafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, Boolean.toString(useConnectorOperator))
            .endMetadata()
            .done();

        String depConfName = KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName);
        Map<String, String> connectSnapshot = DeploymentConfigUtils.depConfigSnapshot(depConfName);

        File dir = FileUtils.downloadAndUnzip("https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/0.7.5/debezium-connector-mongodb-0.7.5-plugin.zip");

        // Start a new image build using the plugins directory
        cmdKubeClient().execInCurrentNamespace("start-build", depConfName, "--from-dir", dir.getAbsolutePath());
        // Wait for rolling update connect pods
        DeploymentConfigUtils.waitTillDepConfigHasRolled(depConfName, connectSnapshot);

        LOGGER.info("Collect plugins information from connect s2i pod");
        String plugins = cmdKubeClient().execInPod(kafkaClientsPodName, "curl", "-X", "GET", "http://" + KafkaConnectS2IResources.serviceName(kafkaConnectS2IName) + ":8083/connector-plugins").out();

        assertThat(plugins, containsString("io.debezium.connector.mongodb.MongoDbConnector"));
    }

    private void checkConnectorInStatus(String namespace, String kafkaConnectS2IName) {
        KafkaConnectS2IStatus kafkaConnectS2IStatus = KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(namespace).withName(kafkaConnectS2IName).get().getStatus();
        List<ConnectorPlugin> pluginsList = kafkaConnectS2IStatus.getConnectorPlugins();
        assertThat(pluginsList, notNullValue());
        List<String> pluginsClasses = pluginsList.stream().map(p -> p.getConnectorClass()).collect(Collectors.toList());
        assertThat(pluginsClasses, hasItems("org.apache.kafka.connect.file.FileStreamSinkConnector",
                "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
                "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
                "org.apache.kafka.connect.mirror.MirrorSourceConnector",
                "io.debezium.connector.mongodb.MongoDbConnector"));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT, Constants.RECONCILIATION_INTERVAL);

        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();
        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME).get(0).getMetadata().getName();

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
