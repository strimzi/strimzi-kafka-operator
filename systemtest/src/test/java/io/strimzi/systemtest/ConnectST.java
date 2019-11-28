/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.clients.lib.KafkaClient;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TRAVIS;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Failed;
import static io.strimzi.systemtest.k8s.Events.FailedSync;
import static io.strimzi.systemtest.k8s.Events.FailedValidation;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.k8s.Events.Unhealthy;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.systemtest.matchers.Matchers.hasNoneOfReasons;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
class ConnectST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConnectST.class);
    public static final String NAMESPACE = "connect-cluster-test";

    private static final String CONNECT_TOPIC_NAME = "connect-topic-example";

    @Test
    void testDeployUndeploy() {
        Map<String, Object> exceptedConfig = loadProperties("group.id=connect-cluster\n" +
                "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
                "config.storage.topic=connect-cluster-configs\n" +
                "status.storage.topic=connect-cluster-status\n" +
                "offset.storage.topic=connect-cluster-offsets\n");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1).done();
        LOGGER.info("Looks like the connect cluster my-cluster deployed OK");

        String podName = StUtils.getPodNameByPrefix(KafkaConnectResources.deploymentName(CLUSTER_NAME));
        String kafkaPodJson = TestUtils.toJsonString(kubeClient().getPod(podName));

        assertThat(kafkaPodJson, hasJsonPath(globalVariableJsonPathBuilder("KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))));
        assertThat(getPropertiesFromJson(kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"), is(exceptedConfig));
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
        String connectImageName = getContainerImageNameFromPod(kubeClient().listPods("strimzi.io/kind", "KafkaConnect").
                get(0).getMetadata().getName());

        String connectVersion = Crds.kafkaConnectOperation(kubeClient().getClient()).inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getVersion();
        if (connectVersion == null) {
            connectVersion = Environment.ST_KAFKA_VERSION;
        }

        assertThat(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_CONNECT_IMAGE_MAP)).get(connectVersion), is(connectImageName));
        LOGGER.info("Docker images verified");
    }

    @Test
    @Tag(ACCEPTANCE)
    @Tag(TRAVIS)
    @Tag(NODEPORT_SUPPORTED)
    void testKafkaConnectWithFileSinkPlugin() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect")
            .endMetadata()
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
            .endSpec().done();
        testMethodResources().topic(CLUSTER_NAME, CONNECT_TOPIC_NAME).done();

        String kafkaConnectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();

        StUtils.createFileSinkConnector(kafkaConnectPodName, CONNECT_TOPIC_NAME);

        waitForClusterAvailability(NAMESPACE, CLUSTER_NAME, CONNECT_TOPIC_NAME, 2);

        StUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName);

        LOGGER.info("Deleting topic {} from CR", CONNECT_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", CONNECT_TOPIC_NAME);
        StUtils.waitForKafkaTopicDeletion(CONNECT_TOPIC_NAME);
    }

    @Test
    @Tag(ACCEPTANCE)
    @Tag(TRAVIS)
    @Tag(NODEPORT_SUPPORTED)
    void testKafkaConnectAndConnectorFileSinkPlugin() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                .endSpec().done();
        testMethodResources().topic(CLUSTER_NAME, CONNECT_TOPIC_NAME).done();

        String connectorName = "license-source";
        String topicName = "my-topic";
        testMethodResources().kafkaConnector(new KafkaConnectorBuilder()
                .editOrNewMetadata()
                    .withName(connectorName)
                    .addToLabels("strimzi.io/cluster", CLUSTER_NAME)
                .endMetadata()
                .editOrNewSpec()
                    .withClassName("org.apache.kafka.connect.file.FileStreamSourceConnector")
                    .addToConfig("file", "/opt/kafka/LICENSE")
                    .addToConfig("topic", topicName)
                    .withTasksMax(2)
                .endSpec()
                .build()).done();

        // consume from the topic (we don't really care about the contents)
        try (KafkaClient testClient = new KafkaClient()) {
            int messageCount = 10;
            Future<Integer> consumer = testClient.receiveMessages(topicName, NAMESPACE, CLUSTER_NAME, messageCount);
            assertThat("Consumer consumed all messages", consumer.get(1, TimeUnit.MINUTES), greaterThanOrEqualTo(messageCount));
        }

        LOGGER.info("Deleting connector {} CR", connectorName);
        cmdKubeClient().deleteByName("kafkaconnector", connectorName);

        LOGGER.info("Deleting topic {} from CR", topicName);
        cmdKubeClient().deleteByName("kafkatopic", topicName);
        StUtils.waitForKafkaTopicDeletion(topicName);
    }


    @Test
    void testJvmAndResources() {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1)
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

        String podName = StUtils.getPodNameByPrefix(KafkaConnectResources.deploymentName(CLUSTER_NAME));
        assertResources(NAMESPACE, podName, KafkaConnectResources.deploymentName(CLUSTER_NAME),
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(podName, KafkaConnectResources.deploymentName(CLUSTER_NAME),
                "-Xmx200m", "-Xms200m", "-server", "-XX:+UseG1GC");
    }

    @Test
    void testKafkaConnectScaleUpScaleDown() {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        LOGGER.info("Running kafkaConnectScaleUP {} in namespace", NAMESPACE);
        testMethodResources().kafkaConnect(CLUSTER_NAME, 1).done();

        // kafka cluster Connect already deployed
        List<String> connectPods = kubeClient().listPodNames("strimzi.io/kind", "KafkaConnect");
        int initialReplicas = connectPods.size();
        assertThat(initialReplicas, is(1));
        final int scaleTo = initialReplicas + 1;

        LOGGER.info("Scaling up to {}", scaleTo);
        replaceKafkaConnectResource(CLUSTER_NAME, c -> c.getSpec().setReplicas(initialReplicas + 1));
        StUtils.waitForDeploymentReady(KafkaConnectResources.deploymentName(CLUSTER_NAME), initialReplicas + 1);
        connectPods = kubeClient().listPodNames("strimzi.io/kind", "KafkaConnect");
        assertThat(connectPods.size(), is(scaleTo));
        for (String pod : connectPods) {
            StUtils.waitForPod(pod);
            String uid = kubeClient().getPodUid(pod);
            List<Event> events = kubeClient().listEvents(uid);
            assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
            assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
        }

        LOGGER.info("Scaling down to {}", initialReplicas);
        replaceKafkaConnectResource(CLUSTER_NAME, c -> c.getSpec().setReplicas(initialReplicas));
        while (kubeClient().listPods("strimzi.io/kind", "KafkaConnect").size() == scaleTo) {
            LOGGER.info("Waiting for connect pod deletion");
        }
        connectPods = kubeClient().listPodNames("strimzi.io/kind", "KafkaConnect");
        assertThat(connectPods.size(), is(initialReplicas));
        for (String pod : connectPods) {
            String uid = kubeClient().getPodUid(pod);
            List<Event> events = kubeClient().listEvents(uid);
            assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
            assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
        }
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testSecretsWithKafkaConnectWithTlsAndTlsClientAuthentication() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewTls()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                            .endTls()
                            .withNewKafkaListenerExternalNodePort()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        final String userName = "user-example";

        testMethodResources().tlsUser(CLUSTER_NAME, userName).done();

        StUtils.waitForSecretReady(userName);

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(CLUSTER_NAME + "-cluster-ca-cert")
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(CLUSTER_NAME + "-kafka-bootstrap:9093")
                    .withNewKafkaClientAuthenticationTls()
                        .withNewCertificateAndKey()
                            .withSecretName("user-example")
                            .withCertificate("user.crt")
                            .withKey("user.key")
                        .endCertificateAndKey()
                    .endKafkaClientAuthenticationTls()
                .endSpec()
                .done();

        testMethodResources().topic(CLUSTER_NAME, CONNECT_TOPIC_NAME).done();

        String kafkaConnectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();
        String kafkaConnectLogs = kubeClient().logs(kafkaConnectPodName);

        LOGGER.info("Verifying that in kafka connect logs are everything fine");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector in pod {} with topic {}", kafkaConnectPodName, CONNECT_TOPIC_NAME);

        StUtils.createFileSinkConnector(kafkaConnectPodName, CONNECT_TOPIC_NAME);

        waitForClusterAvailabilityTls(userName, NAMESPACE, CLUSTER_NAME, CONNECT_TOPIC_NAME, 2);

        StUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName);

        assertThat(cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "cat /tmp/test-file-sink.txt").out(),
                containsString("0\n1\n"));

        LOGGER.info("Deleting topic {} from CR", CONNECT_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", CONNECT_TOPIC_NAME);
        StUtils.waitForKafkaTopicDeletion(CONNECT_TOPIC_NAME);
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testSecretsWithKafkaConnectWithTlsAndScramShaAuthentication() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endTls()
                        .withNewKafkaListenerExternalNodePort()
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        final String userName = "user-example-one";

        testMethodResources().scramShaUser(CLUSTER_NAME, userName).done();

        StUtils.waitForSecretReady(userName);

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(CLUSTER_NAME + "-cluster-ca-cert")
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(CLUSTER_NAME + "-kafka-bootstrap:9093")
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(userName)
                        .withNewPasswordSecret()
                            .withSecretName(userName)
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .done();

        testMethodResources().topic(CLUSTER_NAME, CONNECT_TOPIC_NAME).done();

        String kafkaConnectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();
        String kafkaConnectLogs = kubeClient().logs(kafkaConnectPodName);

        LOGGER.info("Verifying that in kafka connect logs are everything fine");
        assertThat(kafkaConnectLogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector in pod {} with topic {}", kafkaConnectPodName, CONNECT_TOPIC_NAME);
        StUtils.createFileSinkConnector(kafkaConnectPodName, CONNECT_TOPIC_NAME);

        waitForClusterAvailabilityScramSha(userName, NAMESPACE, CLUSTER_NAME, CONNECT_TOPIC_NAME, 2);

        StUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName);

        assertThat(cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "cat /tmp/test-file-sink.txt").out(),
                containsString("0\n1\n"));

        LOGGER.info("Deleting topic {} from CR", CONNECT_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", CONNECT_TOPIC_NAME);
        StUtils.waitForKafkaTopicDeletion(CONNECT_TOPIC_NAME);
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

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1)
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

        Map<String, String> connectSnapshot = StUtils.depSnapshot(KafkaConnectResources.deploymentName(CLUSTER_NAME));

        // Remove variable which is already in use
        envVarGeneral.remove(usedVariable);
        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), envVarGeneral);

        StUtils.checkCOlogForUsedVariable(usedVariable);

        LOGGER.info("Updating values in MirrorMaker container");
        replaceKafkaConnectResource(CLUSTER_NAME, kc -> {
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

        StUtils.waitTillDepHasRolled(KafkaConnectResources.deploymentName(CLUSTER_NAME), 1, connectSnapshot);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), envVarUpdated);
        checkComponentConfiguration(KafkaConnectResources.deploymentName(CLUSTER_NAME), KafkaConnectResources.deploymentName(CLUSTER_NAME), "KAFKA_CONNECT_CONFIGURATION", connectConfig);
    }

    @BeforeEach
    void createTestResources() {
        createTestMethodResources();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);
        createTestClassResources();

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
    }
}