/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.REGRESSION;
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
import static io.strimzi.test.TestUtils.getFileAsString;
import static io.strimzi.test.TestUtils.getFileAsString;
import static io.strimzi.test.extensions.StrimziExtension.ACCEPTANCE;
import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@Tag(REGRESSION)
class ConnectST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConnectST.class);

    public static final String NAMESPACE = "connect-cluster-test";
    public static final String KAFKA_CLUSTER_NAME = "connect-tests";
    public static final String KAFKA_CONNECT_BOOTSTRAP_SERVERS = KafkaResources.plainBootstrapAddress(KAFKA_CLUSTER_NAME);
    private static final Map EXPECTED_CONFIG = loadProperties("group.id=connect-cluster\n" +
            "key.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "value.converter=org.apache.kafka.connect.json.JsonConverter\n" +
            "config.storage.topic=connect-cluster-configs\n" +
            "status.storage.topic=connect-cluster-status\n" +
            "offset.storage.topic=connect-cluster-offsets\n");

    @Test
    @Tag(REGRESSION)
    void testDeployUndeploy() {
        testMethodResources().kafkaConnect(KAFKA_CLUSTER_NAME, 1).done();
        LOGGER.info("Looks like the connect cluster my-cluster deployed OK");

        String podName = StUtils.getPodNameByPrefix(kafkaConnectName(KAFKA_CLUSTER_NAME));
        String kafkaPodJson = TestUtils.toJsonString(KUBE_CLIENT.getPod(podName));

        assertThat(kafkaPodJson, hasJsonPath(globalVariableJsonPathBuilder("KAFKA_CONNECT_BOOTSTRAP_SERVERS"),
                hasItem(KAFKA_CONNECT_BOOTSTRAP_SERVERS)));
        assertEquals(EXPECTED_CONFIG, getPropertiesFromJson(kafkaPodJson, "KAFKA_CONNECT_CONFIGURATION"));
        testDockerImagesForKafkaConnect();
    }

    @Test
    @Tag(ACCEPTANCE)
    void testKafkaConnectWithFileSinkPlugin() {
        testMethodResources().kafkaConnect(KAFKA_CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect")
            .endMetadata()
            .editSpec()
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
            .endSpec().done();
        testMethodResources().topic(KAFKA_CLUSTER_NAME, TEST_TOPIC_NAME).done();

        String connectorConfig = getFileAsString("../systemtest/src/test/resources/file/sink/connector.json");
        String kafkaConnectPodName = KUBE_CLIENT.listPods("type", "kafka-connect").get(0).getMetadata().getName();
        KUBE_CLIENT.execInPod(kafkaConnectPodName, kafkaConnectName(KAFKA_CLUSTER_NAME), "/bin/bash", "-c", "curl -X POST -H \"Content-Type: application/json\" --data "
                + "'" + connectorConfig + "'" + " http://localhost:8083/connectors");

        sendMessages(kafkaConnectPodName, KAFKA_CLUSTER_NAME, TEST_TOPIC_NAME, 2);

        TestUtils.waitFor("messages in file sink", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_SEND_RECEIVE_MSG,
            () -> KUBE_CLIENT.execInPod(kafkaConnectPodName, kafkaConnectName(KAFKA_CLUSTER_NAME), "/bin/bash", "-c", "cat /tmp/test-file-sink.txt").equals("0\n1\n"));
    }

    @Test
    @Tag(REGRESSION)
    void testJvmAndResources() {
        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        testMethodResources().kafkaConnect(KAFKA_CLUSTER_NAME, 1)
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
            .endSpec().done();

        String podName = StUtils.getPodNameByPrefix(kafkaConnectName(KAFKA_CLUSTER_NAME));
        assertResources(NAMESPACE, podName, "connect-tests-connect",
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(podName,
                "-Xmx200m", "-Xms200m", "-server", "-XX:+UseG1GC");
    }

    @Test
    @Tag(REGRESSION)
    void testKafkaConnectScaleUpScaleDown() {
        LOGGER.info("Running kafkaConnectScaleUP {} in namespace", NAMESPACE);
        testMethodResources().kafkaConnect(KAFKA_CLUSTER_NAME, 1).done();

        // kafka cluster Connect already deployed
        List<String> connectPods = KUBE_CLIENT.listPods("strimzi.io/kind", "KafkaConnect").stream()
                .map(pod -> pod.getMetadata().getName())
                .collect(Collectors.toList());
        int initialReplicas = connectPods.size();
        assertEquals(1, initialReplicas);
        final int scaleTo = initialReplicas + 1;

        LOGGER.info("Scaling up to {}", scaleTo);
        replaceKafkaConnectResource(KAFKA_CLUSTER_NAME, c -> c.getSpec().setReplicas(initialReplicas + 1));
        StUtils.waitForDeploymentReady(kafkaConnectName(KAFKA_CLUSTER_NAME));
        connectPods = KUBE_CLIENT.listPods("strimzi.io/kind", "KafkaConnect").stream()
                .map(pod -> pod.getMetadata().getName())
                .collect(Collectors.toList());
        assertEquals(scaleTo, connectPods.size());
        for (String pod : connectPods) {
            StUtils.waitForPod(pod);
            String uid = CLIENT.pods().inNamespace(NAMESPACE).withName(pod).get().getMetadata().getUid();
            List<Event> events = getEvents(uid);
            assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
            assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
        }

        LOGGER.info("Scaling down to {}", initialReplicas);
        replaceKafkaConnectResource(KAFKA_CLUSTER_NAME, c -> c.getSpec().setReplicas(initialReplicas));
        while (KUBE_CLIENT.listPods("strimzi.io/kind", "KafkaConnect").size() == scaleTo) {
            LOGGER.info("Waiting for connect pod deletion");
        }
        connectPods = KUBE_CLIENT.listPods("strimzi.io/kind", "KafkaConnect").stream()
                .map(pod -> pod.getMetadata().getName())
                .collect(Collectors.toList());
        assertEquals(initialReplicas, connectPods.size());
        for (String pod : connectPods) {
            String uid = CLIENT.pods().inNamespace(NAMESPACE).withName(pod).get().getMetadata().getUid();
            List<Event> events = getEvents(uid);
            assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
            assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
        }
    }

    @Test
    @Tag(REGRESSION)
    void testForUpdateValuesInConnectCM() {
        testMethodResources().kafkaConnect(KAFKA_CLUSTER_NAME, 1)
            .editSpec()
                .withNewReadinessProbe()
                    .withInitialDelaySeconds(15)
                    .withTimeoutSeconds(5)
                .endReadinessProbe()
                .withNewLivenessProbe()
                    .withInitialDelaySeconds(15)
                    .withTimeoutSeconds(5)
                .endLivenessProbe()
            .endSpec().done();

        List<String> connectPods = KUBE_CLIENT.listPods("strimzi.io/kind", "KafkaConnect").stream()
                .map(pod -> pod.getMetadata().getName())
                .collect(Collectors.toList());

        String connectConfig = "{\n" +
                "      \"config.storage.replication.factor\": \"1\",\n" +
                "      \"offset.storage.replication.factor\": \"1\",\n" +
                "      \"status.storage.replication.factor\": \"1\"\n" +
                "    }";
        replaceKafkaConnectResource(KAFKA_CLUSTER_NAME, c -> {
            c.getSpec().setBootstrapServers(KAFKA_CONNECT_BOOTSTRAP_SERVERS);
            c.getSpec().setConfig(TestUtils.fromJson(connectConfig, Map.class));
            c.getSpec().getLivenessProbe().setInitialDelaySeconds(61);
            c.getSpec().getReadinessProbe().setInitialDelaySeconds(61);
            c.getSpec().getLivenessProbe().setTimeoutSeconds(6);
            c.getSpec().getReadinessProbe().setTimeoutSeconds(6);
        });

        StUtils.waitForDeploymentReady(kafkaConnectName(KAFKA_CLUSTER_NAME));
        for (String connectPod : connectPods) {
            StUtils.waitForPodDeletion(connectPod);
        }
        LOGGER.info("Verify values after update");
        connectPods = KUBE_CLIENT.listPods("strimzi.io/kind", "KafkaConnect").stream()
                .map(pod -> pod.getMetadata().getName())
                .collect(Collectors.toList());
        for (String connectPod : connectPods) {
            String connectPodJson = TestUtils.toJsonString(KUBE_CLIENT.getPod(connectPod));
            assertThat(connectPodJson, hasJsonPath("$.spec.containers[*].readinessProbe.initialDelaySeconds", hasItem(61)));
            assertThat(connectPodJson, hasJsonPath("$.spec.containers[*].readinessProbe.timeoutSeconds", hasItem(6)));
            assertThat(connectPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(61)));
            assertThat(connectPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(6)));
            assertThat(connectPodJson, containsString("config.storage.replication.factor=1"));
            assertThat(connectPodJson, containsString("offset.storage.replication.factor=1"));
            assertThat(connectPodJson, containsString("status.storage.replication.factor=1"));
            assertThat(connectPodJson, hasJsonPath("$.spec.containers[*].env[?(@.name=='KAFKA_CONNECT_BOOTSTRAP_SERVERS')].value",
                    hasItem(KAFKA_CONNECT_BOOTSTRAP_SERVERS)));
        }
    }

    private void testDockerImagesForKafkaConnect() {
        LOGGER.info("Verifying docker image names");
        //Verifying docker image for cluster-operator

        Map<String, String> imgFromDeplConf = getImagesFromConfig();
        //Verifying docker image for kafka connect
        String connectImageName = getContainerImageNameFromPod(KUBE_CLIENT.listPods("strimzi.io/kind", "KafkaConnect").
                get(0).getMetadata().getName());

        String connectVersion = Crds.kafkaConnectOperation(KUBE_CLIENT.getClient()).inNamespace(NAMESPACE).withName(KAFKA_CLUSTER_NAME).get().getSpec().getVersion();
        if (connectVersion == null) {
            connectVersion = ENVIRONMENT.getStKafkaVersionEnv();
        }

        assertEquals(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_CONNECT_IMAGE_MAP)).get(connectVersion), connectImageName);
        LOGGER.info("Docker images verified");
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
        testClassResources.clusterOperator(NAMESPACE).done();
        deployTestSpecificResources();
    }

    void deployTestSpecificResources() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "3");
        kafkaConfig.put("transaction.state.log.replication.factor", "3");
        kafkaConfig.put("transaction.state.log.min.isr", "2");

        testClassResources.kafkaEphemeral(KAFKA_CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withConfig(kafkaConfig)
                .endKafka()
            .endSpec().done();
    }

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
        deployTestSpecificResources();
    }
}