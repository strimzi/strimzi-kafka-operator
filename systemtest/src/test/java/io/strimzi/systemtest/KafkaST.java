/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerPlain;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.OpenShiftOnly;
import io.strimzi.test.extensions.StrimziExtension;
import io.strimzi.test.k8s.Oc;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Failed;
import static io.strimzi.systemtest.k8s.Events.FailedSync;
import static io.strimzi.systemtest.k8s.Events.FailedValidation;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.k8s.Events.SuccessfulDelete;
import static io.strimzi.systemtest.k8s.Events.Unhealthy;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.systemtest.matchers.Matchers.hasNoneOfReasons;
import static io.strimzi.test.TestUtils.fromYamlString;
import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.waitFor;
import static io.strimzi.test.extensions.StrimziExtension.CCI_FLAKY;
import static io.strimzi.test.extensions.StrimziExtension.FLAKY;
import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@ExtendWith(StrimziExtension.class)
class KafkaST extends MessagingBaseST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);

    public static final String NAMESPACE = "kafka-cluster-test";
    private static final String TOPIC_NAME = "test-topic";
    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    @Test
    @Tag(FLAKY)
    @OpenShiftOnly
    void testDeployKafkaClusterViaTemplate() {
        createCustomResources("../examples/templates/cluster-operator");
        Oc oc = (Oc) KUBE_CLIENT;
        String clusterName = "openshift-my-cluster";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName));
        oc.waitForStatefulSet(zookeeperClusterName(clusterName), 3);
        oc.waitForStatefulSet(kafkaClusterName(clusterName), 3);
        oc.waitForDeployment(entityOperatorDeploymentName(clusterName), 1);

        //Testing docker images
        testDockerImagesForKafkaCluster(clusterName, 3, 3, false);

        LOGGER.info("Deleting Kafka cluster {} after test", clusterName);
        oc.deleteByName("Kafka", clusterName);

        // Delete all pods created by this test
        CLIENT.pods().inNamespace(NAMESPACE).list().getItems().stream()
                .filter(p -> p.getMetadata().getName().startsWith(clusterName))
                .forEach(p -> CLIENT.pods().inNamespace(NAMESPACE).delete(p));

        oc.waitForResourceDeletion("statefulset", kafkaClusterName(clusterName));
        oc.waitForResourceDeletion("statefulset", zookeeperClusterName(clusterName));
        oc.waitForResourceDeletion("deployment", entityOperatorDeploymentName(clusterName));

        CLIENT.pods().inNamespace(NAMESPACE).list().getItems().stream()
                .filter(p -> p.getMetadata().getName().startsWith(clusterName))
                .forEach(p -> waitForPodDeletion(NAMESPACE, p.getMetadata().getName()));
        deleteCustomResources("../examples/templates/cluster-operator");
    }

    @Test
    @Tag(REGRESSION)
    void testKafkaAndZookeeperScaleUpScaleDown() {
        operationID = startTimeMeasuring(Operation.SCALE_UP);
        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        testDockerImagesForKafkaCluster(CLUSTER_NAME, 3, 1, false);
        // kafka cluster already deployed
        LOGGER.info("Running kafkaScaleUpScaleDown {}", CLUSTER_NAME);
        //kubeClient.waitForStatefulSet(kafkaStatefulSetName(clusterName), 3);

        final int initialReplicas = CLIENT.apps().statefulSets().inNamespace(KUBE_CLIENT.namespace()).withName(kafkaClusterName(CLUSTER_NAME)).get().getStatus().getReplicas();
        assertEquals(3, initialReplicas);
        // scale up
        final int scaleTo = initialReplicas + 1;
        final int newPodId = initialReplicas;
        final int newBrokerId = newPodId;
        final String newPodName = kafkaPodName(CLUSTER_NAME,  newPodId);
        final String firstPodName = kafkaPodName(CLUSTER_NAME,  0);
        LOGGER.info("Scaling up to {}", scaleTo);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(initialReplicas + 1));
        KUBE_CLIENT.waitForStatefulSet(kafkaClusterName(CLUSTER_NAME), initialReplicas + 1);

        // Test that the new broker has joined the kafka cluster by checking it knows about all the other broker's API versions
        // (execute bash because we want the env vars expanded in the pod)
        String versions = getBrokerApiVersions(newPodName);
        for (int brokerId = 0; brokerId < scaleTo; brokerId++) {
            assertTrue(versions.indexOf("(id: " + brokerId + " rack: ") >= 0, versions);
        }

        //Test that the new pod does not have errors or failures in events
        List<Event> events = getEvents("Pod", newPodName);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(operationID);
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));

        // scale down
        LOGGER.info("Scaling down");
        operationID = startTimeMeasuring(Operation.SCALE_DOWN);
        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getKafka().setReplicas(initialReplicas);
        });
        KUBE_CLIENT.waitForStatefulSet(kafkaClusterName(CLUSTER_NAME), initialReplicas);

        final int finalReplicas = CLIENT.apps().statefulSets().inNamespace(KUBE_CLIENT.namespace()).withName(kafkaClusterName(CLUSTER_NAME)).get().getStatus().getReplicas();
        assertEquals(initialReplicas, finalReplicas);
        versions = getBrokerApiVersions(firstPodName);

        assertTrue(versions.indexOf("(id: " + newBrokerId + " rack: ") == -1,
                "Expect the added broker, " + newBrokerId + ",  to no longer be present in output of kafka-broker-api-versions.sh");

        //Test that the new broker has event 'Killing'
        assertThat(getEvents("Pod", newPodName), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        assertThat(getEvents("StatefulSet", kafkaClusterName(CLUSTER_NAME)), hasAllOfReasons(SuccessfulDelete));
        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(operationID);
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    @Tag(REGRESSION)
    void testEODeletion() {
        // Deploy kafka cluster with EO
        Kafka kafka = resources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Get pod name to check termination process
        Optional<Pod> pod = CLIENT.pods().inNamespace(KUBE_CLIENT.namespace()).list().getItems()
                .stream().filter(p -> p.getMetadata().getName().startsWith(entityOperatorDeploymentName(CLUSTER_NAME)))
                .findFirst();

        assertTrue(pod.isPresent(), "EO pod does not exist");

        // Remove EO from Kafka DTO
        kafka.getSpec().setEntityOperator(null);
        // Replace Kafka configuration with removed EO
        resources.kafka(kafka).done();

        // Wait when EO(UO + TO) will be removed
        KUBE_CLIENT.waitForResourceDeletion("deployment", entityOperatorDeploymentName(CLUSTER_NAME));
        KUBE_CLIENT.waitForResourceDeletion("pod", pod.get().getMetadata().getName());
    }

    @Test
    @Tag(REGRESSION)
    void testZookeeperScaleUpScaleDown() {
        operationID = startTimeMeasuring(Operation.SCALE_UP);
        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        // kafka cluster already deployed
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", CLUSTER_NAME);
        final int initialZkReplicas = CLIENT.apps().statefulSets().inNamespace(KUBE_CLIENT.namespace()).withName(zookeeperClusterName(CLUSTER_NAME)).get().getStatus().getReplicas();
        assertEquals(3, initialZkReplicas);

        final int scaleZkTo = initialZkReplicas + 4;
        final List<String> newZkPodNames = new ArrayList<String>() {{
                for (int i = initialZkReplicas; i < scaleZkTo; i++) {
                    add(zookeeperPodName(CLUSTER_NAME, i));
                }
            }};

        LOGGER.info("Scaling up to {}", scaleZkTo);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(scaleZkTo));

        waitForZkPods(newZkPodNames);
        // check the new node is either in leader or follower state
        waitForZkMntr(ZK_SERVER_STATE, 0, 1, 2, 3, 4, 5, 6);
        checkZkPodsLog(newZkPodNames);

        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(operationID);
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));

        // scale down
        LOGGER.info("Scaling down");
        operationID = startTimeMeasuring(Operation.SCALE_DOWN);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(initialZkReplicas));

        for (String name : newZkPodNames) {
            KUBE_CLIENT.waitForResourceDeletion("pod", name);
        }

        // Wait for one zk pods will became leader and others follower state
        waitForZkMntr(ZK_SERVER_STATE, 0, 1, 2);

        //Test that the second pod has event 'Killing'
        assertThat(getEvents("Pod", newZkPodNames.get(3)), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        assertThat(getEvents("StatefulSet", zookeeperClusterName(CLUSTER_NAME)), hasAllOfReasons(SuccessfulDelete));
        // Stop measuring
        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    @Tag(REGRESSION)
    void testCustomAndUpdatedValues() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("transaction.state.log.replication.factor", "1");
        kafkaConfig.put("default.replication.factor", "1");

        Map<String, Object> zookeeperConfig = new HashMap<>();
        zookeeperConfig.put("timeTick", "2000");
        zookeeperConfig.put("initLimit", "5");
        zookeeperConfig.put("syncLimit", "2");

        resources().kafkaEphemeral(CLUSTER_NAME, 2)
            .editSpec()
                .editKafka()
                    .withNewReadinessProbe()
                        .withInitialDelaySeconds(30)
                        .withTimeoutSeconds(10)
                    .endReadinessProbe()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(30)
                        .withTimeoutSeconds(10)
                    .endLivenessProbe()
                    .withConfig(kafkaConfig)
                .endKafka()
                .editZookeeper()
                    .withReplicas(2)
                    .withNewReadinessProbe()
                       .withInitialDelaySeconds(30)
                        .withTimeoutSeconds(10)
                    .endReadinessProbe()
                        .withNewLivenessProbe()
                        .withInitialDelaySeconds(30)
                        .withTimeoutSeconds(10)
                    .endLivenessProbe()
                    .withConfig(zookeeperConfig)
                .endZookeeper()
            .endSpec()
            .done();

        int expectedZKPods = 2;
        int expectedKafkaPods = 2;
        List<Date> zkPodStartTime = new ArrayList<>();
        for (int i = 0; i < expectedZKPods; i++) {
            zkPodStartTime.add(KUBE_CLIENT.getResourceCreateTimestamp("pod", zookeeperPodName(CLUSTER_NAME, i)));
        }
        List<Date> kafkaPodStartTime = new ArrayList<>();
        for (int i = 0; i < expectedKafkaPods; i++) {
            kafkaPodStartTime.add(KUBE_CLIENT.getResourceCreateTimestamp("pod", kafkaPodName(CLUSTER_NAME, i)));
        }

        LOGGER.info("Verify values before update");
        for (int i = 0; i < expectedKafkaPods; i++) {
            String kafkaPodJson = KUBE_CLIENT.getResourceAsJson("pod", kafkaPodName(CLUSTER_NAME, i));
            assertThat(kafkaPodJson, hasJsonPath(globalVariableJsonPathBuilder("KAFKA_CONFIGURATION"),
                    hasItem("default.replication.factor=1\noffsets.topic.replication.factor=1\ntransaction.state.log.replication.factor=1\n")));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(30)));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(10)));
        }
        LOGGER.info("Testing Zookeepers");
        for (int i = 0; i < expectedZKPods; i++) {
            String zkPodJson = KUBE_CLIENT.getResourceAsJson("pod", zookeeperPodName(CLUSTER_NAME, i));
            assertThat(zkPodJson, hasJsonPath(globalVariableJsonPathBuilder("ZOOKEEPER_CONFIGURATION"),
                    hasItem("autopurge.purgeInterval=1\ntimeTick=2000\ninitLimit=5\nsyncLimit=2\n")));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(30)));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(10)));
        }

        replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.getLivenessProbe().setInitialDelaySeconds(31);
            kafkaClusterSpec.getReadinessProbe().setInitialDelaySeconds(31);
            kafkaClusterSpec.getLivenessProbe().setTimeoutSeconds(11);
            kafkaClusterSpec.getReadinessProbe().setTimeoutSeconds(11);
            kafkaClusterSpec.setConfig(TestUtils.fromJson("{\"default.replication.factor\": 2,\"offsets.topic.replication.factor\": 2,\"transaction.state.log.replication.factor\": 2}", Map.class));
            ZookeeperClusterSpec zookeeperClusterSpec = k.getSpec().getZookeeper();
            zookeeperClusterSpec.getLivenessProbe().setInitialDelaySeconds(31);
            zookeeperClusterSpec.getReadinessProbe().setInitialDelaySeconds(31);
            zookeeperClusterSpec.getLivenessProbe().setTimeoutSeconds(11);
            zookeeperClusterSpec.getReadinessProbe().setTimeoutSeconds(11);
            zookeeperClusterSpec.setConfig(TestUtils.fromJson("{\"timeTick\": 2100, \"initLimit\": 6, \"syncLimit\": 3}", Map.class));
        });

        for (int i = 0; i < expectedZKPods; i++) {
            KUBE_CLIENT.waitForResourceUpdate("pod", zookeeperPodName(CLUSTER_NAME, i), zkPodStartTime.get(i));
            KUBE_CLIENT.waitForPod(zookeeperPodName(CLUSTER_NAME,  i));
        }
        for (int i = 0; i < expectedKafkaPods; i++) {
            KUBE_CLIENT.waitForResourceUpdate("pod", kafkaPodName(CLUSTER_NAME, i), kafkaPodStartTime.get(i));
            KUBE_CLIENT.waitForPod(kafkaPodName(CLUSTER_NAME,  i));
        }

        LOGGER.info("Verify values after update");
        for (int i = 0; i < expectedKafkaPods; i++) {
            String kafkaPodJson = KUBE_CLIENT.getResourceAsJson("pod", kafkaPodName(CLUSTER_NAME, i));
            assertThat(kafkaPodJson, hasJsonPath(globalVariableJsonPathBuilder("KAFKA_CONFIGURATION"),
                    hasItem("default.replication.factor=2\noffsets.topic.replication.factor=2\ntransaction.state.log.replication.factor=2\n")));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(31)));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(11)));
        }
        LOGGER.info("Testing Zookeepers");
        for (int i = 0; i < expectedZKPods; i++) {
            String zkPodJson = KUBE_CLIENT.getResourceAsJson("pod", zookeeperPodName(CLUSTER_NAME, i));
            assertThat(zkPodJson, hasJsonPath(globalVariableJsonPathBuilder("ZOOKEEPER_CONFIGURATION"),
                    hasItem("autopurge.purgeInterval=1\ntimeTick=2100\ninitLimit=6\nsyncLimit=3\n")));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(31)));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(11)));
        }
    }

    /**
     * Test sending messages over plain transport, without auth
     */
    @Test
    @Tag(REGRESSION)
    void testSendMessagesPlainAnonymous() throws Exception {
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        resources().topic(CLUSTER_NAME, topicName).done();

        resources().deployKafkaClients(CLUSTER_NAME).done();

        availabilityTest(messagesCount, TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, false, topicName, null);
    }

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @Test
    @Tag(REGRESSION)
    void testSendMessagesTlsAuthenticated() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Use a Kafka with plain listener disabled
        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withTls(listenerTls)
                            .withNewTls()
                            .endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        resources().topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = resources().tlsUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);

        resources().deployKafkaClients(true, CLUSTER_NAME, user).done();
        availabilityTest(messagesCount, TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, true, topicName, user);
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @Test
    @Tag(REGRESSION)
    void testSendMessagesPlainScramSha() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaListenerAuthenticationScramSha512 auth = new KafkaListenerAuthenticationScramSha512();
        KafkaListenerPlain listenerTls = new KafkaListenerPlain();
        listenerTls.setAuthentication(auth);

        // Use a Kafka with plain listener disabled
        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withPlain(listenerTls)
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        resources().topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = resources().scramShaUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);

        String brokerPodLog = podLog(CLUSTER_NAME + "-kafka-0", "kafka");
        Pattern p = Pattern.compile("^.*" + Pattern.quote(kafkaUser) + ".*$", Pattern.MULTILINE);
        Matcher m = p.matcher(brokerPodLog);
        boolean found = false;
        while (m.find()) {
            found = true;
            LOGGER.info("Broker pod log line about user {}: {}", kafkaUser, m.group());
        }
        if (!found) {
            LOGGER.warn("No broker pod log lines about user {}", kafkaUser);
            LOGGER.info("Broker pod log:\n----\n{}\n----\n", brokerPodLog);
        }

        resources().deployKafkaClients(false, CLUSTER_NAME, user).done();
        availabilityTest(messagesCount, TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, false, topicName, user);
    }

    /**
     * Test sending messages over tls transport using scram sha auth
     */
    @Test
    @Tag(REGRESSION)
    void testSendMessagesTlsScramSha() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(new KafkaListenerAuthenticationScramSha512());

        // Use a Kafka with plain listener disabled
        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        resources().topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = resources().scramShaUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);

        resources().deployKafkaClients(true, CLUSTER_NAME, user).done();
        availabilityTest(messagesCount, 180000, CLUSTER_NAME, true, topicName, user);
    }

    @Test
    @Tag(REGRESSION)
    void testJvmAndResources() {
        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resources().kafkaEphemeral(CLUSTER_NAME, 1)
            .editSpec()
                .editKafka()
                    .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1.5Gi"))
                            .addToLimits("cpu", new Quantity("1"))
                            .addToRequests("memory", new Quantity("1Gi"))
                            .addToRequests("cpu", new Quantity("500m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1g")
                        .withXms("512m")
                        .withServer(true)
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1G"))
                            .addToLimits("cpu", new Quantity("0.5"))
                            .addToRequests("memory", new Quantity("0.5G"))
                            .addToRequests("cpu", new Quantity("250m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1G")
                        .withXms("512M")
                        .withServer(true)
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endZookeeper()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                        .withResources(new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("1024Mi"))
                                .addToLimits("cpu", new Quantity("500m"))
                                .addToRequests("memory", new Quantity("512Mi"))
                                .addToRequests("cpu", new Quantity("0.25"))
                                .build())
                    .endTopicOperator()
                    .withNewUserOperator()
                        .withResources(new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("512M"))
                                .addToLimits("cpu", new Quantity("300m"))
                                .addToRequests("memory", new Quantity("256M"))
                                .addToRequests("cpu", new Quantity("300m"))
                                .build())
                    .endUserOperator()
                .endEntityOperator()
            .endSpec().done();

        operationID = startTimeMeasuring(Operation.NEXT_RECONCILIATION);

        // Make snapshots for Kafka cluster to meke sure that there is no rolling update after CO reconciliation
        String zkSsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        String kafkaSsName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);
        Map<String, String> zkPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, zkSsName);
        Map<String, String> kafkaPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, kafkaSsName);
        Map<String, String> eoPods = StUtils.depSnapshot(CLIENT, NAMESPACE, eoDepName);

        assertResources(KUBE_CLIENT.namespace(), kafkaPodName(CLUSTER_NAME, 0), "kafka",
                "1536Mi", "1", "1Gi", "500m");
        assertExpectedJavaOpts(kafkaPodName(CLUSTER_NAME, 0),
                "-Xmx1g", "-Xms512m", "-server", "-XX:+UseG1GC");

        assertResources(KUBE_CLIENT.namespace(), zookeeperPodName(CLUSTER_NAME, 0), "zookeeper",
                "1G", "500m", "500M", "250m");
        assertExpectedJavaOpts(zookeeperPodName(CLUSTER_NAME, 0),
                "-Xmx1G", "-Xms512M", "-server", "-XX:+UseG1GC");

        Optional<Pod> pod = CLIENT.pods().inNamespace(KUBE_CLIENT.namespace()).list().getItems()
                .stream().filter(p -> p.getMetadata().getName().startsWith(entityOperatorDeploymentName(CLUSTER_NAME)))
                .findFirst();
        assertTrue(pod.isPresent(), "EO pod does not exist");

        assertResources(KUBE_CLIENT.namespace(), pod.get().getMetadata().getName(), "topic-operator",
                "1Gi", "500m", "512Mi", "250m");
        assertResources(KUBE_CLIENT.namespace(), pod.get().getMetadata().getName(), "user-operator",
                "512M", "300m", "256M", "300m");

        TestUtils.waitFor("Wait till reconciliation timeout", GLOBAL_POLL_INTERVAL, GLOBAL_TIMEOUT,
            () -> !KUBE_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, operationID), "\"Assembly reconciled\"").isEmpty());

        // Checking no rolling update after last CO reconciliation
        LOGGER.info("Checking no rolling update for Kafka cluster");
        assertFalse(StUtils.ssHasRolled(CLIENT, NAMESPACE, zkSsName, zkPods));
        assertFalse(StUtils.ssHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods));
        assertFalse(StUtils.depHasRolled(CLIENT, NAMESPACE, eoDepName, eoPods));
        TimeMeasuringSystem.stopOperation(operationID);
    }

    @Test
    @Tag(REGRESSION)
    void testForTopicOperator() throws InterruptedException {
        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        //Creating topics for testing
        KUBE_CLIENT.create(TOPIC_CM);
        TestUtils.waitFor("wait for 'my-topic' to be created in Kafka", GLOBAL_POLL_INTERVAL, TIMEOUT_FOR_TOPIC_CREATION, () -> {
            List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
            return topics.contains("my-topic");
        });

        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), hasItem("my-topic"));

        createTopicUsingPodCLI(CLUSTER_NAME, 0, "topic-from-cli", 1, 1);
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), hasItems("my-topic", "topic-from-cli"));
        assertThat(KUBE_CLIENT.list("kafkatopic"), hasItems("my-topic", "topic-from-cli", "my-topic"));

        //Updating first topic using pod CLI
        updateTopicPartitionsCountUsingPodCLI(CLUSTER_NAME, 0, "my-topic", 2);
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, "my-topic"),
                hasItems("PartitionCount:2"));
        KafkaTopic testTopic = fromYamlString(KUBE_CLIENT.get("kafkatopic", "my-topic"), KafkaTopic.class);
        assertNotNull(testTopic);
        assertNotNull(testTopic.getSpec());
        assertEquals(Integer.valueOf(2), testTopic.getSpec().getPartitions());

        //Updating second topic via KafkaTopic update
        replaceTopicResource("topic-from-cli", topic -> {
            topic.getSpec().setPartitions(2);
        });
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, "topic-from-cli"),
                hasItems("PartitionCount:2"));
        testTopic = fromYamlString(KUBE_CLIENT.get("kafkatopic", "topic-from-cli"), KafkaTopic.class);
        assertNotNull(testTopic);
        assertNotNull(testTopic.getSpec());
        assertEquals(Integer.valueOf(2), testTopic.getSpec().getPartitions());

        //Deleting first topic by deletion of CM
        KUBE_CLIENT.deleteByName("kafkatopic", "topic-from-cli");

        //Deleting another topic using pod CLI
        deleteTopicUsingPodCLI(CLUSTER_NAME, 0, "my-topic");
        KUBE_CLIENT.waitForResourceDeletion("kafkatopic", "my-topic");

        //Checking all topics were deleted
        Thread.sleep(TIMEOUT_TEARDOWN);
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems("my-topic")));
        assertThat(topics, not(hasItems("topic-from-cli")));
    }

    @Test
    @Tag(REGRESSION)
    void testTopicWithoutLabels() {
        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Creating topic without any label
        resources().topic(CLUSTER_NAME, "topic-without-labels")
            .editMetadata()
                .withLabels(null)
            .endMetadata()
            .done();

        // Checking that resource was created
        assertThat(KUBE_CLIENT.list("kafkatopic"), hasItems("topic-without-labels"));
        // Checking that TO didn't handl new topic and zk pods don't contain new topic
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), not(hasItems("topic-without-labels")));

        // Checking TO logs
        String tOPodName = KUBE_CLIENT.listResourcesByLabel("pod", "strimzi.io/name=my-cluster-entity-operator").get(0);
        String tOlogs = KUBE_CLIENT.logs(tOPodName, "topic-operator");
        assertThat(tOlogs, not(containsString("Created topic 'topic-without-labels'")));

        //Deleting topic
        KUBE_CLIENT.deleteByName("kafkatopic", "topic-without-labels");
        KUBE_CLIENT.waitForResourceDeletion("kafkatopic", "topic-without-labels");

        //Checking all topics were deleted
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems("topic-without-labels")));
    }

    private void testDockerImagesForKafkaCluster(String clusterName, int kafkaPods, int zkPods, boolean rackAwareEnabled) {
        LOGGER.info("Verifying docker image names");
        //Verifying docker image for cluster-operator

        Map<String, String> imgFromDeplConf = getImagesFromConfig();

        //Verifying docker image for zookeeper pods
        for (int i = 0; i < zkPods; i++) {
            String imgFromPod = getContainerImageNameFromPod(zookeeperPodName(clusterName, i), "zookeeper");
            assertEquals(imgFromDeplConf.get(ZK_IMAGE), imgFromPod);
            imgFromPod = getContainerImageNameFromPod(zookeeperPodName(clusterName, i), "tls-sidecar");
            assertEquals(imgFromDeplConf.get(TLS_SIDECAR_ZOOKEEPER_IMAGE), imgFromPod);
        }

        //Verifying docker image for kafka pods
        for (int i = 0; i < kafkaPods; i++) {
            String imgFromPod = getContainerImageNameFromPod(kafkaPodName(clusterName, i), "kafka");
            String kafkaVersion = Crds.kafkaOperation(CLIENT).inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getKafka().getVersion();
            if (kafkaVersion == null) {
                kafkaVersion = ENVIRONMENT.getStKafkaVersionEnv();
            }
            assertEquals(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_IMAGE_MAP)).get(kafkaVersion), imgFromPod);
            imgFromPod = getContainerImageNameFromPod(kafkaPodName(clusterName, i), "tls-sidecar");
            assertEquals(imgFromDeplConf.get(TLS_SIDECAR_KAFKA_IMAGE), imgFromPod);
            if (rackAwareEnabled) {
                String initContainerImage = getInitContainerImageName(kafkaPodName(clusterName, i));
                assertEquals(imgFromDeplConf.get(KAFKA_INIT_IMAGE), initContainerImage);
            }
        }

        //Verifying docker image for entity-operator
        String entityOperatorPodName = KUBE_CLIENT.listResourcesByLabel("pod",
                "strimzi.io/name=" + clusterName + "-entity-operator").get(0);
        String imgFromPod = getContainerImageNameFromPod(entityOperatorPodName, "topic-operator");
        assertEquals(imgFromDeplConf.get(TO_IMAGE), imgFromPod);
        imgFromPod = getContainerImageNameFromPod(entityOperatorPodName, "user-operator");
        assertEquals(imgFromDeplConf.get(UO_IMAGE), imgFromPod);
        imgFromPod = getContainerImageNameFromPod(entityOperatorPodName, "tls-sidecar");
        assertEquals(imgFromDeplConf.get(TLS_SIDECAR_EO_IMAGE), imgFromPod);

        LOGGER.info("Docker images verified");
    }

    @Test
    @Tag(REGRESSION)
    void testRackAware() {
        resources().kafkaEphemeral(CLUSTER_NAME, 1)
            .editSpec()
                .editKafka()
                    .withNewRack()
                        .withTopologyKey("rack-key")
                    .endRack()
                .endKafka()
            .endSpec().done();

        testDockerImagesForKafkaCluster(CLUSTER_NAME, 1, 1, true);

        String kafkaPodName = kafkaPodName(CLUSTER_NAME, 0);
        KUBE_CLIENT.waitForPod(kafkaPodName);

        String rackId = KUBE_CLIENT.execInPodContainer(kafkaPodName, "kafka", "/bin/bash", "-c", "cat /opt/kafka/init/rack.id").out();
        assertEquals("zone", rackId);

        String brokerRack = KUBE_CLIENT.execInPodContainer(kafkaPodName, "kafka", "/bin/bash", "-c", "cat /tmp/strimzi.properties | grep broker.rack").out();
        assertTrue(brokerRack.contains("broker.rack=zone"));

        List<Event> events = getEvents("Pod", kafkaPodName);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
    }

    @Test
    @Tag(CCI_FLAKY)
    void testMirrorMaker() throws Exception {
        operationID = startTimeMeasuring(Operation.MM_DEPLOYMENT);
        String topicSourceName = TOPIC_NAME + "-source" + "-" + rng.nextInt(Integer.MAX_VALUE);
        String kafkaSourceName = CLUSTER_NAME + "-source";
        String kafkaTargetName = CLUSTER_NAME + "-target";
        int messagesCount = 200;

        // Deploy source kafka
        resources().kafkaEphemeral(kafkaSourceName, 1, 1).done();
        // Deploy target kafka
        resources().kafkaEphemeral(kafkaTargetName, 1, 1).done();
        // Deploy Topic
        resources().topic(kafkaSourceName, topicSourceName).done();

        resources().deployKafkaClients(CLUSTER_NAME).done();

        // Check brokers availability
        availabilityTest(messagesCount, TIMEOUT_AVAILABILITY_TEST, kafkaSourceName);
        availabilityTest(messagesCount, TIMEOUT_AVAILABILITY_TEST, kafkaTargetName);

        // Deploy Mirror Maker
        resources().kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, false).done();

        TimeMeasuringSystem.stopOperation(operationID);
        // Wait when Mirror Maker will join group
        waitFor("Mirror Maker will join group", GLOBAL_POLL_INTERVAL, TIMEOUT_FOR_MIRROR_JOIN_TO_GROUP, () ->
            !KUBE_CLIENT.searchInLog("deploy", "my-cluster-mirror-maker", TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID),  "\"Successfully joined group\"").isEmpty()
        );

        int sent = sendMessages(messagesCount, TIMEOUT_SEND_MESSAGES, kafkaSourceName, false, topicSourceName, null);
        int receivedSource = receiveMessages(messagesCount, TIMEOUT_RECV_MESSAGES, kafkaSourceName, false, topicSourceName, null);
        int receivedTarget = receiveMessages(messagesCount, TIMEOUT_RECV_MESSAGES, kafkaTargetName, false, topicSourceName, null);

        assertSentAndReceivedMessages(sent, receivedSource);
        assertSentAndReceivedMessages(sent, receivedTarget);
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using mutual tls auth
     */
    @Test
    @Tag(CCI_FLAKY)
    void testMirrorMakerTlsAuthenticated() throws Exception {
        operationID = startTimeMeasuring(Operation.MM_DEPLOYMENT);
        String topicSourceName = TOPIC_NAME + "-source" + "-" + rng.nextInt(Integer.MAX_VALUE);
        String kafkaSourceUserName = "my-user-source";
        String kafkaUserTargetName = "my-user-target";
        String kafkaClusterSourceName = CLUSTER_NAME + "-source";
        String kafkaClusterTargetName = CLUSTER_NAME + "-target";
        int messagesCount = 200;

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Deploy source kafka with tls listener and mutual tls auth
        resources().kafka(resources().defaultKafka(kafkaClusterSourceName, 1, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withTls(listenerTls)
                            .withNewTls()
                            .endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();

        // Deploy target kafka with tls listener and mutual tls auth
        resources().kafka(resources().defaultKafka(kafkaClusterTargetName, 1, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withTls(listenerTls)
                            .withNewTls()
                            .endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();

        // Deploy topic
        resources().topic(kafkaClusterSourceName, topicSourceName).done();

        // Create Kafka user
        KafkaUser userSource = resources().tlsUser(kafkaClusterSourceName, kafkaSourceUserName).done();
        waitTillSecretExists(kafkaSourceUserName);

        KafkaUser userTarget = resources().tlsUser(kafkaClusterTargetName, kafkaUserTargetName).done();
        waitTillSecretExists(kafkaUserTargetName);

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(clusterCaCertSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(clusterCaCertSecretName(kafkaClusterTargetName));

        resources().deployKafkaClients(true, CLUSTER_NAME, userSource, userTarget).done();

        // Check brokers availability
        availabilityTest(messagesCount, TIMEOUT_AVAILABILITY_TEST, kafkaClusterSourceName, true, "my-topic-test-1", userSource);
        availabilityTest(messagesCount, TIMEOUT_AVAILABILITY_TEST, kafkaClusterTargetName, true, "my-topic-test-2", userTarget);

        // Deploy Mirror Maker with tls listener and mutual tls auth
        resources().kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName, "my-group", 1, true)
                .editSpec()
                .editConsumer()
                    .withNewTls()
                        .withTrustedCertificates(certSecretSource)
                    .endTls()
                .endConsumer()
                .editProducer()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endProducer()
                .endSpec()
                .done();

        TimeMeasuringSystem.stopOperation(operationID);
        // Wait when Mirror Maker will join the group
        waitFor("Mirror Maker will join group", GLOBAL_POLL_INTERVAL, TIMEOUT_FOR_MIRROR_JOIN_TO_GROUP, () ->
            !KUBE_CLIENT.searchInLog("deploy", CLUSTER_NAME + "-mirror-maker", TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID),  "\"Successfully joined group\"").isEmpty()
        );

        int sent = sendMessages(messagesCount, TIMEOUT_SEND_MESSAGES, kafkaClusterSourceName, true, topicSourceName, userSource);
        int receivedSource = receiveMessages(messagesCount, TIMEOUT_RECV_MESSAGES, kafkaClusterSourceName, true, topicSourceName, userSource);
        int receivedTarget = receiveMessages(messagesCount, TIMEOUT_RECV_MESSAGES, kafkaClusterTargetName, true, topicSourceName, userTarget);

        assertSentAndReceivedMessages(sent, receivedSource);
        assertSentAndReceivedMessages(sent, receivedTarget);
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using scram-sha auth
     */
    @Test
    @Tag(CCI_FLAKY)
    void testMirrorMakerTlsScramSha() throws Exception {
        operationID = startTimeMeasuring(Operation.MM_DEPLOYMENT);
        String kafkaUserSource = "my-user-source";
        String kafkaUserTarget = "my-user-target";
        String kafkaSourceName = CLUSTER_NAME + "-source";
        String kafkaTargetName = CLUSTER_NAME + "-target";
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        int messagesCount = 200;

        // Deploy source kafka with tls listener and SCRAM-SHA authentication
        resources().kafka(resources().defaultKafka(kafkaSourceName, 1, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();

        // Deploy target kafka with tls listener and SCRAM-SHA authentication
        resources().kafka(resources().defaultKafka(kafkaTargetName, 1, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                          .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();

        // Create Kafka user for source cluster
        KafkaUser userSource = resources().scramShaUser(kafkaSourceName, kafkaUserSource).done();
        waitTillSecretExists(kafkaUserSource);

        // Create Kafka user for target cluster
        KafkaUser userTarget = resources().scramShaUser(kafkaTargetName, kafkaUserTarget).done();
        waitTillSecretExists(kafkaUserTarget);

        // Initialize PasswordSecretSource to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(kafkaUserSource);
        passwordSecretSource.setPassword("password");

        // Initialize PasswordSecretSource to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(kafkaUserTarget);
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(clusterCaCertSecretName(kafkaSourceName));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(clusterCaCertSecretName(kafkaTargetName));

        // Deploy client
        resources().deployKafkaClients(true, CLUSTER_NAME, userSource, userTarget).done();

        // Check brokers availability
        availabilityTest(messagesCount, TIMEOUT_AVAILABILITY_TEST, kafkaSourceName, true, "my-topic-test-1", userSource);
        availabilityTest(messagesCount, TIMEOUT_AVAILABILITY_TEST, kafkaTargetName, true, "my-topic-test-2", userTarget);

        // Deploy Mirror Maker with TLS and ScramSha512
        resources().kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, true)
                .editSpec()
                    .editConsumer()
                        .withNewKafkaMirrorMakerAuthenticationScramSha512()
                            .withUsername(kafkaUserSource)
                            .withPasswordSecret(passwordSecretSource)
                        .endKafkaMirrorMakerAuthenticationScramSha512()
                        .withNewTls()
                            .withTrustedCertificates(certSecretSource)
                        .endTls()
                    .endConsumer()
                .editProducer()
                    .withNewKafkaMirrorMakerAuthenticationScramSha512()
                        .withUsername(kafkaUserTarget)
                        .withPasswordSecret(passwordSecretTarget)
                    .endKafkaMirrorMakerAuthenticationScramSha512()
                    .withNewTls()
                        .withTrustedCertificates(certSecretTarget)
                    .endTls()
                .endProducer()
                .endSpec().done();

        // Deploy topic
        resources().topic(kafkaSourceName, topicName).done();

        TimeMeasuringSystem.stopOperation(operationID);
        // Wait when Mirror Maker will join group
        waitFor("Mirror Maker will join group", GLOBAL_POLL_INTERVAL, TIMEOUT_FOR_MIRROR_JOIN_TO_GROUP, () ->
            !KUBE_CLIENT.searchInLog("deploy", CLUSTER_NAME + "-mirror-maker", TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID),  "\"Successfully joined group\"").isEmpty()
        );

        int sent = sendMessages(messagesCount, TIMEOUT_SEND_MESSAGES, kafkaSourceName, true, topicName, userSource);
        int receivedSource = receiveMessages(messagesCount, TIMEOUT_RECV_MESSAGES, kafkaSourceName, true, topicName, userSource);
        int receivedTarget = receiveMessages(messagesCount, TIMEOUT_RECV_MESSAGES, kafkaTargetName, true, topicName, userTarget);

        assertSentAndReceivedMessages(sent, receivedSource);
        assertSentAndReceivedMessages(sent, receivedTarget);
    }

    @Test
    @Tag(REGRESSION)
    void testManualTriggeringRollingUpdate() {
        String coPodName = KUBE_CLIENT.listResourcesByLabel("pod", "name=strimzi-cluster-operator").get(0);
        resources().kafkaEphemeral(CLUSTER_NAME, 1).done();

        // rolling update for kafka
        operationID = startTimeMeasuring(Operation.ROLLING_UPDATE);
        // set annotation to trigger Kafka rolling update
        CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaClusterName(CLUSTER_NAME)).cascading(false).edit()
                .editMetadata()
                    .addToAnnotations("strimzi.io/manual-rolling-update", "true")
                .endMetadata().done();

        // check annotation to trigger rolling update
        assertTrue(Boolean.parseBoolean(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaClusterName(CLUSTER_NAME))
                .get().getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")));

        // wait when annotation will be removed
        waitFor("CO removes rolling update annotation", WAIT_FOR_ROLLING_UPDATE_INTERVAL, WAIT_FOR_ROLLING_UPDATE_TIMEOUT,
            () -> getAnnotationsForSS(NAMESPACE, kafkaClusterName(CLUSTER_NAME)) == null
                || !getAnnotationsForSS(NAMESPACE, kafkaClusterName(CLUSTER_NAME)).containsKey("strimzi.io/manual-rolling-update"));

        // check rolling update messages in CO log
        String coLog = KUBE_CLIENT.logs(coPodName);
        assertThat(coLog, containsString("Rolling Kafka pod " + kafkaClusterName(CLUSTER_NAME) + "-0" + " due to manual rolling update"));

        // rolling update for zookeeper
        operationID = startTimeMeasuring(Operation.ROLLING_UPDATE);
        // set annotation to trigger Zookeeper rolling update
        CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zookeeperClusterName(CLUSTER_NAME)).cascading(false).edit()
                .editMetadata()
                    .addToAnnotations("strimzi.io/manual-rolling-update", "true")
                .endMetadata().done();

        // check annotation to trigger rolling update
        assertTrue(Boolean.parseBoolean(CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zookeeperClusterName(CLUSTER_NAME))
                .get().getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")));

        // wait when annotation will be removed
        waitFor("CO removes rolling update annotation", WAIT_FOR_ROLLING_UPDATE_INTERVAL, WAIT_FOR_ROLLING_UPDATE_TIMEOUT,
            () -> getAnnotationsForSS(NAMESPACE, zookeeperClusterName(CLUSTER_NAME)) == null
                || !getAnnotationsForSS(NAMESPACE, zookeeperClusterName(CLUSTER_NAME)).containsKey("strimzi.io/manual-rolling-update"));

        // check rolling update messages in CO log
        coLog = KUBE_CLIENT.logs(coPodName);
        assertThat(coLog, containsString("Rolling Zookeeper pod " + zookeeperClusterName(CLUSTER_NAME) + "-0" + " to manual rolling update"));
    }

    @Test
    @Tag(REGRESSION)
    void testNodePort() throws Exception {
        resources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
                .editZookeeper()
                    .withReplicas(1)
                .endZookeeper()
            .endSpec()
            .done();

        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    @Tag(REGRESSION)
    void testNodePortTls() throws Exception {
        resources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                    .withNewKafkaListenerExternalNodePort()
                    .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
                .editZookeeper()
                    .withReplicas(1)
                .endZookeeper()
            .endSpec()
            .done();

        String userName = "alice";
        resources().tlsUser(CLUSTER_NAME, userName).done();
        waitFor("Wait for secrets became available", GLOBAL_POLL_INTERVAL, TIMEOUT_FOR_GET_SECRETS,
            () -> CLIENT.secrets().inNamespace(NAMESPACE).withName("alice").get() != null,
            () -> LOGGER.error("Couldn't find user secret {}", CLIENT.secrets().inNamespace(NAMESPACE).list().getItems()));

        waitForClusterAvailabilityTls(userName, NAMESPACE);
    }

    private Map<String, String> getAnnotationsForSS(String namespace, String ssName) {
        return CLIENT.apps().statefulSets().inNamespace(namespace).withName(ssName).get().getMetadata().getAnnotations();
    }

    void waitForZkRollUp() {
        LOGGER.info("Waiting for cluster stability");
        Map<String, String>[] zkPods = new Map[1];
        AtomicInteger count = new AtomicInteger();
        zkPods[0] = StUtils.ssSnapshot(CLIENT, NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME));
        TestUtils.waitFor("Cluster stable and ready", GLOBAL_POLL_INTERVAL, TIMEOUT_FOR_ZK_CLUSTER_STABILIZATION, () -> {
            Map<String, String> zkSnapshot = StUtils.ssSnapshot(CLIENT, NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME));
            boolean zkSameAsLast = zkSnapshot.equals(zkPods[0]);
            if (!zkSameAsLast) {
                LOGGER.info("ZK Cluster not stable");
            }
            if (zkSameAsLast) {
                int c = count.getAndIncrement();
                LOGGER.info("All stable for {} polls", c);
                return c > 60;
            }
            zkPods[0] = zkSnapshot;
            count.set(0);
            return false;
        });
    }

    void checkZkPodsLog(List<String> newZkPodNames) {
        for (String name : newZkPodNames) {
            //Test that second pod does not have errors or failures in events
            LOGGER.info("Checking logs fro pod {}", name);
            List<Event> eventsForSecondPod = getEvents("Pod", name);
            assertThat(eventsForSecondPod, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        }
    }

    void waitForZkPods(List<String> newZkPodNames) {
        for (String name : newZkPodNames) {
            KUBE_CLIENT.waitForPod(name);
            LOGGER.info("Pod {} is ready", name);
        }
        waitForZkRollUp();
    }

    @BeforeEach
    void createTestResources() throws Exception {
        createResources();
        resources.createServiceResource(Resources.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, NAMESPACE).done();
        resources.createIngress(Resources.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, ENVIRONMENT.getKubernetesApiUrl(), NAMESPACE).done();
    }

    @AfterEach
    void deleteTestResources() throws Exception {
        deleteResources();
        waitForDeletion(TIMEOUT_TEARDOWN, NAMESPACE);
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();
    }

    @AfterAll
    void teardownEnvironment() {
        testClassResources.deleteResources();
        teardownEnvForOperator();
    }
}
