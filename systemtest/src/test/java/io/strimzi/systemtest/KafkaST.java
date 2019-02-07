/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.KafkaListenerPlain;
import io.strimzi.api.kafka.model.KafkaListenerTls;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import io.strimzi.test.annotations.OpenShiftOnly;
import io.strimzi.test.extensions.StrimziExtension;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.Oc;
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
import static io.strimzi.test.extensions.StrimziExtension.ACCEPTANCE;
import static io.strimzi.test.extensions.StrimziExtension.FLAKY;
import static io.strimzi.test.extensions.StrimziExtension.CCI_FLAKY;
import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static io.strimzi.test.TestUtils.fromYamlString;
import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.waitFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@ExtendWith(StrimziExtension.class)
class KafkaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);

    public static final String NAMESPACE = "kafka-cluster-test";
    private static final String TOPIC_NAME = "test-topic";
    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    private static final long POLL_INTERVAL_FOR_CREATION = 1_000;
    private static final long TIMEOUT_FOR_MIRROR_MAKER_CREATION = 120_000;
    private static final long TIMEOUT_FOR_TOPIC_CREATION = 60_000;
    private static final long POLL_INTERVAL_SECRET_CREATION = 5_000;
    private static final long TIMEOUT_FOR_SECRET_CREATION = 360_000;
    private static final long TIMEOUT_FOR_ZK_CLUSTER_STABILIZATION = 450_000;

    @Test
    @Tag(REGRESSION)
    @OpenShiftOnly
    void testDeployKafkaClusterViaTemplate() {
        createCustomResources("../examples/templates/cluster-operator");
        Oc oc = (Oc) KUBE_CLIENT;
        String clusterName = "openshift-my-cluster";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName));
        oc.waitForStatefulSet(zookeeperClusterName(clusterName), 3);
        oc.waitForStatefulSet(kafkaClusterName(clusterName), 3);

        //Testing docker images
        testDockerImagesForKafkaCluster(clusterName, 3, 3, false);

        LOGGER.info("Deleting Kafka cluster {} after test", clusterName);
        oc.deleteByName("Kafka", clusterName);
        oc.waitForResourceDeletion("statefulset", kafkaClusterName(clusterName));
        oc.waitForResourceDeletion("statefulset", zookeeperClusterName(clusterName));

        CLIENT.pods().list().getItems().stream()
                .filter(p -> p.getMetadata().getName().startsWith(clusterName))
                .forEach(p -> waitForPodDeletion(NAMESPACE, p.getMetadata().getName()));
        deleteCustomResources("../examples/templates/cluster-operator");
    }

    @Test
    @Tag(ACCEPTANCE)
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
    @Tag(FLAKY)
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
        assertThat(getEvents("Pod", newZkPodNames.get(4)), hasAllOfReasons(Killing));
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
                    hasItem("default.replication.factor=1\noffsets.topic.replication.factor=1\ntransaction.state.log.replication.factor=1")));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(30)));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(10)));
        }
        LOGGER.info("Testing Zookeepers");
        for (int i = 0; i < expectedZKPods; i++) {
            String zkPodJson = KUBE_CLIENT.getResourceAsJson("pod", zookeeperPodName(CLUSTER_NAME, i));
            assertThat(zkPodJson, hasJsonPath(globalVariableJsonPathBuilder("ZOOKEEPER_CONFIGURATION"),
                    hasItem("timeTick=2000\nautopurge.purgeInterval=1\nsyncLimit=2\ninitLimit=5\n")));
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
                    hasItem("transaction.state.log.replication.factor=2\ndefault.replication.factor=2\noffsets.topic.replication.factor=2\n")));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(31)));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(11)));
        }
        LOGGER.info("Testing Zookeepers");
        for (int i = 0; i < expectedZKPods; i++) {
            String zkPodJson = KUBE_CLIENT.getResourceAsJson("pod", zookeeperPodName(CLUSTER_NAME, i));
            assertThat(zkPodJson, hasJsonPath(globalVariableJsonPathBuilder("ZOOKEEPER_CONFIGURATION"),
                    hasItem("timeTick=2100\nautopurge.purgeInterval=1\nsyncLimit=3\ninitLimit=6\n")));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(31)));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(11)));
        }
    }

    /**
     * Test sending messages over plain transport, without auth
     */
    @Test
    @Tag(ACCEPTANCE)
    void testSendMessagesPlainAnonymous() throws InterruptedException {
        String name = "send-messages-plain-anon";
        int messagesCount = 20;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        resources().topic(CLUSTER_NAME, topicName).done();

        // Create ping job
        Job job = waitForJobSuccess(pingJob(name, topicName, messagesCount, null, false));

        // Now get the pod logs (which will be both producer and consumer logs)
        checkPings(messagesCount, job);
    }

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @Test
    @Tag(CCI_FLAKY)
    void testSendMessagesTlsAuthenticated() {
        String kafkaUser = "my-user";
        String name = "send-messages-tls-auth";
        int messagesCount = 20;
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

        // Create ping job
        Job job = waitForJobSuccess(pingJob(name, topicName, messagesCount, user, true));

        // Now check the pod logs the messages were produced and consumed
        checkPings(messagesCount, job);
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @Test
    @Tag(CCI_FLAKY)
    void testSendMessagesPlainScramSha() {
        String kafkaUser = "my-user";
        String name = "send-messages-plain-scram-sha";
        int messagesCount = 20;
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

        // Create ping job
        Job job = waitForJobSuccess(pingJob(name, topicName, messagesCount, user, false));

        // Now check the pod logs the messages were produced and consumed
        checkPings(messagesCount, job);
    }

    /**
     * Test sending messages over tls transport using scram sha auth
     */
    @Test
    @Tag(CCI_FLAKY)
    void testSendMessagesTlsScramSha() {
        String kafkaUser = "my-user";
        String name = "send-messages-tls-scram-sha";
        int messagesCount = 20;
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

        // Create ping job
        Job job = waitForJobSuccess(pingJob(name, topicName, messagesCount, user, true));

        // Now check the pod logs the messages were produced and consumed
        checkPings(messagesCount, job);
    }

    @Test
    @Tag(REGRESSION)
    void testJvmAndResources() {
        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resources().kafkaEphemeral(CLUSTER_NAME, 1)
            .editSpec()
                .editKafka()
                    .withNewResources()
                        .withNewLimits()
                            .withMemory("2Gi")
                            .withMilliCpu("400m")
                        .endLimits()
                        .withNewRequests()
                            .withMemory("2Gi")
                            .withMilliCpu("400m")
                        .endRequests()
                    .endResources()
                    .withNewJvmOptions()
                        .withXmx("1g")
                        .withXms("1G")
                        .withServer(true)
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                .withNewResources()
                    .withNewLimits()
                        .withMemory("1Gi")
                        .withMilliCpu("300m")
                    .endLimits()
                        .withNewRequests()
                        .withMemory("1Gi")
                        .withMilliCpu("300m")
                    .endRequests()
                .endResources()
                    .withNewJvmOptions()
                        .withXmx("600m")
                        .withXms("300m")
                        .withServer(true)
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endZookeeper()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                        .withNewResources()
                            .withNewLimits()
                                .withMemory("500M")
                                .withMilliCpu("300m")
                            .endLimits()
                            .withNewRequests()
                                .withMemory("500M")
                                .withMilliCpu("300m")
                            .endRequests()
                        .endResources()
                    .endTopicOperator()
                    .withNewUserOperator()
                        .withNewResources()
                            .withNewLimits()
                                .withMemory("500M")
                                .withMilliCpu("300m")
                            .endLimits()
                            .withNewRequests()
                                .withMemory("500M")
                                .withMilliCpu("300m")
                            .endRequests()
                        .endResources()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec().done();

        assertResources(KUBE_CLIENT.namespace(), kafkaPodName(CLUSTER_NAME, 0),
                "2Gi", "400m", "2Gi", "400m");
        assertExpectedJavaOpts(kafkaPodName(CLUSTER_NAME, 0),
                "-Xmx1g", "-Xms1G", "-server", "-XX:+UseG1GC");

        assertResources(KUBE_CLIENT.namespace(), zookeeperPodName(CLUSTER_NAME, 0),
                "1Gi", "300m", "1Gi", "300m");
        assertExpectedJavaOpts(zookeeperPodName(CLUSTER_NAME, 0),
                "-Xmx600m", "-Xms300m", "-server", "-XX:+UseG1GC");

        String podName = CLIENT.pods().inNamespace(KUBE_CLIENT.namespace()).list().getItems()
                .stream().filter(p -> p.getMetadata().getName().startsWith(entityOperatorDeploymentName(CLUSTER_NAME)))
                .findFirst().get().getMetadata().getName();

        assertResources(KUBE_CLIENT.namespace(), podName,
                "500M", "300m", "500M", "300m");
    }

    @Test
    @Tag(REGRESSION)
    void testForTopicOperator() throws InterruptedException {

        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "3");
        kafkaConfig.put("transaction.state.log.replication.factor", "3");
        kafkaConfig.put("transaction.state.log.min.isr", "2");

        resources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withConfig(kafkaConfig)
                .endKafka()
            .endSpec().done();

        //Creating topics for testing
        KUBE_CLIENT.create(TOPIC_CM);
        TestUtils.waitFor("wait for 'my-topic' to be created in Kafka", POLL_INTERVAL_FOR_CREATION, TIMEOUT_FOR_TOPIC_CREATION, () -> {
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
        Thread.sleep(10000L);
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems("my-topic")));
        assertThat(topics, not(hasItems("topic-from-cli")));
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
                kafkaVersion = "2.1.0";
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
    void testMirrorMaker() {
        operationID = startTimeMeasuring(Operation.MM_DEPLOYMENT);
        String topicSourceName = TOPIC_NAME + "-source" + "-" + rng.nextInt(Integer.MAX_VALUE);
        String nameProducerSource = "send-messages-producer-source";
        String nameConsumerSource = "send-messages-consumer-source";
        String nameConsumerTarget = "send-messages-consumer-target";
        String kafkaSourceName = CLUSTER_NAME + "-source";
        String kafkaTargetName = CLUSTER_NAME + "-target";
        int messagesCount = 20;

        // Deploy source kafka
        resources().kafkaEphemeral(kafkaSourceName, 3).done();
        // Deploy target kafka
        resources().kafkaEphemeral(kafkaTargetName, 3).done();
        // Deploy Topic
        resources().topic(kafkaSourceName, topicSourceName).done();
        // Deploy Mirror Maker
        resources().kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, false).done();

        TimeMeasuringSystem.stopOperation(operationID);
        // Wait when Mirror Maker will join group
        waitFor("Mirror Maker will join group", POLL_INTERVAL_FOR_CREATION, TIMEOUT_FOR_MIRROR_MAKER_CREATION, () ->
            !KUBE_CLIENT.searchInLog("deploy", "my-cluster-mirror-maker", TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID),  "\"Successfully joined group\"").isEmpty()
        );

        // Create job to send 20 records using Kafka producer for source cluster
        waitForJobSuccess(sendRecordsToClusterJob(kafkaSourceName, nameProducerSource, topicSourceName, messagesCount, null, false));
        // Create job to read 20 records using Kafka producer for source cluster
        waitForJobSuccess(readMessagesFromClusterJob(kafkaSourceName, nameConsumerSource, topicSourceName, messagesCount, null, false));
        // Create job to read 20 records using Kafka consumer for target cluster
        Job jobReadMessagesForTarget = waitForJobSuccess(readMessagesFromClusterJob(kafkaTargetName, nameConsumerTarget, topicSourceName, messagesCount, null, false));
        // Check consumed messages in target cluster
        checkRecordsForConsumer(messagesCount, jobReadMessagesForTarget);
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using mutual tls auth
     */
    @Test
    @Tag(CCI_FLAKY)
    void testMirrorMakerTlsAuthenticated() {
        operationID = startTimeMeasuring(Operation.MM_DEPLOYMENT);
        String topicSourceName = TOPIC_NAME + "-source" + "-" + rng.nextInt(Integer.MAX_VALUE);
        String nameProducerSource = "send-messages-producer-source";
        String nameConsumerSource = "send-messages-consumer-source";
        String nameConsumerTarget = "send-messages-consumer-target";
        String kafkaUser = "my-user";
        String kafkaSourceName = CLUSTER_NAME + "-source";
        String kafkaTargetName = CLUSTER_NAME + "-target";
        int messagesCount = 20;

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Deploy source kafka with tls listener and mutual tls auth
        resources().kafka(resources().defaultKafka(CLUSTER_NAME + "-source", 3)
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
        resources().kafka(resources().defaultKafka(CLUSTER_NAME + "-target", 3)
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
        resources().topic(kafkaSourceName, topicSourceName).done();

        // Create Kafka user
        KafkaUser user = resources().tlsUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(clusterCaCertSecretName(kafkaSourceName));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(clusterCaCertSecretName(kafkaTargetName));

        // Deploy Mirror Maker with tls listener and mutual tls auth
        resources().kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, true)
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
        waitFor("Mirror Maker will join group", POLL_INTERVAL_FOR_CREATION, TIMEOUT_FOR_MIRROR_MAKER_CREATION, () ->
            !KUBE_CLIENT.searchInLog("deploy", CLUSTER_NAME + "-mirror-maker", TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID),  "\"Successfully joined group\"").isEmpty()
        );

        // Create job to send 20 records using Kafka producer for source cluster
        waitForJobSuccess(sendRecordsToClusterJob(kafkaSourceName, nameProducerSource, topicSourceName, messagesCount, user, true));
        // Create job to read 20 records using Kafka producer for source cluster
        waitForJobSuccess(readMessagesFromClusterJob(kafkaSourceName, nameConsumerSource, topicSourceName, messagesCount, user, true));
        // Create job to read 20 records using Kafka consumer for target cluster
        Job jobReadMessagesForTarget = waitForJobSuccess(readMessagesFromClusterJob(kafkaTargetName, nameConsumerTarget, topicSourceName, messagesCount, user, true));
        // Check consumed messages in target cluster
        checkRecordsForConsumer(messagesCount, jobReadMessagesForTarget);
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using scram-sha auth
     */
    @Test
    @Tag(CCI_FLAKY)
    void testMirrorMakerTlsScramSha() {
        operationID = startTimeMeasuring(Operation.MM_DEPLOYMENT);
        String kafkaUserSource = "my-user-source";
        String kafkaUserTarget = "my-user-target";
        String nameProducerSource = "send-messages-producer-source";
        String nameConsumerSource = "read-messages-consumer-source";
        String nameConsumerTarget = "read-messages-consumer-target";
        String kafkaSourceName = CLUSTER_NAME + "-source";
        String kafkaTargetName = CLUSTER_NAME + "-target";
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);
        int messagesCount = 20;


        // Deploy source kafka with tls listener and SCRAM-SHA authentication
        resources().kafka(resources().defaultKafka(kafkaSourceName, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();

        // Deploy target kafka with tls listener and SCRAM-SHA authentication
        resources().kafka(resources().defaultKafka(kafkaTargetName, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                          .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();

        // Deploy topic
        resources().topic(kafkaSourceName, topicName).done();

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

        TimeMeasuringSystem.stopOperation(operationID);
        // Wait when Mirror Maker will join group
        waitFor("Mirror Maker will join group", POLL_INTERVAL_FOR_CREATION, TIMEOUT_FOR_MIRROR_MAKER_CREATION, () ->
            !KUBE_CLIENT.searchInLog("deploy", CLUSTER_NAME + "-mirror-maker", TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID),  "\"Successfully joined group\"").isEmpty()
        );

        // Create job to send 20 records using Kafka producer for source cluster
        waitForJobSuccess(sendRecordsToClusterJob(CLUSTER_NAME + "-source", nameProducerSource, topicName, messagesCount, userSource, true));
        // Create job to read 20 records using Kafka consumer for source cluster
        waitForJobSuccess(readMessagesFromClusterJob(CLUSTER_NAME + "-source", nameConsumerSource, topicName, messagesCount, userSource, true));
        // Create job to read 20 records using Kafka consumer for target cluster
        Job jobReadMessagesForTarget = waitForJobSuccess(readMessagesFromClusterJob(CLUSTER_NAME + "-target", nameConsumerTarget, topicName, messagesCount, userTarget, true));
        // Check consumed messages in target cluster
        checkRecordsForConsumer(messagesCount, jobReadMessagesForTarget);
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
    void createTestResources() {
        createResources();
    }

    @AfterEach
    void deleteTestResources() throws Exception {
        deleteResources();
        waitForDeletion(TEARDOWN_GLOBAL_WAIT, NAMESPACE);
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
