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
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.SingleVolumeStorage;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerPlain;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.test.k8s.Oc;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.k8s.Events.SuccessfulDelete;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.TestUtils.fromYamlString;
import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.waitFor;
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

@Tag(REGRESSION)
class KafkaST extends MessagingBaseST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);

    public static final String NAMESPACE = "kafka-cluster-test";
    private static final String TOPIC_NAME = "test-topic";
    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    @Test
    @Tag(REGRESSION)
    @OpenShiftOnly
    void testDeployKafkaClusterViaTemplate() {
        createCustomResources("../examples/templates/cluster-operator");
        Oc oc = (Oc) cmdKubeClient();
        String clusterName = "openshift-my-cluster";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName));
        StUtils.waitForAllStatefulSetPodsReady(zookeeperClusterName(clusterName), 3);
        StUtils.waitForAllStatefulSetPodsReady(kafkaClusterName(clusterName), 3);
        StUtils.waitForDeploymentReady(entityOperatorDeploymentName(clusterName), 1);

        //Testing docker images
        testDockerImagesForKafkaCluster(clusterName, 3, 3, false);

        LOGGER.info("Deleting Kafka cluster {} after test", clusterName);
        oc.deleteByName("Kafka", clusterName);

        // Delete all pods created by this test
        kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(clusterName))
                .forEach(kubeClient()::deletePod);

        StUtils.waitForStatefulSetDeletion(kafkaClusterName(clusterName));
        StUtils.waitForStatefulSetDeletion(zookeeperClusterName(clusterName));
        StUtils.waitForDeploymentDeletion(entityOperatorDeploymentName(clusterName));


        StUtils.waitForStatefulSetDeletion(kafkaClusterName(clusterName));
        StUtils.waitForStatefulSetDeletion(zookeeperClusterName(clusterName));
        deleteCustomResources("../examples/templates/cluster-operator");
    }

    @Test
    void testKafkaAndZookeeperScaleUpScaleDown() throws Exception {
        long kafkaRollingUpdateTimeout = 600000;
        operationID = startTimeMeasuring(Operation.SCALE_UP);
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                            .withTls(false)
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec().done();

        testDockerImagesForKafkaCluster(CLUSTER_NAME, 3, 1, false);
        // kafka cluster already deployed
        LOGGER.info("Running kafkaScaleUpScaleDown {}", CLUSTER_NAME);

        final int initialReplicas = kubeClient().getStatefulSet(kafkaClusterName(CLUSTER_NAME)).getStatus().getReplicas();
        assertEquals(3, initialReplicas);
        // scale up
        final int scaleTo = initialReplicas + 1;
        final int newPodId = initialReplicas;
        final int newBrokerId = newPodId;
        final String newPodName = kafkaPodName(CLUSTER_NAME,  newPodId);
        final String firstPodName = kafkaPodName(CLUSTER_NAME,  0);
        LOGGER.info("Scaling up to {}", scaleTo);
        // Create snapshot of current cluster
        String kafkaSsName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, kafkaSsName);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(initialReplicas + 1));
        StUtils.waitForAllStatefulSetPodsReady(kafkaClusterName(CLUSTER_NAME), initialReplicas + 1);

        // Test that the new broker has joined the kafka cluster by checking it knows about all the other broker's API versions
        // (execute bash because we want the env vars expanded in the pod)
        String versions = getBrokerApiVersions(newPodName);
        for (int brokerId = 0; brokerId < scaleTo; brokerId++) {
            assertTrue(versions.indexOf("(id: " + brokerId + " rack: ") >= 0, versions);
        }

        //Test that the new pod does not have errors or failures in events
        String uid = CLIENT.pods().inNamespace(NAMESPACE).withName(newPodName).get().getMetadata().getUid();
        List<Event> events = getEvents(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        waitForClusterAvailability(NAMESPACE);
        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(operationID);
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));

        // scale down
        LOGGER.info("Scaling down");
        // Get kafka new pod uid before deletion
        uid = CLIENT.pods().inNamespace(NAMESPACE).withName(newPodName).get().getMetadata().getUid();
        operationID = startTimeMeasuring(Operation.SCALE_DOWN);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(initialReplicas));
        StUtils.waitTillSsHasRolled(CLIENT, NAMESPACE, kafkaSsName, kafkaPods, kafkaRollingUpdateTimeout);

        final int finalReplicas = kubeClient().getStatefulSet(kafkaClusterName(CLUSTER_NAME)).getStatus().getReplicas();
        assertEquals(initialReplicas, finalReplicas);
        versions = getBrokerApiVersions(firstPodName);

        assertTrue(versions.indexOf("(id: " + newBrokerId + " rack: ") == -1,
                "Expect the added broker, " + newBrokerId + ",  to no longer be present in output of kafka-broker-api-versions.sh");

        //Test that the new broker has event 'Killing'
        assertThat(getEvents(uid), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        uid = CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaClusterName(CLUSTER_NAME)).get().getMetadata().getUid();
        assertThat(getEvents(uid), hasAllOfReasons(SuccessfulDelete));
        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(operationID);
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    void testEODeletion() {
        // Deploy kafka cluster with EO
        Kafka kafka = testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Get pod name to check termination process
        Optional<Pod> pod = kubeClient().listPods()
                .stream().filter(p -> p.getMetadata().getName().startsWith(entityOperatorDeploymentName(CLUSTER_NAME)))
                .findFirst();

        assertTrue(pod.isPresent(), "EO pod does not exist");

        // Remove EO from Kafka DTO
        kafka.getSpec().setEntityOperator(null);
        // Replace Kafka configuration with removed EO
        testMethodResources.kafka(kafka).done();

        // Wait when EO(UO + TO) will be removed
        StUtils.waitForDeploymentDeletion(entityOperatorDeploymentName(CLUSTER_NAME));
        StUtils.waitForPodDeletion(pod.get().getMetadata().getName());
    }

    @Test
    void testZookeeperScaleUpScaleDown() {
        operationID = startTimeMeasuring(Operation.SCALE_UP);
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        // kafka cluster already deployed
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", CLUSTER_NAME);
        final int initialZkReplicas = kubeClient().getStatefulSet(zookeeperClusterName(CLUSTER_NAME)).getStatus().getReplicas();
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
        // Get zk-3 uid before deletion
        String uid = CLIENT.pods().inNamespace(NAMESPACE).withName(newZkPodNames.get(3)).get().getMetadata().getUid();
        operationID = startTimeMeasuring(Operation.SCALE_DOWN);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(initialZkReplicas));

        for (String name : newZkPodNames) {
            StUtils.waitForPodDeletion(name);
        }

        // Wait for one zk pods will became leader and others follower state
        waitForZkMntr(ZK_SERVER_STATE, 0, 1, 2);

        //Test that the second pod has event 'Killing'
        assertThat(getEvents(uid), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        uid = CLIENT.apps().statefulSets().inNamespace(NAMESPACE).withName(zookeeperClusterName(CLUSTER_NAME)).get().getMetadata().getUid();
        assertThat(getEvents(uid), hasAllOfReasons(SuccessfulDelete));
        // Stop measuring
        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    void testCustomAndUpdatedValues() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("transaction.state.log.replication.factor", "1");
        kafkaConfig.put("default.replication.factor", "1");

        Map<String, Object> zookeeperConfig = new HashMap<>();
        zookeeperConfig.put("tickTime", "2000");
        zookeeperConfig.put("initLimit", "5");
        zookeeperConfig.put("syncLimit", "2");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 2)
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
            zkPodStartTime.add(cmdKubeClient().getResourceCreateTimestamp("pod", zookeeperPodName(CLUSTER_NAME, i)));
        }
        List<Date> kafkaPodStartTime = new ArrayList<>();
        for (int i = 0; i < expectedKafkaPods; i++) {
            kafkaPodStartTime.add(cmdKubeClient().getResourceCreateTimestamp("pod", kafkaPodName(CLUSTER_NAME, i)));
        }

        LOGGER.info("Verify values before update");
        for (int i = 0; i < expectedKafkaPods; i++) {
            String kafkaPodJson = cmdKubeClient().getResourceAsJson("pod", kafkaPodName(CLUSTER_NAME, i));
            assertThat(kafkaPodJson, hasJsonPath(globalVariableJsonPathBuilder("KAFKA_CONFIGURATION"),
                    hasItem("default.replication.factor=1\noffsets.topic.replication.factor=1\ntransaction.state.log.replication.factor=1\n")));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(30)));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(10)));
        }
        LOGGER.info("Testing Zookeepers");
        for (int i = 0; i < expectedZKPods; i++) {
            String zkPodJson = cmdKubeClient().getResourceAsJson("pod", zookeeperPodName(CLUSTER_NAME, i));
            assertThat(zkPodJson, hasJsonPath(globalVariableJsonPathBuilder("ZOOKEEPER_CONFIGURATION"),
                    hasItem("autopurge.purgeInterval=1\ntickTime=2000\ninitLimit=5\nsyncLimit=2\n")));
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
            zookeeperClusterSpec.setConfig(TestUtils.fromJson("{\"tickTime\": 2100, \"initLimit\": 6, \"syncLimit\": 3}", Map.class));
        });

        for (int i = 0; i < expectedZKPods; i++) {
            StUtils.waitForPodUpdate(zookeeperPodName(CLUSTER_NAME, i), zkPodStartTime.get(i));
            StUtils.waitForPod(zookeeperPodName(CLUSTER_NAME,  i));
        }
        for (int i = 0; i < expectedKafkaPods; i++) {
            StUtils.waitForPodUpdate(kafkaPodName(CLUSTER_NAME, i), kafkaPodStartTime.get(i));
            StUtils.waitForPod(kafkaPodName(CLUSTER_NAME,  i));
        }

        LOGGER.info("Verify values after update");
        for (int i = 0; i < expectedKafkaPods; i++) {
            String kafkaPodJson = cmdKubeClient().getResourceAsJson("pod", kafkaPodName(CLUSTER_NAME, i));
            assertThat(kafkaPodJson, hasJsonPath(globalVariableJsonPathBuilder("KAFKA_CONFIGURATION"),
                    hasItem("default.replication.factor=2\noffsets.topic.replication.factor=2\ntransaction.state.log.replication.factor=2\n")));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(31)));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(11)));
        }
        LOGGER.info("Testing Zookeepers");
        for (int i = 0; i < expectedZKPods; i++) {
            String zkPodJson = cmdKubeClient().getResourceAsJson("pod", zookeeperPodName(CLUSTER_NAME, i));
            assertThat(zkPodJson, hasJsonPath(globalVariableJsonPathBuilder("ZOOKEEPER_CONFIGURATION"),
                    hasItem("autopurge.purgeInterval=1\ntickTime=2100\ninitLimit=6\nsyncLimit=3\n")));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(31)));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(11)));
        }
    }

    /**
     * Test sending messages over plain transport, without auth
     */
    @Test
    void testSendMessagesPlainAnonymous() throws Exception {
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        testMethodResources().topic(CLUSTER_NAME, topicName).done();

        testMethodResources().deployKafkaClients(CLUSTER_NAME).done();

        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, false, topicName, null);
    }

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @Test
    void testSendMessagesTlsAuthenticated() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Use a Kafka with plain listener disabled
        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withTls(listenerTls)
                            .withNewTls()
                            .endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        testMethodResources().topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = testMethodResources().tlsUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);

        testMethodResources().deployKafkaClients(true, CLUSTER_NAME, user).done();
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, true, topicName, user);
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @Test
    void testSendMessagesPlainScramSha() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaListenerAuthenticationScramSha512 auth = new KafkaListenerAuthenticationScramSha512();
        KafkaListenerPlain listenerTls = new KafkaListenerPlain();
        listenerTls.setAuthentication(auth);

        // Use a Kafka with plain listener disabled
        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withPlain(listenerTls)
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        testMethodResources().topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = testMethodResources().scramShaUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);
        String brokerPodLog = kubeClient().logs(CLUSTER_NAME + "-kafka-0", "kafka");
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

        testMethodResources().deployKafkaClients(false, CLUSTER_NAME, user).done();
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, false, topicName, user);
    }

    /**
     * Test sending messages over tls transport using scram sha auth
     */
    @Test
    void testSendMessagesTlsScramSha() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(new KafkaListenerAuthenticationScramSha512());

        // Use a Kafka with plain listener disabled
        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        testMethodResources().topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = testMethodResources().scramShaUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);

        testMethodResources().deployKafkaClients(true, CLUSTER_NAME, user).done();
        availabilityTest(messagesCount, 180000, CLUSTER_NAME, true, topicName, user);
    }

    @Test
    void testJvmAndResources() {
        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1)
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
        Map<String, String> zkPods = StUtils.ssSnapshot(NAMESPACE, zkSsName);
        Map<String, String> kafkaPods = StUtils.ssSnapshot(NAMESPACE, kafkaSsName);
        Map<String, String> eoPods = StUtils.depSnapshot(NAMESPACE, eoDepName);

        assertResources(cmdKubeClient().namespace(), kafkaPodName(CLUSTER_NAME, 0), "kafka",
                "1536Mi", "1", "1Gi", "500m");
        assertExpectedJavaOpts(kafkaPodName(CLUSTER_NAME, 0), "kafka",
                "-Xmx1g", "-Xms512m", "-server", "-XX:+UseG1GC");

        assertResources(cmdKubeClient().namespace(), zookeeperPodName(CLUSTER_NAME, 0), "zookeeper",
                "1G", "500m", "500M", "250m");
        assertExpectedJavaOpts(zookeeperPodName(CLUSTER_NAME, 0), "zookeeper",
                "-Xmx1G", "-Xms512M", "-server", "-XX:+UseG1GC");

        Optional<Pod> pod = kubeClient().listPods()
                .stream().filter(p -> p.getMetadata().getName().startsWith(entityOperatorDeploymentName(CLUSTER_NAME)))
                .findFirst();
        assertTrue(pod.isPresent(), "EO pod does not exist");

        assertResources(cmdKubeClient().namespace(), pod.get().getMetadata().getName(), "topic-operator",
                "1Gi", "500m", "512Mi", "250m");
        assertResources(cmdKubeClient().namespace(), pod.get().getMetadata().getName(), "user-operator",
                "512M", "300m", "256M", "300m");

        TestUtils.waitFor("Wait till reconciliation timeout", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> !cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, operationID), "\"Assembly reconciled\"").isEmpty());

        // Checking no rolling update after last CO reconciliation
        LOGGER.info("Checking no rolling update for Kafka cluster");
        assertFalse(StUtils.ssHasRolled(NAMESPACE, zkSsName, zkPods));
        assertFalse(StUtils.ssHasRolled(NAMESPACE, kafkaSsName, kafkaPods));
        assertFalse(StUtils.depHasRolled(NAMESPACE, eoDepName, eoPods));
        TimeMeasuringSystem.stopOperation(operationID);
    }

    @Test
    void testForTopicOperator() throws InterruptedException {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        //Creating topics for testing
        cmdKubeClient().create(TOPIC_CM);
        TestUtils.waitFor("wait for 'my-topic' to be created in Kafka", GLOBAL_POLL_INTERVAL, TIMEOUT_FOR_TOPIC_CREATION, () -> {
            List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
            return topics.contains("my-topic");
        });

        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), hasItem("my-topic"));

        createTopicUsingPodCLI(CLUSTER_NAME, 0, "topic-from-cli", 1, 1);
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), hasItems("my-topic", "topic-from-cli"));
        assertThat(cmdKubeClient().list("kafkatopic"), hasItems("my-topic", "topic-from-cli", "my-topic"));

        //Updating first topic using pod CLI
        updateTopicPartitionsCountUsingPodCLI(CLUSTER_NAME, 0, "my-topic", 2);
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, "my-topic"),
                hasItems("PartitionCount:2"));
        KafkaTopic testTopic = fromYamlString(cmdKubeClient().get("kafkatopic", "my-topic"), KafkaTopic.class);
        assertNotNull(testTopic);
        assertNotNull(testTopic.getSpec());
        assertEquals(Integer.valueOf(2), testTopic.getSpec().getPartitions());

        //Updating second topic via KafkaTopic update
        replaceTopicResource("topic-from-cli", topic -> {
            topic.getSpec().setPartitions(2);
        });
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, "topic-from-cli"),
                hasItems("PartitionCount:2"));
        testTopic = fromYamlString(cmdKubeClient().get("kafkatopic", "topic-from-cli"), KafkaTopic.class);
        assertNotNull(testTopic);
        assertNotNull(testTopic.getSpec());
        assertEquals(Integer.valueOf(2), testTopic.getSpec().getPartitions());

        //Deleting first topic by deletion of CM
        cmdKubeClient().deleteByName("kafkatopic", "topic-from-cli");

        //Deleting another topic using pod CLI
        deleteTopicUsingPodCLI(CLUSTER_NAME, 0, "my-topic");
        StUtils.waitForKafkaTopicDeletion("my-topic");

        //Checking all topics were deleted
        Thread.sleep(Constants.TIMEOUT_TEARDOWN);
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems("my-topic")));
        assertThat(topics, not(hasItems("topic-from-cli")));
    }

    @Test
    void testTopicWithoutLabels() {
        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Creating topic without any label
        testMethodResources().topic(CLUSTER_NAME, "topic-without-labels")
            .editMetadata()
                .withLabels(null)
            .endMetadata()
            .done();

        // Checking that resource was created
        assertThat(cmdKubeClient().list("kafkatopic"), hasItems("topic-without-labels"));
        // Checking that TO didn't handl new topic and zk pods don't contain new topic
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), not(hasItems("topic-without-labels")));

        // Checking TO logs
        String tOPodName = cmdKubeClient().listResourcesByLabel("pod", "strimzi.io/name=my-cluster-entity-operator").get(0);
        String tOlogs = kubeClient().logs(tOPodName, "topic-operator");
        assertThat(tOlogs, not(containsString("Created topic 'topic-without-labels'")));

        //Deleting topic
        cmdKubeClient().deleteByName("kafkatopic", "topic-without-labels");
        StUtils.waitForKafkaTopicDeletion("topic-without-labels");

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
            String kafkaVersion = Crds.kafkaOperation(kubeClient().getClient()).inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getKafka().getVersion();
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
        String entityOperatorPodName = cmdKubeClient().listResourcesByLabel("pod",
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
    void testRackAware() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewRack()
                        .withTopologyKey("rack-key")
                    .endRack()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                            .withTls(false)
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        testDockerImagesForKafkaCluster(CLUSTER_NAME, 1, 1, true);

        String kafkaPodName = kafkaPodName(CLUSTER_NAME, 0);
        StUtils.waitForPod(kafkaPodName);

        String rackId = kubeClient().execInPod(kafkaPodName, "kafka", "/bin/bash", "-c", "cat /opt/kafka/init/rack.id");
        assertEquals("zone", rackId.trim());

        String brokerRack = kubeClient().execInPod(kafkaPodName, "kafka", "/bin/bash", "-c", "cat /tmp/strimzi.properties | grep broker.rack");
        assertTrue(brokerRack.contains("broker.rack=zone"));

        String uid = CLIENT.pods().inNamespace(NAMESPACE).withName(kafkaPodName).get().getMetadata().getUid();
        List<Event> events = getEvents(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    void testManualTriggeringRollingUpdate() {
        String coPodName = kubeClient().listPods("name", "strimzi-cluster-operator").get(0).getMetadata().getName();
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1).done();

        // rolling update for kafka
        operationID = startTimeMeasuring(Operation.ROLLING_UPDATE);
        // set annotation to trigger Kafka rolling update
        kubeClient().statefulSet(kafkaClusterName(CLUSTER_NAME)).cascading(false).edit()
                .editMetadata()
                    .addToAnnotations("strimzi.io/manual-rolling-update", "true")
                .endMetadata().done();

        // check annotation to trigger rolling update
        assertTrue(Boolean.parseBoolean(kubeClient().getStatefulSet(kafkaClusterName(CLUSTER_NAME))
                .getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")));

        // wait when annotation will be removed
        waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.WAIT_FOR_ROLLING_UPDATE_TIMEOUT,
            () -> getAnnotationsForSS(NAMESPACE, kafkaClusterName(CLUSTER_NAME)) == null
                || !getAnnotationsForSS(NAMESPACE, kafkaClusterName(CLUSTER_NAME)).containsKey("strimzi.io/manual-rolling-update"));

        // check rolling update messages in CO log
        String coLog = kubeClient().logs(coPodName);
        assertThat(coLog, containsString("Rolling Kafka pod " + kafkaClusterName(CLUSTER_NAME) + "-0" + " due to manual rolling update"));

        // rolling update for zookeeper
        operationID = startTimeMeasuring(Operation.ROLLING_UPDATE);
        // set annotation to trigger Zookeeper rolling update
        kubeClient().statefulSet(zookeeperClusterName(CLUSTER_NAME)).cascading(false).edit()
                .editMetadata()
                    .addToAnnotations("strimzi.io/manual-rolling-update", "true")
                .endMetadata().done();

        // check annotation to trigger rolling update
        assertTrue(Boolean.parseBoolean(kubeClient().getStatefulSet(zookeeperClusterName(CLUSTER_NAME))
                .getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")));

        // wait when annotation will be removed
        waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.WAIT_FOR_ROLLING_UPDATE_TIMEOUT,
            () -> getAnnotationsForSS(NAMESPACE, zookeeperClusterName(CLUSTER_NAME)) == null
                || !getAnnotationsForSS(NAMESPACE, zookeeperClusterName(CLUSTER_NAME)).containsKey("strimzi.io/manual-rolling-update"));

        // check rolling update messages in CO log
        coLog = kubeClient().logs(coPodName);
        assertThat(coLog, containsString("Rolling Zookeeper pod " + zookeeperClusterName(CLUSTER_NAME) + "-0" + " to manual rolling update"));
    }

    @Test
    void testNodePort() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .done();

        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    void testNodePortTls() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                    .withNewKafkaListenerExternalNodePort()
                    .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .done();

        String userName = "alice";
        resources().tlsUser(CLUSTER_NAME, userName).done();
        waitFor("Wait for secrets became available", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS,
            () -> kubeClient().getSecret("alice") != null,
            () -> LOGGER.error("Couldn't find user secret {}", kubeClient().listSecrets()));

        waitForClusterAvailabilityTls(userName, NAMESPACE);
    }

    @Test
    void testLoadBalancer() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                            .withTls(false)
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .done();

        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    void testLoadBalancerTls() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .done();

        String userName = "alice";
        resources().tlsUser(CLUSTER_NAME, userName).done();
        waitFor("Wait for secrets became available", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS,
            () -> kubeClient().getSecret("alice") != null,
            () -> LOGGER.error("Couldn't find user secret {}", kubeClient().listSecrets()));

        waitForClusterAvailabilityTls(userName, NAMESPACE);
    }

    private Map<String, String> getAnnotationsForSS(String namespace, String ssName) {
        return kubeClient().getStatefulSet(ssName).getMetadata().getAnnotations();
    }

    void waitForZkRollUp() {
        LOGGER.info("Waiting for cluster stability");
        Map<String, String>[] zkPods = new Map[1];
        AtomicInteger count = new AtomicInteger();
        zkPods[0] = StUtils.ssSnapshot(NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME));
        TestUtils.waitFor("Cluster stable and ready", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_ZK_CLUSTER_STABILIZATION, () -> {
            Map<String, String> zkSnapshot = StUtils.ssSnapshot(NAMESPACE, zookeeperStatefulSetName(CLUSTER_NAME));
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
            String uid = CLIENT.pods().inNamespace(NAMESPACE).withName(name).get().getMetadata().getUid();
            List<Event> eventsForSecondPod = getEvents(uid);
            assertThat(eventsForSecondPod, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        }
    }

    void waitForZkPods(List<String> newZkPodNames) {
        for (String name : newZkPodNames) {
            StUtils.waitForPod(name);
            LOGGER.info("Pod {} is ready", name);
        }
        waitForZkRollUp();
    }

    @Test
    @Tag(REGRESSION)
    void testKafkaJBODDeleteClaimsTrueFalse() {
        int kafkaReplicas = 2;
        int diskSizeGi = 10;

        List<SingleVolumeStorage> volumes = new ArrayList<>();
        volumes.add(new PersistentClaimStorageBuilder()
                .withId(0)
                .withDeleteClaim(true)
                .withSize(diskSizeGi + "Gi").build());

        volumes.add(new PersistentClaimStorageBuilder()
                .withId(1)
                .withDeleteClaim(false)
                .withSize(diskSizeGi + "Gi").build());

        testMethodResources().kafkaJBOD(CLUSTER_NAME, kafkaReplicas, volumes).done();
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(2, 2, 10);
        LOGGER.info("Deleting cluster");
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME).waitForResourceDeletion("pod", kafkaPodName(CLUSTER_NAME, 0));
        verifyPVCDeletion(2, volumes);
    }

    @Test
    @Tag(REGRESSION)
    void testKafkaJBODDeleteClaimsTrue() {
        int diskCountPerReplica = 2;
        int kafkaReplicas = 2;
        int diskSizeGi = 10;

        List<SingleVolumeStorage> volumes = new ArrayList<>();
        for (int i = 0; i < diskCountPerReplica; i++) {
            volumes.add(new PersistentClaimStorageBuilder()
                    .withId(i)
                    .withDeleteClaim(true)
                    .withSize(diskSizeGi + "Gi").build());
        }

        testMethodResources().kafkaJBOD(CLUSTER_NAME, kafkaReplicas, volumes).done();
        // kafka cluster already deployed

        verifyVolumeNamesAndLabels(2, 2, 10);
        LOGGER.info("Deleting cluster");
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME).waitForResourceDeletion("pod", kafkaPodName(CLUSTER_NAME, 0));
        verifyPVCDeletion(2, volumes);
    }

    @Test
    @Tag(REGRESSION)
    void testKafkaJBODDeleteClaimsFalse() {
        int diskCountPerReplica = 2;
        int kafkaReplicas = 2;
        int diskSizeGi = 10;

        List<SingleVolumeStorage> volumes = new ArrayList<>();
        for (int i = 0; i < diskCountPerReplica; i++) {
            volumes.add(new PersistentClaimStorageBuilder()
                    .withId(i)
                    .withDeleteClaim(false)
                    .withSize(diskSizeGi + "Gi").build());
        }

        testMethodResources().kafkaJBOD(CLUSTER_NAME, kafkaReplicas, volumes).done();
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(2, 2, 10);
        LOGGER.info("Deleting cluster");
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME).waitForResourceDeletion("pod", kafkaPodName(CLUSTER_NAME, 0));
        verifyPVCDeletion(2, volumes);
    }

    void verifyPVCDeletion(int kafkaReplicas, List<SingleVolumeStorage> volumes) {
        ArrayList pvcs = new ArrayList();
        kubeClient().listPersistentVolumeClaims().stream()
                .forEach(pvc -> pvcs.add(pvc.getMetadata().getName()));

        volumes.forEach(volume -> {
            for (int i = 0; i < kafkaReplicas; i++) {
                String volumeName = "data-" + volume.getId() + "-" + CLUSTER_NAME + "-kafka-" + i;
                LOGGER.info("Verifying volume: " + volumeName);
                if (((PersistentClaimStorage) volume).isDeleteClaim()) {
                    assertFalse(pvcs.contains(volumeName));
                } else {
                    assertTrue(pvcs.contains(volumeName));
                }

            }
        });
    }

    void verifyVolumeNamesAndLabels(int kafkaReplicas, int diskCountPerReplica, int diskSizeGi) {

        ArrayList pvcs = new ArrayList();

        kubeClient().listPersistentVolumeClaims().stream()
                .forEach(volume -> {
                    String volumeName = volume.getMetadata().getName();
                    pvcs.add(volumeName);
                    LOGGER.info("Checking labels for volume:" + volumeName);
                    assertEquals(CLUSTER_NAME, volume.getMetadata().getLabels().get("strimzi.io/cluster"));
                    assertEquals("Kafka", volume.getMetadata().getLabels().get("strimzi.io/kind"));
                    assertEquals(CLUSTER_NAME.concat("-kafka"), volume.getMetadata().getLabels().get("strimzi.io/name"));
                    assertEquals(diskSizeGi + "Gi", volume.getSpec().getResources().getRequests().get("storage").getAmount());
                });

        LOGGER.info("Checking PVC names included in JBOD array");
        for (int i = 0; i < kafkaReplicas; i++) {
            for (int j = 0; j < diskCountPerReplica; j++) {
                pvcs.contains("data-" + j + "-" + CLUSTER_NAME + "-kafka-" + i);
            }
        }

        LOGGER.info("Checking PVC on Kafka pods");
        for (int i = 0; i < kafkaReplicas; i++) {
            ArrayList dataSourcesOnPod = new ArrayList();
            ArrayList pvcsOnPod = new ArrayList();

            LOGGER.info("Getting list of mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                dataSourcesOnPod.add(kubeClient().getPod(CLUSTER_NAME.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getName());
                pvcsOnPod.add(kubeClient().getPod(CLUSTER_NAME.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getPersistentVolumeClaim().getClaimName());
            }

            LOGGER.info("Verifying mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                dataSourcesOnPod.contains("data-" + j);
                pvcsOnPod.contains("data-" + j + "-" + CLUSTER_NAME + "-kafka-" + i);
            }
        }
    }

    @BeforeEach
    void createTestResources() throws Exception {
        createTestMethodResources();
        testMethodResources.createServiceResource(Constants.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, NAMESPACE).done();
        testMethodResources.createIngress(Constants.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, CONFIG.getMasterUrl(), NAMESPACE).done();
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

    @Override
    void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN, NAMESPACE);
    }
}
