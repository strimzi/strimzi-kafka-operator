/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.k8s.Events.SuccessfulDelete;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(REGRESSION)
class RollingUpdateST extends MessagingBaseST {

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    static final String NAMESPACE = "rolling-update-cluster-test";
    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    private String defaultKafkaClientsPodName = "";

    @Test
    void testRecoveryDuringZookeeperRollingUpdate() throws Exception {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        int messageCount = 50;
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        String logZkPattern = "'Exceeded timeout of .* while waiting for Pods resource " + KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)  + ".*'";

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName, 2, 2).done();

        int sent = sendMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(sent, is(messageCount));

        LOGGER.info("Update resources for pods");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
        });

        StUtils.waitForRollingUpdateTimeout(testClass, testName, logZkPattern, timeMeasuringSystem.getOperationID());

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        int received = receiveMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(received, is(sent));

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);

        // Second part
        String rollingUpdateOperation = timeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        StUtils.waitForRollingUpdateTimeout(testClass, testName, logZkPattern, rollingUpdateOperation);

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        received = receiveMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(received, is(sent));

        timeMeasuringSystem.stopOperation(rollingUpdateOperation);
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                    .getZookeeper()
                    .setResources(new ResourceRequirementsBuilder()
                            .addToRequests("cpu", new Quantity("200m"))
                            .build());
        });

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3);

        received = receiveMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(received, is(sent));
    }

    @Test
    void testRecoveryDuringKafkaRollingUpdate() throws Exception {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        int messageCount = 50;
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        String logKafkaPattern = "'Exceeded timeout of .* while waiting for Pods resource " + KafkaResources.kafkaStatefulSetName(CLUSTER_NAME) + ".*'";

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .addToConfig("offsets.topic.replication.factor", 1)
                .endKafka()
            .endSpec().done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName, 2, 3, 1).done();

        int sent = sendMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(sent, is(messageCount));

        LOGGER.info("Update resources for pods");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getKafka()
                .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
        });

        StUtils.waitForRollingUpdateTimeout(testClass, testName, logKafkaPattern, timeMeasuringSystem.getOperationID());

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        int received = receiveMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(received, is(sent));

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);

        // Second part
        String rollingUpdateOperation = timeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        StUtils.waitForRollingUpdateTimeout(testClass, testName, logKafkaPattern, rollingUpdateOperation);

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        received = receiveMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(received, is(sent));

        timeMeasuringSystem.stopOperation(rollingUpdateOperation);
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                    .getKafka()
                    .setResources(new ResourceRequirementsBuilder()
                            .addToRequests("cpu", new Quantity("200m"))
                            .build());
        });

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        received = receiveMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(received, is(sent));
    }

    @Test
    @Tag(ACCEPTANCE)
    @Tag(NODEPORT_SUPPORTED)
    void testKafkaAndZookeeperScaleUpScaleDown() throws Exception {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        int messageCount = 50;
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .addToConfig(singletonMap("default.replication.factor", 1))
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec().done();

        testDockerImagesForKafkaCluster(CLUSTER_NAME, NAMESPACE, 3, 1, false);
        // kafka cluster already deployed

        LOGGER.info("Running kafkaScaleUpScaleDown {}", CLUSTER_NAME);

        final int initialReplicas = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getStatus().getReplicas();

        assertEquals(3, initialReplicas);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName, 3, initialReplicas, initialReplicas).done();

        sendMessagesExternal(NAMESPACE, topicName, messageCount);
        receiveMessagesExternal(NAMESPACE, topicName, messageCount);

        // scale up
        final int scaleTo = initialReplicas + 4;
        final int newPodId = initialReplicas;
        final String newPodName = KafkaResources.kafkaPodName(CLUSTER_NAME,  newPodId);
        LOGGER.info("Scaling up to {}", scaleTo);
        // Create snapshot of current cluster
        String kafkaStsName = kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStsName);
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(scaleTo));
        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(kafkaStsName, scaleTo, kafkaPods);
        LOGGER.info("Scaling to {} finished", scaleTo);

        //Test that the new pod does not have errors or failures in events
        String uid = kubeClient().getPodUid(newPodName);
        List<Event> events = kubeClient().listEvents(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));

        receiveMessagesExternal(NAMESPACE, topicName, messageCount);
        //Test that CO doesn't have any exceptions in log
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSecconds(testClass, testName, timeMeasuringSystem.getOperationID()));

        // scale down
        LOGGER.info("Scaling down to {}", initialReplicas);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.SCALE_DOWN));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(initialReplicas));
        StatefulSetUtils.waitTillSsHasRolled(kafkaStsName, initialReplicas, kafkaPods);
        LOGGER.info("Scaling down to {} finished", initialReplicas);

        final int finalReplicas = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getStatus().getReplicas();
        assertThat(finalReplicas, is(initialReplicas));

        // Test that stateful set has event 'SuccessfulDelete'
        uid = kubeClient().getStatefulSetUid(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        assertThat(kubeClient().listEvents(uid), hasAllOfReasons(SuccessfulDelete));
        //Test that CO doesn't have any exceptions in log
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSecconds(testClass, testName, timeMeasuringSystem.getOperationID()));

        receiveMessagesExternal(NAMESPACE, topicName, messageCount);
    }

    /**
     * This test cover case, when KafkaRoller will not roll Kafka pods, because created topic doesn't meet requirements for roll remaining pods
     * 1. Deploy kafka cluster with 4 pods
     * 2. Create topic with 4 replicas
     * 3. Scale down kafka cluster to 3 replicas
     * 4. Trigger rolling update for Kafka cluster
     * 5. Rolling update will not be performed, because topic which we created had some replicas on deleted pods - manual fix is needed in that case
     */
    @Test
    void testKafkaWontRollUpBecauseTopic() {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 4)
            .editSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec().done();

        LOGGER.info("Running kafkaScaleUpScaleDown {}", CLUSTER_NAME);
        final int initialReplicas = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getStatus().getReplicas();
        assertEquals(4, initialReplicas);

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaTopicResource.topic(CLUSTER_NAME, topicName, 4, 4, 4).done();

        //Test that the new pod does not have errors or failures in events
        String uid = kubeClient().getPodUid(KafkaResources.kafkaPodName(CLUSTER_NAME,  3));
        List<Event> events = kubeClient().listEvents(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));

        //Test that CO doesn't have any exceptions in log
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSecconds(testClass, testName, timeMeasuringSystem.getOperationID()));

        // scale down
        int scaledDownReplicas = 3;
        LOGGER.info("Scaling down to {}", scaledDownReplicas);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.SCALE_DOWN));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(scaledDownReplicas));

        PodUtils.waitUntilPodsCountIsPresent(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), scaledDownReplicas);

        // set annotation to trigger Kafka rolling update
        kubeClient().statefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).cascading(false).edit()
            .editMetadata()
                .addToAnnotations("strimzi.io/manual-rolling-update", "true")
            .endMetadata().done();

        // Wait for first reconciliation
        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);
        // Wait for second reconciliation and check that pods are not rolled
        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);
        Map<String, String> kafkaPodsScaleDown = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        for (Map.Entry<String, String> entry : kafkaPodsScaleDown.entrySet()) {
            assertThat(kafkaPods, hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    @Test
    void testZookeeperScaleUpScaleDown() throws Exception {
        int messageCount = 50;
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        // kafka cluster already deployed
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", CLUSTER_NAME);
        final int initialZkReplicas = kubeClient().getStatefulSet(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)).getStatus().getReplicas();
        assertThat(initialZkReplicas, is(3));

        int sent = sendMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(sent, is(messageCount));

        Map<String, String> zkSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        final int scaleZkTo = initialZkReplicas + 4;
        final List<String> newZkPodNames = new ArrayList<String>() {{
                for (int i = initialZkReplicas; i < scaleZkTo; i++) {
                    add(KafkaResources.zookeeperPodName(CLUSTER_NAME, i));
                }
            }};

        LOGGER.info("Scaling up to {}", scaleZkTo);
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(scaleZkTo));
        int received = receiveMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(received, is(sent));

        zkSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), scaleZkTo, zkSnapshot);
        // check the new node is either in leader or follower state
        KafkaUtils.waitForZkMntr(CLUSTER_NAME, ZK_SERVER_STATE, 0, 1, 2, 3, 4, 5, 6);

        //Test that CO doesn't have any exceptions in log
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSecconds(testClass, testName, timeMeasuringSystem.getOperationID()));

        received = receiveMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(received, is(sent));

        // scale down
        LOGGER.info("Scaling down");
        // Get zk-3 uid before deletion
        String uid = kubeClient().getPodUid(newZkPodNames.get(3));
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.SCALE_DOWN));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(initialZkReplicas));

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), initialZkReplicas, zkSnapshot);

        // Wait for one zk pods will became leader and others follower state
        KafkaUtils.waitForZkMntr(CLUSTER_NAME, ZK_SERVER_STATE, 0, 1, 2);
        received = receiveMessages(messageCount, CLUSTER_NAME, false, topicName, null, defaultKafkaClientsPodName);
        assertThat(received, is(sent));

        //Test that the second pod has event 'Killing'
        assertThat(kubeClient().listEvents(uid), hasAllOfReasons(Killing));
        // Stop measuring
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSecconds(testClass, testName, timeMeasuringSystem.getOperationID()));
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testManualTriggeringRollingUpdate() throws Exception {
        String topicName = "test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        int messageCount = 50;

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
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

        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        String zkName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zkName);

        sendMessagesExternal(NAMESPACE, topicName, messageCount);

        // rolling update for kafka
        LOGGER.info("Annotate Kafka StatefulSet {} with manual rolling update annotation", kafkaName);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE));
        // set annotation to trigger Kafka rolling update
        kubeClient().statefulSet(kafkaName).cascading(false).edit()
            .editMetadata()
                .addToAnnotations("strimzi.io/manual-rolling-update", "true")
            .endMetadata().done();

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(kubeClient().getStatefulSet(kafkaName)
                .getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")), is(true));

        // wait when annotation will be removed
        TestUtils.waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getStatefulSet(kafkaName).getMetadata().getAnnotations() == null
                    || !kubeClient().getStatefulSet(kafkaName).getMetadata().getAnnotations().containsKey("strimzi.io/manual-rolling-update"));

        StatefulSetUtils.waitTillSsHasRolled(kafkaName, 3, kafkaPods);

        // rolling update for zookeeper
        LOGGER.info("Annotate Zookeeper StatefulSet {} with manual rolling update annotation", zkName);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE));

        receiveMessagesExternal(NAMESPACE, topicName, messageCount);
        // set annotation to trigger Zookeeper rolling update
        kubeClient().statefulSet(zkName).cascading(false).edit()
            .editMetadata()
                .addToAnnotations("strimzi.io/manual-rolling-update", "true")
            .endMetadata().done();

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(kubeClient().getStatefulSet(zkName)
                .getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")), is(true));

        // wait when annotation will be removed
        TestUtils.waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getStatefulSet(zkName).getMetadata().getAnnotations() == null
                    || !kubeClient().getStatefulSet(zkName).getMetadata().getAnnotations().containsKey("strimzi.io/manual-rolling-update"));

        StatefulSetUtils.waitTillSsHasRolled(zkName, 3, zkPods);
        receiveMessagesExternal(NAMESPACE, topicName, messageCount);
    }

    void assertThatRollingUpdatedFinished(String rolledComponent, String stableComponent) {
        List<String> podStatuses = kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(rolledComponent)
                        && p.getMetadata().getLabels().containsKey("strimzi.io/kind")
                        && p.getMetadata().getLabels().containsValue("Kafka"))
                .map(p -> p.getStatus().getPhase()).sorted().collect(Collectors.toList());

        assertThat(rolledComponent + " doesn't have any pod in desired state: \"Pending\"", podStatuses.contains("Pending"));

        Map<String, Long> statusCount = podStatuses.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LOGGER.info("{} pods statutes: {}", rolledComponent, statusCount);

        assertThat(statusCount.get("Pending"), is(1L));
        assertThat(statusCount.get("Running"), is(Integer.toUnsignedLong(podStatuses.size() - 1)));

        podStatuses = kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(stableComponent))
                .map(p -> p.getStatus().getPhase()).sorted().collect(Collectors.toList());

        statusCount = podStatuses.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LOGGER.info("{} pods statutes: {}", stableComponent, statusCount);

        assertThat("", statusCount.get("Running"), is(Integer.toUnsignedLong(podStatuses.size())));
    }

    void deployTestSpecificResources() {
        KafkaClientsResource.deployKafkaClients(Constants.KAFKA_CLIENTS).done();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        super.recreateTestEnv(coNamespace, bindingsNamespaces, Constants.CO_OPERATION_TIMEOUT_SHORT);
        deployTestSpecificResources();
    }

    @BeforeEach
    void setKafkaClientsPodName() {
        // Get clients pod name
        defaultKafkaClientsPodName =
                kubeClient().listPodsByPrefixInName(Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT).done();
        deployTestSpecificResources();
    }
}
