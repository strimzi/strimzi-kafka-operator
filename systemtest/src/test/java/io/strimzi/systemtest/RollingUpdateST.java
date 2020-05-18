/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePort;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePortBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.MetricsUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.timemeasuring.Operation;
import io.vertx.core.cli.annotations.Description;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
class RollingUpdateST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(RollingUpdateST.class);

    static final String NAMESPACE = "rolling-update-cluster-test";

    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    @Test
    void testRecoveryDuringZookeeperRollingUpdate() throws Exception {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName, 2, 2).done();

        String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();
        final String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();

        assertThat(sent, is(MESSAGE_COUNT));

        LOGGER.info("Update resources for pods");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
        });

        ClientUtils.waitUntilClientReceivedMessagesTls(internalKafkaClient, MESSAGE_COUNT);

        LOGGER.info("Verifying stability of kafka pods except the one, which is in pending phase");
        PodUtils.waitForPendingPod(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        TestUtils.waitFor("Waiting for some zookeeper pod to be in the pending phase because of selected high cpu resource",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> filteredPod = kubeClient().listPodsByPrefixInName(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME))
                        .stream().filter(pod -> pod.getStatus().getPhase().equals("Pending")).collect(Collectors.toList());
                LOGGER.info("Filtered pods are {}", filteredPod.toString());
                return filteredPod.get(0).getStatus().getPhase().equals("Pending");
            }
        );

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        ClientUtils.waitUntilClientReceivedMessagesTls(internalKafkaClient, MESSAGE_COUNT);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build());
        });

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3);

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + new Random().nextInt(Integer.MAX_VALUE));

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = "new-test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        KafkaTopicResource.topic(CLUSTER_NAME, newTopicName, 1, 1).done();

        internalKafkaClient.setTopicName(newTopicName);
        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + new Random().nextInt(Integer.MAX_VALUE));

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));
    }

    @Test
    void testRecoveryDuringKafkaRollingUpdate() throws Exception {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .addToConfig("offsets.topic.replication.factor", 1)
                .endKafka()
            .endSpec().done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName, 2, 3, 1).done();

        String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        final String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        LOGGER.info("Update resources for pods");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getKafka()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
        });

        ClientUtils.waitUntilClientReceivedMessagesTls(internalKafkaClient, MESSAGE_COUNT);

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        TestUtils.waitFor("Waiting for some kafka pod to be in the pending phase because of selected high cpu resource",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                List<Pod> filteredPod = kubeClient().listPodsByPrefixInName(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME))
                    .stream().filter(pod -> pod.getStatus().getPhase().equals("Pending")).collect(Collectors.toList());
                LOGGER.info("Filtered pods are {}", filteredPod.toString());
                return filteredPod.get(0).getStatus().getPhase().equals("Pending");
            }
        );

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        LOGGER.info("Verifying stability of kafka pods except the one, which is in pending phase");
        PodUtils.waitForPendingPod(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        ClientUtils.waitUntilClientReceivedMessagesTls(internalKafkaClient, MESSAGE_COUNT);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getKafka()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build());
        });

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        ClientUtils.waitUntilClientReceivedMessagesTls(internalKafkaClient, MESSAGE_COUNT);

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = "new-test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        KafkaTopicResource.topic(CLUSTER_NAME, newTopicName, 1, 1).done();

        internalKafkaClient.setTopicName(newTopicName);
        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));
        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

    }

    @Test
    @Tag(ACCEPTANCE)
    void testKafkaAndZookeeperScaleUpScaleDown() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .addToConfig(singletonMap("default.replication.factor", 1))
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec().done();

        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        testDockerImagesForKafkaCluster(CLUSTER_NAME, NAMESPACE, 3, 1, false);
        // kafka cluster already deployed

        LOGGER.info("Running kafkaScaleUpScaleDown {}", CLUSTER_NAME);

        final int initialReplicas = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getStatus().getReplicas();

        assertEquals(3, initialReplicas);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName, 3, initialReplicas, initialReplicas).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        final String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // scale up
        final int scaleTo = initialReplicas + 4;
        LOGGER.info("Scale up Kafka to {}", scaleTo);
        // Create snapshot of current cluster
        String kafkaStsName = kafkaStatefulSetName(CLUSTER_NAME);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setReplicas(scaleTo);
        });

        StatefulSetUtils.waitForAllStatefulSetPodsReady(kafkaStsName, scaleTo);
        LOGGER.info("Kafka scale up to {} finished", scaleTo);

        internalKafkaClient.setConsumerGroup(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE));

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(MESSAGE_COUNT));

        //Test that CO doesn't have any exceptions in log
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());

        assertThat((int) kubeClient().listPersistentVolumeClaims().stream().filter(
            pvc -> pvc.getMetadata().getName().contains(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME))).count(), is(scaleTo));

        final int zookeeperScaleTo = initialReplicas + 2;
        LOGGER.info("Scale up Zookeeper to {}", zookeeperScaleTo);
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(zookeeperScaleTo));
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), zookeeperScaleTo);
        LOGGER.info("Kafka scale up to {} finished", zookeeperScaleTo);

        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // scale down
        LOGGER.info("Scale down Kafka to {}", initialReplicas);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.SCALE_DOWN));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(initialReplicas));
        StatefulSetUtils.waitForAllStatefulSetPodsReady(kafkaStsName, initialReplicas);
        LOGGER.info("Kafka scale down to {} finished", initialReplicas);
        //Test that CO doesn't have any exceptions in log
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());

        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        assertThat((int) kubeClient().listPersistentVolumeClaims().stream()
                .filter(pvc -> pvc.getMetadata().getName().contains(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME))).count(), is(initialReplicas));

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = "new-test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        KafkaTopicResource.topic(CLUSTER_NAME, newTopicName, 1, 1).done();

        internalKafkaClient.setTopicName(newTopicName);
        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));
    }


    @Test
    void testKafkaRollsWhenTopicIsUnderReplicated() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
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
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSeconds(testClass, testName, timeMeasuringSystem.getOperationID()));

        // scale down
        int scaledDownReplicas = 3;
        LOGGER.info("Scaling down to {}", scaledDownReplicas);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.SCALE_DOWN));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(scaledDownReplicas));
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), scaledDownReplicas);

        PodUtils.verifyThatRunningPodsAreStable(CLUSTER_NAME);

        // set annotation to trigger Kafka rolling update
        kubeClient().statefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).cascading(false).edit()
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata().done();

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), kafkaPods);
    }

    @Test
    void testZookeeperScaleUpScaleDown() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3).done();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        // kafka cluster already deployed
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", CLUSTER_NAME);
        final int initialZkReplicas = kubeClient().getStatefulSet(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)).getStatus().getReplicas();
        assertThat(initialZkReplicas, is(3));

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        final String defaultKafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        final int scaleZkTo = initialZkReplicas + 4;
        final List<String> newZkPodNames = new ArrayList<String>() {{
                for (int i = initialZkReplicas; i < scaleZkTo; i++) {
                    add(KafkaResources.zookeeperPodName(CLUSTER_NAME, i));
                }
            }};

        LOGGER.info("Scale up Zookeeper to {}", scaleZkTo);
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(scaleZkTo));
        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), scaleZkTo);
        // check the new node is either in leader or follower state
        KafkaUtils.waitForZkMntr(CLUSTER_NAME, ZK_SERVER_STATE, 0, 1, 2, 3, 4, 5, 6);

        //Test that CO doesn't have any exceptions in log
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());

        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // Create new topic to ensure, that ZK is working properly
        String scaleUpTopicName = "new-scale-up-test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        KafkaTopicResource.topic(CLUSTER_NAME, scaleUpTopicName, 1, 1).done();

        internalKafkaClient.setTopicName(scaleUpTopicName);
        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // scale down
        LOGGER.info("Scale down Zookeeper to {}", initialZkReplicas);
        // Get zk-3 uid before deletion
        String uid = kubeClient().getPodUid(newZkPodNames.get(3));
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.SCALE_DOWN));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(initialZkReplicas));

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), initialZkReplicas);

        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));

        // Wait for one zk pods will became leader and others follower state
        KafkaUtils.waitForZkMntr(CLUSTER_NAME, ZK_SERVER_STATE, 0, 1, 2);
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // Create new topic to ensure, that ZK is working properly
        String scaleDownTopicName = "new-scale-down-test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        KafkaTopicResource.topic(CLUSTER_NAME, scaleDownTopicName, 1, 1).done();

        internalKafkaClient.setTopicName(scaleDownTopicName);
        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));
        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        //Test that the second pod has event 'Killing'
        assertThat(kubeClient().listEvents(uid), hasAllOfReasons(Killing));
        // Stop measuring
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    @Test
    void testManualTriggeringRollingUpdate() {
        // This test needs Operation Timetout set to higher value, because manual rolling update work in different way
        kubeClient().deleteDeployment(Constants.STRIMZI_DEPLOYMENT_NAME);
        ResourceManager.setClassResources();
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT).done();
        ResourceManager.setMethodResources();

        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3).done();

        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        String zkName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zkName);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();
        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();

        final String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        // rolling update for kafka
        LOGGER.info("Annotate Kafka StatefulSet {} with manual rolling update annotation", kafkaName);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE));
        // set annotation to trigger Kafka rolling update
        kubeClient().statefulSet(kafkaName).cascading(false).edit()
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata().done();

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(kubeClient().getStatefulSet(kafkaName)
                .getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE)), is(true));

        StatefulSetUtils.waitTillSsHasRolled(kafkaName, 3, kafkaPods);

        // wait when annotation will be removed
        TestUtils.waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getStatefulSet(kafkaName).getMetadata().getAnnotations() == null
                    || !kubeClient().getStatefulSet(kafkaName).getMetadata().getAnnotations().containsKey(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE));

        // rolling update for zookeeper
        LOGGER.info("Annotate Zookeeper StatefulSet {} with manual rolling update annotation", zkName);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE));

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // set annotation to trigger Zookeeper rolling update
        kubeClient().statefulSet(zkName).cascading(false).edit()
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata().done();

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(kubeClient().getStatefulSet(zkName)
                .getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE)), is(true));

        StatefulSetUtils.waitTillSsHasRolled(zkName, 3, zkPods);

        // wait when annotation will be removed
        TestUtils.waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getStatefulSet(zkName).getMetadata().getAnnotations() == null
                    || !kubeClient().getStatefulSet(zkName).getMetadata().getAnnotations().containsKey(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE));


        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = "new-test-topic-" + new Random().nextInt(Integer.MAX_VALUE);
        KafkaTopicResource.topic(CLUSTER_NAME, newTopicName, 1, 1).done();

        internalKafkaClient.setTopicName(newTopicName);
        internalKafkaClient.setConsumerGroup("group" + new Random().nextInt(Integer.MAX_VALUE));

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // deploy Cluster Operator with short timeout for other tests (restore default configuration)
        kubeClient().deleteDeployment(Constants.STRIMZI_DEPLOYMENT_NAME);
        ResourceManager.setClassResources();
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT).done();
        ResourceManager.setMethodResources();
    }

    @Test
    void testBrokerConfigurationChangeTriggerRollingUpdate() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3).done();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setMetrics(singletonMap("kafka.config", "magic-config"));
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)), is(zkPods));
    }

    @Test
    void testManualKafkaConfigMapChangeDontTriggerRollingUpdate() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3).done();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        ConfigMap configMap = kubeClient().getConfigMap(KafkaResources.kafkaMetricsAndLogConfigMapName(CLUSTER_NAME));
        configMap.getData().put("new.kafka.config", "new.config.value");
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMap);

        PodUtils.verifyThatRunningPodsAreStable(CLUSTER_NAME);

        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)), is(zkPods));
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)), is(kafkaPods));
    }

    @Test
    void testExternalLoggingChangeTriggerRollingUpdate() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3).done();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        String loggersConfig = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                "kafka.root.logger.level=INFO\n" +
                "log4j.rootLogger=${kafka.root.logger.level}, CONSOLE\n" +
                "log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n" +
                "log4j.logger.org.apache.zookeeper=INFO\n" +
                "log4j.logger.kafka=INFO\n" +
                "log4j.logger.org.apache.kafka=INFO\n" +
                "log4j.logger.kafka.request.logger=WARN, CONSOLE\n" +
                "log4j.logger.kafka.network.Processor=INFO\n" +
                "log4j.logger.kafka.server.KafkaApis=INFO\n" +
                "log4j.logger.kafka.network.RequestChannel$=INFO\n" +
                "log4j.logger.kafka.controller=INFO\n" +
                "log4j.logger.kafka.log.LogCleaner=INFO\n" +
                "log4j.logger.state.change.logger=TRACE\n" +
                "log4j.logger.kafka.authorizer.logger=INFO";

        String configMapLoggersName = "loggers-config-map";
        ConfigMap configMapLoggers = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(configMapLoggersName)
            .endMetadata()
            .addToData("log4j.properties", loggersConfig)
            .addToData("log4j2.properties", loggersConfig)
            .build();

        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapLoggers);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setLogging(new ExternalLoggingBuilder()
                .withName(configMapLoggersName)
                .build());
            kafka.getSpec().getZookeeper().setLogging(new ExternalLoggingBuilder()
                    .withName(configMapLoggersName)
                    .build());
            kafka.getSpec().getEntityOperator().getTopicOperator().setLogging(new ExternalLoggingBuilder()
                    .withName(configMapLoggersName)
                    .build());
            kafka.getSpec().getEntityOperator().getUserOperator().setLogging(new ExternalLoggingBuilder()
                    .withName(configMapLoggersName)
                    .build());
        });

        zkPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPods);

        configMapLoggers.getData().put("log4j.properties", loggersConfig.replace("INFO", "DEBUG"));
        configMapLoggers.getData().put("log4j2.properties", loggersConfig.replace("INFO", "DEBUG"));
        kubeClient().getClient().configMaps().inNamespace(NAMESPACE).createOrReplace(configMapLoggers);

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPods);
    }

    @Test
    void testClusterOperatorFinishAllRollingUpdates() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3).done();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        // Metrics enabling should trigger rolling update
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setMetrics(singletonMap("something", "changed"));
            kafka.getSpec().getZookeeper().setMetrics(singletonMap("something", "changed"));
        });

        TestUtils.waitFor("rolling update starts", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().listPods().stream().filter(pod -> pod.getStatus().getPhase().equals("Running"))
                    .map(pod -> pod.getStatus().getPhase()).collect(Collectors.toList()).size() < kubeClient().listPods().size());

        LabelSelector coLabelSelector = kubeClient().getDeployment(Constants.STRIMZI_DEPLOYMENT_NAME).getSpec().getSelector();
        LOGGER.info("Deleting Cluster Operator pod with labels {}", coLabelSelector);
        kubeClient().deletePod(coLabelSelector);
        LOGGER.info("Cluster Operator pod deleted");

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);

        TestUtils.waitFor("rolling update starts", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient().listPods().stream().map(pod -> pod.getStatus().getPhase()).collect(Collectors.toList()).contains("Pending"));

        LOGGER.info("Deleting Cluster Operator pod with labels {}", coLabelSelector);
        kubeClient().deletePod(coLabelSelector);
        LOGGER.info("Cluster Operator pod deleted");

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
    }

    @Description("Test for checking that overriding of bootstrap server, triggers the rolling update and verifying that" +
            " new bootstrap DNS is appended inside certificate in subject alternative names property.")
    @Test
    void testTriggerRollingUpdateAfterOverrideBootstrap() throws CertificateException {
        String bootstrapDns = "kafka-test.XXXX.azure.XXXX.net";

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3).done();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {

            LOGGER.info("Adding new bootstrap dns: {} to external listeners", bootstrapDns);
            kafka.getSpec().getKafka().getListeners().setExternal(
                new KafkaListenerExternalNodePortBuilder()
                    .withNewOverrides()
                        .withNewBootstrap()
                            .withAddress(bootstrapDns)
                        .endBootstrap()
                    .endOverrides()
                    .build());
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
        KafkaUtils.waitUntilKafkaCRIsReady(CLUSTER_NAME);

        String bootstrapAddressDns = ((KafkaListenerExternalNodePort) Crds.kafkaOperation(kubeClient().getClient())
                .inNamespace(kubeClient().getNamespace()).withName(CLUSTER_NAME).get().getSpec().getKafka()
                .getListeners().getExternal()).getOverrides().getBootstrap().getAddress();

        Map<String, String> secretData = kubeClient().getSecret(KafkaResources.brokersServiceName(CLUSTER_NAME)).getData();

        for (Map.Entry<String, String> item : secretData.entrySet()) {
            if (item.getKey().endsWith(".crt")) {
                LOGGER.info("Encoding {} cert", item.getKey());
                ByteArrayInputStream publicCert = new ByteArrayInputStream(Base64.getDecoder().decode(item.getValue().getBytes()));
                CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
                Certificate certificate = certificateFactory.generateCertificate(publicCert);

                LOGGER.info("Verifying that new DNS is in certificate subject alternative names");
                assertThat(certificate.toString(), containsString(bootstrapAddressDns));
            }
        }

        LOGGER.info("Verifying that new DNS is inside kafka CR");
        assertThat(bootstrapAddressDns, is(bootstrapDns));
    }

    @Test
    void testClusterCaRemovedTriggersRollingUpdate() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3).done();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaTopicResource.topic(CLUSTER_NAME, topicName, 2, 2).done();

        KafkaUser user = KafkaUserResource.tlsUser(CLUSTER_NAME, USER_NAME).done();

        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();
        final String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .build();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        String zkCrtBeforeAccident = kubeClient(NAMESPACE).getSecret(CLUSTER_NAME + "-zookeeper-nodes").getData().get(CLUSTER_NAME + "-zookeeper-0.crt");
        String kCrtBeforeAccident = kubeClient(NAMESPACE).getSecret(CLUSTER_NAME + "-kafka-brokers").getData().get(CLUSTER_NAME + "-kafka-0.crt");

        // some kind of accident happened and the CA secret has been deleted
        kubeClient().deleteSecret(KafkaResources.clusterCaKeySecretName(CLUSTER_NAME));
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        assertThat(kubeClient(NAMESPACE).getSecret(CLUSTER_NAME + "-zookeeper-nodes").getData().get(CLUSTER_NAME + "-zookeeper-0.crt"), is(not(zkCrtBeforeAccident)));
        assertThat(kubeClient(NAMESPACE).getSecret(CLUSTER_NAME + "-kafka-brokers").getData().get(CLUSTER_NAME + "-kafka-0.crt"), is(not(kCrtBeforeAccident)));
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)), is(not(zkPods)));
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)), is(not(kafkaPods)));

        int sentAfter = internalKafkaClient.sendMessagesTls();
        assertThat(sentAfter, is(MESSAGE_COUNT));
    }

    @Test
    void testMetricsChange() {
        //Kafka
        Map<String, Object> kafkaRule = new HashMap<>();
        kafkaRule.put("pattern", "kafka.(\\w+)<type=(.+), name=(.+)><>Count");
        kafkaRule.put("name", "kafka_$1_$2_$3_count");
        kafkaRule.put("type", "COUNTER");

        Map<String, Object> kafkaMetrics = new HashMap<>();
        kafkaMetrics.put("lowercaseOutputName", true);
        kafkaMetrics.put("rules", Collections.singletonList(kafkaRule));

        //Zookeeper
        Map<String, Object> zookeeperLabels = new HashMap<>();
        zookeeperLabels.put("replicaId", "$2");

        Map<String, Object> zookeeperRule = new HashMap<>();
        zookeeperRule.put("labels", zookeeperLabels);
        zookeeperRule.put("name", "zookeeper_$3");
        zookeeperRule.put("pattern", "org.apache.ZooKeeperService<name0=ReplicatedServer_id(\\d+), name1=replica.(\\d+)><>(\\w+)");

        Map<String, Object> zookeeperMetrics = new HashMap<>();
        zookeeperMetrics.put("lowercaseOutputName", true);
        zookeeperMetrics.put("rules", Collections.singletonList(zookeeperRule));

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
                .editSpec()
                    .editKafka()
                        .withMetrics(kafkaMetrics)
                    .endKafka()
                    .editOrNewZookeeper()
                        .withMetrics(zookeeperMetrics)
                    .endZookeeper()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
                .done();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Check if metrics are present in pod of Kafka and Zookeeper");
        HashMap<String, String> kafkaMetricsOutput = MetricsUtils.collectKafkaPodsMetrics(CLUSTER_NAME);
        HashMap<String, String> zkMetricsOutput = MetricsUtils.collectZookeeperPodsMetrics(CLUSTER_NAME);

        assertThat(kafkaMetricsOutput.values().toString().contains("kafka_"), is(true));
        assertThat(zkMetricsOutput.values().toString().contains("replicaId"), is(true));

        LOGGER.info("Changing metrics to something else");

        kafkaRule.replace("pattern", "kafka.(\\w+)<type=(.+), name=(.+)><>Count",
                "kafka.(\\w+)<type=(.+), name=(.+)Percent\\w*><>MeanRate");
        kafkaRule.replace("name", "kafka_$1_$2_$3_count", "kafka_$1_$2_$3_percent");
        kafkaRule.replace("type", "COUNTER", "GAUGE");

        zookeeperRule.replace("pattern",
                "org.apache.ZooKeeperService<name0=ReplicatedServer_id(\\d+), name1=replica.(\\d+)><>(\\w+)",
                "org.apache.ZooKeeperService<name0=StandaloneServer_port(\\d+)><>(\\w+)");
        zookeeperRule.replace("name", "zookeeper_$3", "zookeeper_$2");
        zookeeperRule.replace("labels", zookeeperLabels, null);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setMetrics(kafkaMetrics);
            kafka.getSpec().getZookeeper().setMetrics(zookeeperMetrics);
        });

        LOGGER.info("Check if Kafka and Zookeeper pods doesn't rolled");
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)), is(zkPods));
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)), is(kafkaPods));

        LOGGER.info("Check if Kafka and Zookeeper metrics are changed");
        assertThat(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getKafka().getMetrics(), is(kafkaMetrics));
        assertThat(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getZookeeper().getMetrics(), is(zookeeperMetrics));

        LOGGER.info("Check if metrics are present in pod of Kafka and Zookeeper");

        kafkaMetricsOutput = MetricsUtils.collectKafkaPodsMetrics(CLUSTER_NAME);
        zkMetricsOutput = MetricsUtils.collectZookeeperPodsMetrics(CLUSTER_NAME);

        assertThat(kafkaMetricsOutput.values().toString().contains("kafka_"), is(true));
        assertThat(zkMetricsOutput.values().toString().contains("replicaId"), is(true));

        LOGGER.info("Removing metrics from Kafka and Zookeeper and setting them to null");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setMetrics(null);
            kafka.getSpec().getZookeeper().setMetrics(null);
        });

        LOGGER.info("Wait if Kafka and Zookeeper pods will roll");
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Check if metrics are not existing in pods");

        kafkaMetricsOutput = MetricsUtils.collectKafkaPodsMetrics(CLUSTER_NAME);
        zkMetricsOutput = MetricsUtils.collectZookeeperPodsMetrics(CLUSTER_NAME);

        kafkaMetricsOutput.values().forEach(value -> assertThat(value, is("")));
        zkMetricsOutput.values().forEach(value -> assertThat(value, is("")));
    }

    @Test
    void testKafkaTopicRFLowerThanMinInSyncReplicas() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 2).done();
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME, 1, 1).done();

        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        LOGGER.info("Setting KafkaTopic's min.insync.replicas to be higher than replication factor");
        KafkaTopicResource.replaceTopicResource(TOPIC_NAME, kafkaTopic -> kafkaTopic.getSpec().getConfig().replace("min.insync.replicas", 2));

        // rolling update for kafka
        LOGGER.info("Annotate Kafka StatefulSet {} with manual rolling update annotation", kafkaName);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE));
        // set annotation to trigger Kafka rolling update
        kubeClient().statefulSet(kafkaName).cascading(false).edit()
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata().done();

        StatefulSetUtils.waitTillSsHasRolled(kafkaName, 2, kafkaPods);
        assertThat(StatefulSetUtils.ssSnapshot(kafkaName), is(not(kafkaPods)));
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        super.recreateTestEnv(coNamespace, bindingsNamespaces, Constants.CO_OPERATION_TIMEOUT_SHORT);
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT).done();
    }
}
