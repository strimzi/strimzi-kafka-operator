/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@Tag(ROLLING_UPDATE)
class AlternativeReconcileTriggersST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(RollingUpdateST.class);

    static final String NAMESPACE = "alternative-reconcile-triggers-cluster-test";

    @Test
    void testManualTriggeringRollingUpdate() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        String continuousTopicName = "continuous-topic";
        // 500 messages will take 500 seconds in that case
        int continuousClientsMessageCount = 500;
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3).done();

        String kafkaName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        String zkName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zkName);

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
        KafkaTopicResource.topic(CLUSTER_NAME, continuousTopicName, 3, 3, 2).done();
        String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";
        KafkaClientsResource.producerStrimzi(producerName, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), continuousTopicName, continuousClientsMessageCount, producerAdditionConfiguration).done();
        KafkaClientsResource.consumerStrimzi(consumerName, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), continuousTopicName, continuousClientsMessageCount).done();
        // ##############################

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
        kubeClient().statefulSet(kafkaName).withPropagationPolicy(DeletionPropagation.ORPHAN).edit()
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .done();

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(kubeClient().getStatefulSet(kafkaName)
            .getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE)), is(true));

        StatefulSetUtils.waitTillSsHasRolled(kafkaName, 3, kafkaPods);

        // wait when annotation will be removed
        TestUtils.waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> kubeClient().getStatefulSet(kafkaName).getMetadata().getAnnotations() == null
                || !kubeClient().getStatefulSet(kafkaName).getMetadata().getAnnotations().containsKey(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE));

        // rolling update for zookeeper
        LOGGER.info("Annotate Zookeeper StatefulSet {} with manual rolling update annotation", zkName);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE));

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // set annotation to trigger Zookeeper rolling update
        kubeClient().statefulSet(zkName).withPropagationPolicy(DeletionPropagation.ORPHAN).edit()
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .done();

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(kubeClient().getStatefulSet(zkName)
            .getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE)), is(true));

        StatefulSetUtils.waitTillSsHasRolled(zkName, 3, zkPods);

        // wait when annotation will be removed
        TestUtils.waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> kubeClient().getStatefulSet(zkName).getMetadata().getAnnotations() == null
                || !kubeClient().getStatefulSet(zkName).getMetadata().getAnnotations().containsKey(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE));

        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaTopicResource.topic(CLUSTER_NAME, newTopicName, 1, 1).done();

        internalKafkaClient.setTopicName(newTopicName);
        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

        sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitTillContinuousClientsFinish(producerName, consumerName, NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    /**
     * Scenario:
     *  1. Setup Kafka persistent with 3 replicas
     *  2. Create KafkaUser
     *  3. Run producer and consumer to see if cluster is working
     *  4. Remove cluster CA key
     *  5. Kafka and ZK pods should roll, wiat until rolling update finish
     *  6. Check that CA certificates were renewed
     *  7. Try consumer, producer and consume rmeessages again with new certificates
     */
    @Test
    void testRollingUpdateOnNextReconciliationAfterClusterCAKeyDel() {
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

        String zookeeperDeletedCert = kubeClient(NAMESPACE).getSecret(CLUSTER_NAME + "-zookeeper-nodes").getData().get(CLUSTER_NAME + "-zookeeper-0.crt");
        String kafkaDeletedCert = kubeClient(NAMESPACE).getSecret(CLUSTER_NAME + "-kafka-brokers").getData().get(CLUSTER_NAME + "-kafka-0.crt");

        kubeClient().deleteSecret(KafkaResources.clusterCaKeySecretName(CLUSTER_NAME));
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        assertThat(kubeClient(NAMESPACE).getSecret(CLUSTER_NAME + "-zookeeper-nodes").getData().get(CLUSTER_NAME + "-zookeeper-0.crt"), is(not(zookeeperDeletedCert)));
        assertThat(kubeClient(NAMESPACE).getSecret(CLUSTER_NAME + "-kafka-brokers").getData().get(CLUSTER_NAME + "-kafka-0.crt"), is(not(kafkaDeletedCert)));
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)), is(not(zkPods)));
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)), is(not(kafkaPods)));

        int sentAfter = internalKafkaClient.sendMessagesTls();
        assertThat(sentAfter, is(MESSAGE_COUNT));
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);
    }
}
