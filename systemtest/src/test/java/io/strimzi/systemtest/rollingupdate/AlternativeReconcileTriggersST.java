/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
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
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
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

        KafkaResource.kafkaPersistent(clusterName, 3, 3).done();

        String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        String zkName = KafkaResources.zookeeperStatefulSetName(clusterName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zkName);

        KafkaTopicResource.topic(clusterName, topicName).done();
        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
        KafkaTopicResource.topic(clusterName, continuousTopicName, 3, 3, 2).done();
        String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";
        // Add transactional id to make producer transactional
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\ntransactional.id=" + continuousTopicName + ".1");
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\nenable.idempotence=true");

        KafkaBasicExampleClients kafkaBasicClientJob = new KafkaBasicExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(continuousTopicName)
            .withMessageCount(continuousClientsMessageCount)
            .withAdditionalConfig(producerAdditionConfiguration)
            .withDelayMs(1000)
            .build();

        kafkaBasicClientJob.producerStrimzi().done();
        kafkaBasicClientJob.consumerStrimzi().done();
        // ##############################

        String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();
        KafkaUser user = KafkaUserResource.tlsUser(clusterName, userName).done();

        KafkaClientsResource.deployKafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).done();

        final String defaultKafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(userName)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
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

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaTopicResource.topic(clusterName, newTopicName, 1, 1).done();

        internalKafkaClient = internalKafkaClient.toBuilder()
            .withTopicName(newTopicName)
            .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
            .build();

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
        KafkaResource.kafkaPersistent(clusterName, 3, 3).done();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaTopicResource.topic(clusterName, topicName, 2, 2).done();

        KafkaUser user = KafkaUserResource.tlsUser(clusterName, USER_NAME).done();

        KafkaClientsResource.deployKafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).done();
        final String defaultKafkaClientsPodName =
            ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(clusterName));

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        String zookeeperDeletedCert = kubeClient(NAMESPACE).getSecret(clusterName + "-zookeeper-nodes").getData().get(clusterName + "-zookeeper-0.crt");
        String kafkaDeletedCert = kubeClient(NAMESPACE).getSecret(clusterName + "-kafka-brokers").getData().get(clusterName + "-kafka-0.crt");

        kubeClient().deleteSecret(KafkaResources.clusterCaKeySecretName(clusterName));
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(clusterName), 3, zkPods);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);

        assertThat(kubeClient(NAMESPACE).getSecret(clusterName + "-zookeeper-nodes").getData().get(clusterName + "-zookeeper-0.crt"), is(not(zookeeperDeletedCert)));
        assertThat(kubeClient(NAMESPACE).getSecret(clusterName + "-kafka-brokers").getData().get(clusterName + "-kafka-0.crt"), is(not(kafkaDeletedCert)));
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(clusterName)), is(not(zkPods)));
        assertThat(StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName)), is(not(kafkaPods)));

        int sentAfter = internalKafkaClient.sendMessagesTls();
        assertThat(sentAfter, is(MESSAGE_COUNT));
    }

    // This test is affected by https://github.com/strimzi/strimzi-kafka-operator/issues/3913 so it needs longer operation timeout set in CO
    @Description("Test for checking that overriding of bootstrap server, triggers the rolling update and verifying that" +
            " new bootstrap DNS is appended inside certificate in subject alternative names property.")
    @Test
    @Tag(ROLLING_UPDATE)
    void testTriggerRollingUpdateAfterOverrideBootstrap() throws CertificateException {
        String bootstrapDns = "kafka-test.XXXX.azure.XXXX.net";

        KafkaResource.kafkaPersistent(clusterName, 3, 3).done();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));

        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            LOGGER.info("Adding new bootstrap dns: {} to external listeners", bootstrapDns);
            kafka.getSpec().getKafka()
                .setListeners(new ArrayOrObjectKafkaListeners(asList(
                    new GenericKafkaListenerBuilder()
                        .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .build(),
                    new GenericKafkaListenerBuilder()
                        .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withNewConfiguration()
                            .withNewBootstrap()
                                .withAlternativeNames(bootstrapDns)
                            .endBootstrap()
                        .endConfiguration()
                        .build()
                )));
        });

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
        KafkaUtils.waitForKafkaReady(clusterName);

        Map<String, String> secretData = kubeClient().getSecret(KafkaResources.brokersServiceName(clusterName)).getData();

        for (Map.Entry<String, String> item : secretData.entrySet()) {
            if (item.getKey().endsWith(".crt")) {
                LOGGER.info("Encoding {} cert", item.getKey());
                ByteArrayInputStream publicCert = new ByteArrayInputStream(Base64.getDecoder().decode(item.getValue().getBytes()));
                CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
                Certificate certificate = certificateFactory.generateCertificate(publicCert);

                LOGGER.info("Verifying that new DNS is in certificate subject alternative names");
                assertThat(certificate.toString(), containsString(bootstrapDns));
            }
        }
    }

    @Test
    void testManualRollingUpdateForSinglePod() {
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3).done();

        String kafkaSsName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        String zkSsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);

        Pod kafkaPod = kubeClient().getPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        // snapshot of one single Kafka pod
        Map<String, String> kafkaSnapshot = Collections.singletonMap(kafkaPod.getMetadata().getName(), kafkaPod.getMetadata().getUid());

        Pod zkPod = kubeClient().getPod(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0));
        // snapshot of one single ZK pod
        Map<String, String> zkSnapshot = Collections.singletonMap(zkPod.getMetadata().getName(), zkPod.getMetadata().getUid());

        LOGGER.info("Trying to roll just single Kafka and single ZK pod");
        kubeClient().editPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0))
                .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                .endMetadata()
                .done();

        // here we are waiting just to one pod's snapshot will be changed and all 3 pods ready -> if we set expectedPods to 1,
        // the check will pass immediately without waiting for all pods to be ready -> the method picks first ready pod and return true
        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(kafkaSsName, 3, kafkaSnapshot);

        kubeClient().editPod(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0))
                .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                .endMetadata()
                .done();

        // same as above
        zkSnapshot = StatefulSetUtils.waitTillSsHasRolled(zkSsName, 3, zkSnapshot);


        LOGGER.info("Adding anno to all ZK and Kafka pods");
        kafkaSnapshot.keySet().forEach(podName -> {
            kubeClient().editPod(podName)
                    .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                    .endMetadata()
                    .done();
        });

        LOGGER.info("Checking if the rolling update will be successful for Kafka");
        StatefulSetUtils.waitTillSsHasRolled(kafkaSsName, 3, kafkaSnapshot);

        zkSnapshot.keySet().forEach(podName -> {
            kubeClient().editPod(podName)
                    .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                    .endMetadata()
                    .done();
        });

        LOGGER.info("Checking if the rolling update will be successful for ZK");
        StatefulSetUtils.waitTillSsHasRolled(zkSsName, 3, zkSnapshot);
    }

    /**
     * Adding and removing JBOD volumes requires rolling updates in the sequential order. Otherwise the StatefulSet does
     * not like it. This tests tries to add and remove volume from JBOD to test both of these situations.
     */
    @Test
    void testAddingAndRemovingJbodVolumes() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        String clusterName = CLUSTER_NAME + "-jbod-changes";
        String continuousTopicName = "continuous-topic";
        // 500 messages will take 500 seconds in that case
        int continuousClientsMessageCount = 500;
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";

        PersistentClaimStorage vol0 = new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(true).build();
        PersistentClaimStorage vol1 = new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").withDeleteClaim(true).build();

        KafkaResource.kafkaJBOD(clusterName, 3, 3, new JbodStorageBuilder().addToVolumes(vol0).build()).done();

        String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        KafkaTopicResource.topic(clusterName, topicName).done();
        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
        KafkaTopicResource.topic(clusterName, continuousTopicName, 3, 3, 2).done();
        String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";
        // Add transactional id to make producer transactional
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\ntransactional.id=" + continuousTopicName + ".1");
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\nenable.idempotence=true");

        KafkaBasicExampleClients kafkaBasicClientJob = new KafkaBasicExampleClients.Builder()
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withTopicName(continuousTopicName)
                .withMessageCount(continuousClientsMessageCount)
                .withAdditionalConfig(producerAdditionConfiguration)
                .withDelayMs(1000)
                .build();

        kafkaBasicClientJob.producerStrimzi().done();
        kafkaBasicClientJob.consumerStrimzi().done();
        // ##############################

        String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();
        KafkaUser user = KafkaUserResource.tlsUser(clusterName, userName).done();

        KafkaClientsResource.deployKafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).done();

        final String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
                .withUsingPodName(defaultKafkaClientsPodName)
                .withTopicName(topicName)
                .withNamespaceName(NAMESPACE)
                .withClusterName(clusterName)
                .withMessageCount(MESSAGE_COUNT)
                .withKafkaUsername(userName)
                .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
                .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        // Add Jbod volume to Kafka => triggers RU
        LOGGER.info("Add JBOD volume to the Kafka cluster {}", kafkaName);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE));
        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            JbodStorage storage = (JbodStorage) kafka.getSpec().getKafka().getStorage();
            storage.getVolumes().add(vol1);
        });

        // Wait util it rolls
        StatefulSetUtils.waitTillSsHasRolled(kafkaName, 3, kafkaPods);
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        // Remove Jbod volume to Kafka => triggers RU
        LOGGER.info("Remove JBOD volume to the Kafka cluster {}", kafkaName);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE));
        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            JbodStorage storage = (JbodStorage) kafka.getSpec().getKafka().getStorage();
            storage.getVolumes().remove(vol1);
        });

        // Wait util it rolls
        StatefulSetUtils.waitTillSsHasRolled(kafkaName, 3, kafkaPods);

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitTillContinuousClientsFinish(producerName, consumerName, NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);
    }
}

