/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
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
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
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
import org.junit.jupiter.api.extension.ExtensionContext;

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
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@Tag(ROLLING_UPDATE)
class AlternativeReconcileTriggersST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(AlternativeReconcileTriggersST.class);

    static final String NAMESPACE = "alternative-reconcile-triggers-cluster-test";

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testManualTriggeringRollingUpdate(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String continuousTopicName = "continuous-topic";
        // 500 messages will take 500 seconds in that case
        int continuousClientsMessageCount = 500;
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());

        String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        String zkName = KafkaResources.zookeeperStatefulSetName(clusterName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zkName);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, continuousTopicName, 3, 3, 2).build());
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

        resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerStrimzi().build());
        resourceManager.createResource(extensionContext, kafkaBasicClientJob.consumerStrimzi().build());
        // ##############################

        String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();
        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).build());

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
        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
        // set annotation to trigger Kafka rolling update
        kubeClient().statefulSet(kafkaName).withPropagationPolicy(DeletionPropagation.ORPHAN).edit(sts -> new StatefulSetBuilder(sts)
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .build());

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
        operationId = timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(sent));

        // set annotation to trigger Zookeeper rolling update
        kubeClient().statefulSet(zkName).withPropagationPolicy(DeletionPropagation.ORPHAN).edit(sts -> new StatefulSetBuilder(sts)
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .build());

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

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, newTopicName, 1, 1).build());

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

    // This test is affected by https://github.com/strimzi/strimzi-kafka-operator/issues/3913 so it needs longer operation timeout set in CO
    @Description("Test for checking that overriding of bootstrap server, triggers the rolling update and verifying that" +
            " new bootstrap DNS is appended inside certificate in subject alternative names property.")
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(ROLLING_UPDATE)
    void testTriggerRollingUpdateAfterOverrideBootstrap(ExtensionContext extensionContext) throws CertificateException {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String bootstrapDns = "kafka-test.XXXX.azure.XXXX.net";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());

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

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testManualRollingUpdateForSinglePod(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3).build());

        String kafkaSsName = KafkaResources.kafkaStatefulSetName(clusterName);
        String zkSsName = KafkaResources.zookeeperStatefulSetName(clusterName);

        Pod kafkaPod = kubeClient().getPod(KafkaResources.kafkaPodName(clusterName, 0));
        // snapshot of one single Kafka pod
        Map<String, String> kafkaSnapshot = Collections.singletonMap(kafkaPod.getMetadata().getName(), kafkaPod.getMetadata().getUid());

        Pod zkPod = kubeClient().getPod(KafkaResources.zookeeperPodName(clusterName, 0));
        // snapshot of one single ZK pod
        Map<String, String> zkSnapshot = Collections.singletonMap(zkPod.getMetadata().getName(), zkPod.getMetadata().getUid());

        LOGGER.info("Trying to roll just single Kafka and single ZK pod");
        kubeClient().editPod(KafkaResources.kafkaPodName(clusterName, 0)).edit(pod -> new PodBuilder(pod)
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .build());

        // here we are waiting just to one pod's snapshot will be changed and all 3 pods ready -> if we set expectedPods to 1,
        // the check will pass immediately without waiting for all pods to be ready -> the method picks first ready pod and return true
        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(kafkaSsName, 3, kafkaSnapshot);

        kubeClient().editPod(KafkaResources.zookeeperPodName(clusterName, 0)).edit(pod -> new PodBuilder(pod)
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .build());

        // same as above
        zkSnapshot = StatefulSetUtils.waitTillSsHasRolled(zkSsName, 3, zkSnapshot);


        LOGGER.info("Adding anno to all ZK and Kafka pods");
        kafkaSnapshot.keySet().forEach(podName -> {
            kubeClient().editPod(podName).edit(pod -> new PodBuilder(pod)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                .endMetadata()
                .build());
        });

        LOGGER.info("Checking if the rolling update will be successful for Kafka");
        StatefulSetUtils.waitTillSsHasRolled(kafkaSsName, 3, kafkaSnapshot);

        zkSnapshot.keySet().forEach(podName -> {
            kubeClient().editPod(podName).edit(pod -> new PodBuilder(pod)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                .endMetadata()
                .build());
        });

        LOGGER.info("Checking if the rolling update will be successful for ZK");
        StatefulSetUtils.waitTillSsHasRolled(zkSsName, 3, zkSnapshot);
    }

    /**
     * Adding and removing JBOD volumes requires rolling updates in the sequential order. Otherwise the StatefulSet does
     * not like it. This tests tries to add and remove volume from JBOD to test both of these situations.
     */
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testAddingAndRemovingJbodVolumes(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        String continuousTopicName = "continuous-topic";
        // 500 messages will take 500 seconds in that case
        int continuousClientsMessageCount = 500;
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";

        PersistentClaimStorage vol0 = new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(true).build();
        PersistentClaimStorage vol1 = new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").withDeleteClaim(true).build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaJBOD(clusterName, 3, 3, new JbodStorageBuilder().addToVolumes(vol0).build()).build());

        String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, continuousTopicName, 3, 3, 2).build());
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

        resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerStrimzi().build());
        resourceManager.createResource(extensionContext, kafkaBasicClientJob.consumerStrimzi().build());
        // ##############################

        String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();
        KafkaUser user = KafkaUserTemplates.tlsUser(clusterName, userName).build();

        resourceManager.createResource(extensionContext, user);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, user).build());

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

        String operationId = timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            JbodStorage storage = (JbodStorage) kafka.getSpec().getKafka().getStorage();
            storage.getVolumes().add(vol1);
        });

        // Wait util it rolls
        StatefulSetUtils.waitTillSsHasRolled(kafkaName, 3, kafkaPods);
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaName);

        // Remove Jbod volume to Kafka => triggers RU
        LOGGER.info("Remove JBOD volume to the Kafka cluster {}", kafkaName);

        operationId = timeMeasuringSystem.startTimeMeasuring(Operation.ROLLING_UPDATE, extensionContext.getRequiredTestClass().getName(), extensionContext.getDisplayName());
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
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT);
    }
}

