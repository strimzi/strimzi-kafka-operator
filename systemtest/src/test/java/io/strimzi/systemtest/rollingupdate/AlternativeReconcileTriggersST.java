/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.cli.annotations.Description;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.ByteArrayInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.ROLLING_UPDATE;
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

    @ParallelNamespaceTest
    @SuppressWarnings("checkstyle:MethodLength")
    void testManualTriggeringRollingUpdate() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // 500 messages will take 500 seconds in that case
        final int continuousClientsMessageCount = 500;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getContinuousTopicName(), 3, 3, 2, testStorage.getNamespaceName()).build());

        String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

        // Add transactional id to make producer transactional
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\ntransactional.id=" + testStorage.getContinuousTopicName() + ".1");
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\nenable.idempotence=true");

        final KafkaClients continuousClients = ClientUtils.getContinuousPlainClientBuilder(testStorage)
            .withMessageCount(continuousClientsMessageCount)
            .withAdditionalConfig(producerAdditionConfiguration)
            .build();

        resourceManager.createResourceWithWait(continuousClients.producerStrimzi(), continuousClients.consumerStrimzi());
        resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage).build());

        KafkaClients instantClients = ClientUtils.getInstantTlsClientBuilder(testStorage).build();
        resourceManager.createResourceWithWait(instantClients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        // rolling update for kafka
        // set annotation to trigger Kafka rolling update
        LOGGER.info("Annotate Kafka {} {} with manual rolling update annotation", StrimziPodSet.RESOURCE_KIND, testStorage.getBrokerComponentName());

        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName())
            .get(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE)), is(true));

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // wait when annotation will be removed from kafka
        TestUtils.waitFor("CO removes rolling update annotation", TestConstants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                () -> StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName()) == null
                        || !StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName()).containsKey(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE));

        resourceManager.createResourceWithWait(instantClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        if (!Environment.isKRaftModeEnabled()) {
            // rolling update for zookeeper
            // set annotation to trigger Zookeeper rolling update
            LOGGER.info("Annotate ZooKeeper: {} - {}/{} with manual rolling update annotation", StrimziPodSet.RESOURCE_KIND, testStorage.getNamespaceName(), testStorage.getControllerComponentName());

            StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName(), Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));

            // check annotation to trigger rolling update
            assertThat(Boolean.parseBoolean(StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName())
                    .get(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE)), is(true));

            RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);

            // wait when annotation will be removed
            TestUtils.waitFor("CO removes rolling update annotation", TestConstants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
                    () -> StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName()) == null
                            || !StrimziPodSetUtils.getAnnotationsOfStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName()).containsKey(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE));
        }

        instantClients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(instantClients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, 1, 1, testStorage.getNamespaceName()).build());

        instantClients = new KafkaClientsBuilder(instantClients)
            .withTopicName(newTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(
            instantClients.producerTlsStrimzi(testStorage.getClusterName()),
            instantClients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        ClientUtils.waitForContinuousClientSuccess(testStorage, continuousClientsMessageCount);
    }

    // This test is affected by https://github.com/strimzi/strimzi-kafka-operator/issues/3913 so it needs longer operation timeout set in CO
    @Description("Test for checking that overriding of bootstrap server, triggers the rolling update and verifying that" +
            " new bootstrap DNS is appended inside certificate in subject alternative names property.")
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testTriggerRollingUpdateAfterOverrideBootstrap() throws CertificateException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String bootstrapDns = "kafka-test.XXXX.azure.XXXX.net";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build());

        final Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            LOGGER.info("Adding new bootstrap dns: {} to external listeners", bootstrapDns);
            kafka.getSpec().getKafka()
                .setListeners(asList(
                    new GenericKafkaListenerBuilder()
                        .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .build(),
                    new GenericKafkaListenerBuilder()
                        .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .withNewConfiguration()
                            .withNewBootstrap()
                                .withAlternativeNames(bootstrapDns)
                            .endBootstrap()
                        .endConfiguration()
                        .build()
                ));
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        KafkaUtils.waitForKafkaReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        Map<String, String> secretData = kubeClient().getSecret(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName())).getData();

        for (Map.Entry<String, String> item : secretData.entrySet()) {
            if (item.getKey().endsWith(".crt") && !item.getKey().contains(testStorage.getControllerComponentName())) {
                LOGGER.info("Encoding {} cert", item.getKey());
                ByteArrayInputStream publicCert = new ByteArrayInputStream(Util.decodeBytesFromBase64(item.getValue().getBytes()));
                CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
                Certificate certificate = certificateFactory.generateCertificate(publicCert);

                LOGGER.info("Verifying that new DNS is in certificate subject alternative names");
                assertThat(certificate.toString(), containsString(bootstrapDns));
            }
        }
    }

    @ParallelNamespaceTest
    void testManualRollingUpdateForSinglePod() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3).build());

        Pod kafkaPod = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0);
        String kafkaPodName = kafkaPod.getMetadata().getName();

        // snapshot of one single Kafka pod
        Map<String, String> kafkaSnapshot = Collections.singletonMap(kafkaPod.getMetadata().getName(), kafkaPod.getMetadata().getUid());
        Map<String, String> zkSnapshot = null;
        if (!Environment.isKRaftModeEnabled()) {
            Pod zkPod = kubeClient(testStorage.getNamespaceName()).getPod(KafkaResources.zookeeperPodName(testStorage.getClusterName(), 0));
            // snapshot of one single ZK pod
            zkSnapshot = Collections.singletonMap(zkPod.getMetadata().getName(), zkPod.getMetadata().getUid());
        }

        LOGGER.info("Trying to roll just single Kafka and single ZK Pod");
        kubeClient(testStorage.getNamespaceName()).editPod(kafkaPodName).edit(pod -> new PodBuilder(pod)
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
            .endMetadata()
            .build());

        // here we are waiting just to one pod's snapshot will be changed and all 3 pods ready -> if we set expectedPods to 1,
        // the check will pass immediately without waiting for all pods to be ready -> the method picks first ready pod and return true
        kafkaSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        if (!Environment.isKRaftModeEnabled()) {
            kubeClient(testStorage.getNamespaceName()).editPod(KafkaResources.zookeeperPodName(testStorage.getClusterName(), 0)).edit(pod -> new PodBuilder(pod)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                .endMetadata()
                .build());

            // same as above
            zkSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, zkSnapshot);
        }

        LOGGER.info("Adding anno to all ZK and Kafka Pods");
        kafkaSnapshot.keySet().forEach(podName -> {
            kubeClient(testStorage.getNamespaceName()).editPod(podName).edit(pod -> new PodBuilder(pod)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                .endMetadata()
                .build());
        });

        LOGGER.info("Checking if the rolling update will be successful for Kafka");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaSnapshot);

        if (!Environment.isKRaftModeEnabled()) {
            zkSnapshot.keySet().forEach(podName -> {
                kubeClient(testStorage.getNamespaceName()).editPod(podName).edit(pod -> new PodBuilder(pod)
                    .editMetadata()
                        .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                    .endMetadata()
                    .build());
            });

            LOGGER.info("Checking if the rolling update will be successful for ZK");
            RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, zkSnapshot);
        }
    }

    /**
     * @description This tests tries to add and remove volume from JBOD.
     *
     * @steps
     *  1. - Persistent Kafka with Jbod storage and 3 replicas is deployed
     *     - Kafka is ready
     *  2. - KafkaTopic for continuous communication including additional properties e.g., 3 replicas and 2 min in sync replicas, is deployed
     *     - KafkaTopic is ready
     *  3. - KafkaTopic for temporary data production and consumption is created as well
     *     - KafkaTopic is ready
     *  4. - Deploy Kafka clients targeting respective KafkaTopics
     *     - Clients are running
     *  5. - Add Jbod volume to Kafka
     *     - Rolling Update on Kafka brokers is triggered and persistent volumes are claimed
     *  5. - Add another Jbod volume to Kafka
     *     - Rolling Update on Kafka brokers is triggered and persistent volumes are claimed, now 2 volumes per each Kafka Broker
     *
     * @usecase
     *  - rolling-update
     *  - jbod
     *  - persistent-volumes
     *  - persistent-volume-claims
     */
    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testAddingAndRemovingJbodVolumes() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // 500 messages will take 500 seconds in that case
        final int continuousClientsMessageCount = 500;

        PersistentClaimStorage vol0 = new PersistentClaimStorageBuilder().withId(0).withSize("1Gi").withDeleteClaim(true).build();
        PersistentClaimStorage vol1 = new PersistentClaimStorageBuilder().withId(1).withSize("1Gi").withDeleteClaim(true).build();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3)
                    .editSpec()
                        .withStorage(new JbodStorageBuilder().addToVolumes(vol0).build())
                    .endSpec()
                    .build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaJBOD(testStorage.getClusterName(), 3, 3, new JbodStorageBuilder().addToVolumes(vol0).build()).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        // ##############################
        // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
        // ##############################
        // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getContinuousTopicName(), 3, 3, 2, testStorage.getNamespaceName()).build());

        String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";
        // Add transactional id to make producer transactional
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\ntransactional.id=" + testStorage.getContinuousTopicName() + ".1");
        producerAdditionConfiguration = producerAdditionConfiguration.concat("\nenable.idempotence=true");

        KafkaClients kafkaBasicClientJob = ClientUtils.getContinuousPlainClientBuilder(testStorage)
            .withMessageCount(continuousClientsMessageCount)
            .withAdditionalConfig(producerAdditionConfiguration)
            .build();

        resourceManager.createResourceWithWait(kafkaBasicClientJob.producerStrimzi(), kafkaBasicClientJob.consumerStrimzi());

        // ##############################
        KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage).build();
        resourceManager.createResourceWithWait(clients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        // Add Jbod volume to Kafka => triggers RU
        LOGGER.info("Add JBOD volume to the Kafka cluster {}", testStorage.getBrokerComponentName());

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), kafkaNodePool -> {
                JbodStorage storage = (JbodStorage) kafkaNodePool.getSpec().getStorage();
                storage.getVolumes().add(vol1);
            }, testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
                JbodStorage storage = (JbodStorage) kafka.getSpec().getKafka().getStorage();
                storage.getVolumes().add(vol1);
            }, testStorage.getNamespaceName());
        }

        // Wait util it rolls
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // ensure there are 6 Kafka Volumes (2 per each of 3 broker)
        var kafkaPvcs = kubeClient().listClaimedPersistentVolumes(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
            .filter(pv -> pv.getSpec().getClaimRef().getName().contains(testStorage.getBrokerComponentName())).collect(Collectors.toList());
        LOGGER.debug("Obtained Volumes total '{}' claimed by claims Belonging to Kafka {}", kafkaPvcs.size(), testStorage.getClusterName());
        assertThat("There are not 6 volumes used by Kafka Cluster", kafkaPvcs.size() == 6);

        // Remove Jbod volume to Kafka => triggers RU
        LOGGER.info("Remove JBOD volume to the Kafka cluster {}", testStorage.getBrokerComponentName());

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), kafkaNodePool -> {
                JbodStorage storage = (JbodStorage) kafkaNodePool.getSpec().getStorage();
                storage.getVolumes().remove(vol1);
            }, testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
                JbodStorage storage = (JbodStorage) kafka.getSpec().getKafka().getStorage();
                storage.getVolumes().remove(vol1);
            }, testStorage.getNamespaceName());
        }

        // Wait util it rolls
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        // ensure there are 3 Kafka Volumes (1 per each of 3 broker)
        PersistentVolumeClaimUtils.waitForPersistentVolumeClaimDeletion(testStorage, 3);
        kafkaPvcs = kubeClient().listClaimedPersistentVolumes(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
            .filter(pv -> pv.getSpec().getClaimRef().getName().contains(testStorage.getBrokerComponentName()) && pv.getStatus().getPhase().equals("Bound")).collect(Collectors.toList());
        LOGGER.debug("Obtained Volumes total '{}' claimed by claims Belonging to Kafka {}", kafkaPvcs.size(), testStorage.getClusterName());
        assertThat("There are not 3 volumes used by Kafka Cluster", kafkaPvcs.size() == 3);

        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForContinuousClientSuccess(testStorage, continuousClientsMessageCount);
        // ##############################
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}

