/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;


import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.COMPONENT_SCALING;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.ROLLING_UPDATE;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
class RollingUpdateST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RollingUpdateST.class);
    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @KRaftNotSupported("ZooKeeper is not supported by KRaft mode and is used in this test case")
    void testRecoveryDuringZookeeperRollingUpdate(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 2, 2, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        LOGGER.info("Update resources for Pods");

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
        }, testStorage.getNamespaceName());

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());
        LOGGER.info("Verifying stability of ZooKeeper Pods except the one, which is in pending phase");
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());

        LOGGER.info("Verifying stability of Kafka Pods");
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build());
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 3);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(newTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testRecoveryDuringKafkaRollingUpdate(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 2, 2, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForProducerClientSuccess(testStorage);

        LOGGER.info("Update resources for Pods");

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), knp ->
                knp.getSpec().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build()), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
                k.getSpec()
                    .getKafka()
                    .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
            }, testStorage.getNamespaceName());
        }

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());
        LOGGER.info("Verifying stability of Kafka Pods except the one, which is in pending phase");
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Verifying stability of ZooKeeper Pods");
            PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());
        }

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), knp ->
                knp.getSpec().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build()), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
                k.getSpec()
                    .getKafka()
                    .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("200m"))
                        .build());
            }, testStorage.getNamespaceName());
        }

        // This might need to wait for the previous reconciliation to timeout and for the KafkaRoller to timeout.
        // Therefore we use longer timeout.
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(newTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    /**
     * @description This test case checks scaling Kafka up and down and that it works correctly during this event.
     *
     * @steps
     *  1. - Deploy persistent Kafka Cluster with 3 replicas, and also first kafkaTopic with 3 replicas
     *     - Cluster with 3 replicas and Kafka topics are deployed
     *  2. - Deploy Kafka clients, produce and consume messages targeting created KafkaTopic
     *     - Data are produced and consumed successfully
     *  3. - Scale up Kafka Cluster from 3 to 5 replicas
     *     - Cluster scales to 5 replicas and volumes as such
     *  4. - Deploy KafkaTopic with 4 replicas and new clients which will target this KafkaTopic
     *     - Topic is deployed and ready, clients successfully communicate with topic represented by mentioned KafkaTopic
     *  5. - Scale down Kafka Cluster back from 5 to 3 replicas
     *     - Cluster scales down to 3 replicas and volumes as such
     *  6. - Deploy new KafkaTopic and new clients which will target this KafkaTopic, also do the same for the first KafkaTopic
     *     - New KafkaTopic is created and ready, all clients can communicate successfully
     *
     * @usecase
     *  - kafka
     *  - scale-up
     *  - scale-down
     */
    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    @Tag(COMPONENT_SCALING)
    @KRaftNotSupported("The scaling of the Kafka Pods is not working properly at the moment, topic with extra operators will also need a workaround")
    void testKafkaScaleUpScaleDown(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
            .editSpec()
                .editKafka()
                    // Topic Operator doesn't support KRaft, yet, using auto topic creation and default replication factor as workaround
                    .addToConfig(singletonMap("default.replication.factor", Environment.isKRaftModeEnabled() ? 3 : 1))
                    .addToConfig("auto.create.topics.enable", Environment.isKRaftModeEnabled())
                .endKafka()
            .endSpec()
            .build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        testDockerImagesForKafkaCluster(testStorage.getClusterName(), Constants.CO_NAMESPACE, testStorage.getNamespaceName(), 3, 3, false);

        LOGGER.info("Running kafkaScaleUpScaleDown {}", testStorage.getClusterName());

        final int initialReplicas = kubeClient().getClient().pods().inNamespace(testStorage.getNamespaceName()).withLabelSelector(testStorage.getKafkaSelector()).list().getItems().size();
        assertEquals(3, initialReplicas);

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 3, initialReplicas, initialReplicas, testStorage.getNamespaceName()).build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        // scale up
        final int scaleTo = initialReplicas + 2;
        LOGGER.info("Scale up Kafka to {}", scaleTo);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), knp ->
                knp.getSpec().setReplicas(scaleTo), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
                kafka.getSpec().getKafka().setReplicas(scaleTo);
            }, testStorage.getNamespaceName());
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), scaleTo);

        LOGGER.info("Kafka scale up to {} finished", scaleTo);

        LOGGER.info("Consume data produced before scaling up");
        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        LOGGER.info("Create new KafkaTopic with replica count requiring existence of brokers added by scaling up");
        String scaleUpTopicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaTopic scaleUpKafkaTopicResource = KafkaTopicTemplates.topic(testStorage.getClusterName(), scaleUpTopicName, testStorage.getNamespaceName())
            .editSpec()
            .withReplicas(4)
            .endSpec()
            .build();
        resourceManager.createResourceWithWait(extensionContext, scaleUpKafkaTopicResource);

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(scaleUpTopicName)
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        resourceManager.deleteResource(scaleUpKafkaTopicResource);

        assertThat((int) kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
            pvc -> pvc.getMetadata().getName().contains(testStorage.getKafkaStatefulSetName())).count(), is(scaleTo));

        // scale down
        LOGGER.info("Scale down Kafka to {}", initialReplicas);
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getKafkaNodePoolName(), knp ->
                knp.getSpec().setReplicas(initialReplicas), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getKafka().setReplicas(initialReplicas), testStorage.getNamespaceName());
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), initialReplicas);

        LOGGER.info("Kafka scale down to {} finished", initialReplicas);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        PersistentVolumeClaimUtils.waitForPersistentVolumeClaimDeletion(testStorage, initialReplicas);

        // Create new topic to ensure, that ZK or KRaft is working properly
        String scaleDownTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), scaleDownTopicName, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(scaleDownTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    /**
     * @description This test case checks scaling Zookeeper up and down and that it works correctly during and after this event.
     *
     * @steps
     *  1. - Deploy persistent Kafka Cluster with 3 replicas, first KafkaTopic, and KafkaUser
     *     - Cluster with 3 replicas and other resources are created and ready
     *  2. - Deploy Kafka clients, produce and consume messages targeting created KafkaTopic
     *     - Data are produced and consumed successfully
     *  3. - Scale up Zookeeper Cluster from 3 to 7 replicas
     *     - Cluster scales to 7 replicas, quorum is temporarily lost but regained afterwards
     *  4. - Deploy new KafkaTopic and new clients which will target this KafkaTopic, and also first KafkaTopic
     *     - New KafkaTopic is deployed and ready, clients successfully communicate with topics represented by mentioned KafkaTopics
     *  5. - Scale down Zookeeper Cluster back from 7 to 3 replicas
     *     - Cluster scales down to 3, Zookeeper
     *  6. - Deploy new KafkaTopic and new clients targeting it
     *     - New KafkaTopic is created and ready, all clients can communicate successfully
     *
     * @usecase
     *  - zookeeper
     *  - scale-up
     *  - scale-down
     */
    @ParallelNamespaceTest
    @Tag(COMPONENT_SCALING)
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test case")
    void testZookeeperScaleUpScaleDown(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build(),
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        // kafka cluster already deployed
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", testStorage.getClusterName());
        final int initialZkReplicas = kubeClient().getClient().pods().inNamespace(testStorage.getNamespaceName()).withLabelSelector(testStorage.getZookeeperSelector()).list().getItems().size();
        assertThat(initialZkReplicas, is(3));

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        final int scaleZkTo = initialZkReplicas + 4;
        final List<String> newZkPodNames = new ArrayList<String>() {{
                for (int i = initialZkReplicas; i < scaleZkTo; i++) {
                    add(KafkaResources.zookeeperPodName(testStorage.getClusterName(), i));
                }
            }};

        LOGGER.info("Scale up ZooKeeper to {}", scaleZkTo);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getZookeeper().setReplicas(scaleZkTo), testStorage.getNamespaceName());

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), scaleZkTo);
        // check the new node is either in leader or follower state
        KafkaUtils.waitForZkMntr(testStorage.getNamespaceName(), testStorage.getClusterName(), ZK_SERVER_STATE, 0, 1, 2, 3, 4, 5, 6);


        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        LOGGER.info("Creating new KafkaTopic {}, and producing/consuming data in to to verify Zookeeper handles it after scaling up", newTopicName);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(newTopicName)
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String scaleUpTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), scaleUpTopicName, 1, 1, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(scaleUpTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        // scale down
        LOGGER.info("Scale down ZooKeeper to {}", initialZkReplicas);
        // Get zk-3 uid before deletion
        String uid = kubeClient(testStorage.getNamespaceName()).getPodUid(newZkPodNames.get(3));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getZookeeper().setReplicas(initialZkReplicas), testStorage.getNamespaceName());

        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), initialZkReplicas);

        clients = new KafkaClientsBuilder(clients)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        // Wait for one zk pods will became leader and others follower state
        KafkaUtils.waitForZkMntr(testStorage.getNamespaceName(), testStorage.getClusterName(), ZK_SERVER_STATE, 0, 1, 2);

        resourceManager.createResourceWithWait(extensionContext, clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String scaleDownTopicName = KafkaTopicUtils.generateRandomNameOfTopic();
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), scaleDownTopicName, 1, 1, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(scaleDownTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(extensionContext, clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForClientsSuccess(testStorage);

        //Test that the second pod has event 'Killing'
        assertThat(kubeClient(testStorage.getNamespaceName()).listEventsByResourceUid(uid), hasAllOfReasons(Killing));
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testBrokerConfigurationChangeTriggerRollingUpdate(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, extensionContext);
        final String clusterName = testStorage.getClusterName();
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        Map<String, String> zkPods = null;
        if (!Environment.isKRaftModeEnabled()) {
            zkPods = PodUtils.podSnapshot(namespaceName, zkSelector);
        }

        // Changes to readiness probe should trigger a rolling update
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(6).build());
        }, namespaceName);

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaPods);
        if (!Environment.isKRaftModeEnabled()) {
            assertThat(PodUtils.podSnapshot(namespaceName, zkSelector), is(zkPods));
        }
    }

    /**
     * @description This test case verifies that cluster operator can finish rolling update of components despite being restarted.
     *
     * @steps
     *  1. - Deploy persistent Kafka Cluster with 3 replicas
     *     - Cluster with 3 replicas is deployed and ready
     *  2. - Change specification of readiness probe inside Kafka Cluster, thereby triggering Rolling Update
     *     - Rolling Update is triggered
     *  3. - Delete cluster operator Pod
     *     - Cluster operator Pod is restarted and Rolling Update continues
     *  4. - Delete cluster operator Pod again, this time in the middle of Kafka Pods being rolled
     *     - Cluster operator Pod is restarted and Rolling Update finish successfully
     *
         * @usecase
     *  - rolling-update
     *  - cluster-operator
     */
    @IsolatedTest("Deleting Pod of Shared Cluster Operator")
    @Tag(ROLLING_UPDATE)
    void testClusterOperatorFinishAllRollingUpdates(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String clusterName = testStorage.getClusterName();
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, kafkaSelector);
        Map<String, String> zkPods = null;
        if (!Environment.isKRaftModeEnabled()) {
            zkPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, zkSelector);
        }

        // Changes to readiness probe should trigger a rolling update
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(6).build());
            if (!Environment.isKRaftModeEnabled()) {
                kafka.getSpec().getZookeeper().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(6).build());
            }
        }, Environment.TEST_SUITE_NAMESPACE);

        TestUtils.waitFor("rolling update starts", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient(Environment.TEST_SUITE_NAMESPACE).listPods(Environment.TEST_SUITE_NAMESPACE).stream().filter(pod -> pod.getStatus().getPhase().equals("Running"))
                    .map(pod -> pod.getStatus().getPhase()).collect(Collectors.toList()).size() < kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE).size());

        LabelSelector coLabelSelector = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName()).getSpec().getSelector();
        LOGGER.info("Deleting Cluster Operator Pod with labels {}", coLabelSelector);
        kubeClient(clusterOperator.getDeploymentNamespace()).deletePodsByLabelSelector(coLabelSelector);
        LOGGER.info("Cluster Operator Pod deleted");

        LOGGER.info("Rolling Update is taking place, starting with roll of Zookeeper Pods with labels {}", zkSelector);
        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolled(Environment.TEST_SUITE_NAMESPACE, zkSelector, 3, zkPods);
        }
        LOGGER.info("Wait till first Kafka Pod rolls");
        RollingUpdateUtils.waitTillComponentHasStartedRolling(Environment.TEST_SUITE_NAMESPACE, kafkaSelector, kafkaPods);

        LOGGER.info("Deleting Cluster Operator Pod with labels {}, while Rolling update rolls Kafka Pods", coLabelSelector);
        kubeClient(clusterOperator.getDeploymentNamespace()).deletePodsByLabelSelector(coLabelSelector);
        LOGGER.info("Cluster Operator Pod deleted");

        LOGGER.info("Wait until Rolling Update finish successfully despite Cluster Operator being deleted in beginning of Rolling Update and also during Kafka Pods rolling");
        RollingUpdateUtils.waitTillComponentHasRolled(Environment.TEST_SUITE_NAMESPACE, kafkaSelector, 3, kafkaPods);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation(extensionContext)
                .createInstallation()
                .runInstallation();
    }
}
