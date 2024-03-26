/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.common.ProbeBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.VerificationUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.COMPONENT_SCALING;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.ROLLING_UPDATE;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
class RollingUpdateST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RollingUpdateST.class);
    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    /**
     * @description This test case checks recover during Kafka Rolling Update in Zookeeper based Kafka cluster.
     *
     * @steps
     *  1. - Deploy Kafka Cluster with 3 replicas
     *  2. - Deploy Kafka producer and send messages targeting created KafkaTopic
     *  3. - Modify Zookeeper to unreasonable CPU request causing Rolling Update
     *     - One of Zookeeper Pods is in Pending state
     *  4. - Consume messages from KafkaTopic created previously
     *  5. - Modify Zookeeper to reasonable CPU request
     *     - Zookeeper Pods are rolled including previously pending Pod
     *  6. - Modify Kafka with an unreasonable CPU request to trigger a Rolling Update
     *     - One of Kafka Pods is in Pending state
     *  7. - Consume messages from KafkaTopic created previously
     *  8. - Modify Kafka to the reasonable CPU request
     *     - Pods are rolled including previously pending Pod
     *  9. - Create a new KafkaTopic and transmit messages using this topic
     *     - Topic is created and messages transmitted, verifying both Zookeeper and Kafka works
     *
     * @usecase
     *  - kafka
     *  - zookeeper
     *  - rolling-update
     */
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    @KRaftNotSupported("ZooKeeper is not supported by KRaft mode and is used in this test case")
    void testRecoveryDuringZookeeperBasedRollingUpdate() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 2, 2, testStorage.getNamespaceName()).build()
        );

        // produce messages
        KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(clients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        // zookeeper recovery

        LOGGER.info("Update resources for Pods");
        // modify zookeeper resource to unreasonable value
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
        }, testStorage.getNamespaceName());

        // consume messages
        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), testStorage.getControllerComponentName());
        LOGGER.info("Verifying stability of ZooKeeper Pods except the one, which is in pending phase");
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getControllerComponentName());

        LOGGER.info("Verifying stability of Kafka Pods");
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("200m"))
                    .build());
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3);

        // consume messages with newly generated consumer group
        clients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // Kafka recovery

        // change kafka to unreasonable CPU request causing trigger of Rolling update and recover by second modification
        // if kafka node pool is enabled change specification directly in KNP CR as changing it in kafka would have no impact in case it is already specified in KNP
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), knp -> {
                knp.getSpec()
                    .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
            }, testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
                k.getSpec()
                    .getKafka()
                    .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
            }, testStorage.getNamespaceName());
        }

        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());

        // consume messages with newly generated consumer group
        clients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        LOGGER.info("Recover Kafka {}/{} from pending state by modifying its resource request to realistic value", testStorage.getClusterName(), testStorage.getNamespaceName());
        // if kafka node pool is enabled change specification directly in KNP CR as changing it in kafka would have no impact in case it is already specified in KNP
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), knp -> {
                knp.getSpec()
                    .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("200m"))
                        .build());
            }, testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> {
                k.getSpec()
                    .getKafka()
                    .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("200m"))
                        .build());
            }, testStorage.getNamespaceName());
        }

        // Create new topic to ensure, that ZK is working properly
        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(newTopicName)
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .build();

        resourceManager.createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    /**
     * @description This test case checks recover during Kafka Rolling Update in KRaft based Kafka cluster.
     *
     * @steps
     *  1. - Deploy Kafka Cluster with 2 KafkaNodePools,first one with role broker second with role controller
     *  2. - Deploy Kafka producer and send messages targeting created KafkaTopic
     *  3. - Modify controller KafkaNodePool to unreasonable CPU request causing Rolling Update
     *     - One of controller KafkaNodePool Pods is in Pending state
     *  4. - Modify controller KafkaNodePool to som reasonable CPU request
     *     - Pods are rolled including previously pending Pod
     *  5. - Modify broker KafkaNodePool to unreasonable CPU request causing Rolling Update
     *     - One of broker KafkaNodePool Pods is in Pending state
     *  6. - Modify broker KafkaNodePool to som reasonable CPU request
     *     - Pods are rolled including previously pending Pod
     *  7. - Consume messages from KafkaTopic created previously
     *
     * @usecase
     *  - kafka
     *  - kraft
     *  - rolling-update
     */
    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testRecoveryDuringKRaftRollingUpdate() {
        assumeTrue(Environment.isKRaftModeEnabled());
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        // kafka with 1 knp broker and 1 knp controller
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build(),

            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 2, 2, testStorage.getNamespaceName()).build()
        );

        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // change controller knp to unreasonable CPU request causing trigger of Rolling update and recover by second modification
        modifyNodePoolToUnscheduledAndRecover(testStorage.getControllerPoolName(), testStorage.getControllerSelector(), testStorage);

        // change broker knp to unreasonable CPU request causing trigger of Rolling update
        modifyNodePoolToUnscheduledAndRecover(testStorage.getBrokerPoolName(), testStorage.getBrokerSelector(), testStorage);

        clients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
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
     *  4. - Deploy KafkaTopic with 4 replicas and new clients which will target this KafkaTopic and the first one KafkaTopic
     *     - Topic is deployed and ready, clients successfully communicate with respective KafkaTopics
     *  5. - Scale down Kafka Cluster back from 5 to 3 replicas
     *     - Cluster scales down to 3 replicas and volumes as such
     *  6. - Deploy new KafkaTopic and new clients which will target this KafkaTopic, also do the same for the first KafkaTopic
     *     - New KafkaTopic is created and ready, clients successfully communicate with respective KafkaTopics
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
    void testKafkaScaleUpScaleDown() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final String topicNameScaledUp = testStorage.getTopicName() + "-scaled-up";
        final String topicNameScaledBackDown = testStorage.getTopicName() + "-scaled-down";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
            .editMetadata()
                .addToAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true"))
            .endMetadata()
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

        VerificationUtils.verifyClusterOperatorKafkaDockerImages(testStorage.getClusterName(), TestConstants.CO_NAMESPACE, testStorage.getNamespaceName(), 3, false);

        LOGGER.info("Running kafkaScaleUpScaleDown {}", testStorage.getClusterName());

        final int initialReplicas = kubeClient().getClient().pods().inNamespace(testStorage.getNamespaceName()).withLabelSelector(testStorage.getBrokerSelector()).list().getItems().size();
        assertEquals(3, initialReplicas);

        // communicate with topic before scaling up/down

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 3, initialReplicas, initialReplicas, testStorage.getNamespaceName()).build());
        final KafkaClients clientsBeforeScale = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(
            clientsBeforeScale.producerTlsStrimzi(testStorage.getClusterName()),
            clientsBeforeScale.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // scale up
        final int scaleTo = initialReplicas + 2;
        LOGGER.info("Scale up Kafka to {}", scaleTo);

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), knp ->
                knp.getSpec().setReplicas(scaleTo), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
                kafka.getSpec().getKafka().setReplicas(scaleTo);
            }, testStorage.getNamespaceName());
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaleTo);

        LOGGER.info("Kafka scale up to {} finished", scaleTo);

        // consuming data from original topic after scaling up

        LOGGER.info("Consume data produced before scaling up");
        clientsBeforeScale.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(clientsBeforeScale.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // new topic has more replicas than there was available Kafka brokers before scaling up

        LOGGER.info("Create new KafkaTopic with replica count requiring existence of brokers added by scaling up");
        KafkaTopic scaledUpKafkaTopicResource = KafkaTopicTemplates.topic(testStorage.getClusterName(), topicNameScaledUp, testStorage.getNamespaceName())
            .editSpec()
                .withReplicas(initialReplicas + 1)
            .endSpec()
            .build();
        resourceManager.createResourceWithWait(scaledUpKafkaTopicResource);

        LOGGER.info("Produce and consume messages into KafkaTopic {}/{}", testStorage.getNamespaceName(), topicNameScaledUp);
        final KafkaClients clientsAfterScaleUp = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withTopicName(topicNameScaledUp)
            .build();
        resourceManager.createResourceWithWait(
            clientsAfterScaleUp.producerTlsStrimzi(testStorage.getClusterName()),
            clientsAfterScaleUp.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Verify number of PVCs is increased to 5 after scaling Kafka: {}/{} Up to 5 replicas", testStorage.getNamespaceName(), testStorage.getClusterName());
        assertThat((int) kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
            pvc -> pvc.getMetadata().getName().contains(testStorage.getBrokerComponentName())).count(), is(scaleTo));

        // scale down

        LOGGER.info("Scale down Kafka to {}", initialReplicas);
        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(testStorage.getBrokerPoolName(), knp ->
                knp.getSpec().setReplicas(initialReplicas), testStorage.getNamespaceName());
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getKafka().setReplicas(initialReplicas), testStorage.getNamespaceName());
        }

        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), initialReplicas);
        LOGGER.info("Kafka scale down to {} finished", initialReplicas);

        // consuming from original topic (i.e. created before scaling)

        LOGGER.info("Consume data from topic {}/{} where data were produced before scaling up and down", testStorage.getNamespaceName(), testStorage.getTopicName());
        clientsBeforeScale.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(clientsBeforeScale.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        PersistentVolumeClaimUtils.waitForPersistentVolumeClaimDeletion(testStorage, initialReplicas);

        // Create new topic to ensure, that ZK or KRaft is working properly

        LOGGER.info("Creating new KafkaTopic: {}/{} and producing consuming data", testStorage.getNamespaceName(), topicNameScaledBackDown);

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), topicNameScaledBackDown, testStorage.getNamespaceName()).build());
        final KafkaClients clientsTopicAfterScaleDown = ClientUtils.getInstantTlsClientBuilder(testStorage)
            .withTopicName(topicNameScaledBackDown)
            .build();

        resourceManager.createResourceWithWait(
            clientsTopicAfterScaleDown.producerTlsStrimzi(testStorage.getClusterName()),
            clientsTopicAfterScaleDown.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);
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
    void testZookeeperScaleUpScaleDown() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build(),
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        // kafka cluster already deployed
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", testStorage.getClusterName());
        final int initialZkReplicas = kubeClient().getClient().pods().inNamespace(testStorage.getNamespaceName()).withLabelSelector(testStorage.getControllerSelector()).list().getItems().size();
        assertThat(initialZkReplicas, is(3));

        KafkaClients clients = ClientUtils.getInstantTlsClients(testStorage);
        resourceManager.createResourceWithWait(
            clients.producerTlsStrimzi(testStorage.getClusterName()),
            clients.consumerTlsStrimzi(testStorage.getClusterName())
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);

        final int scaleZkTo = initialZkReplicas + 4;
        final List<String> newZkPodNames = new ArrayList<String>() {{
                for (int i = initialZkReplicas; i < scaleZkTo; i++) {
                    add(KafkaResources.zookeeperPodName(testStorage.getClusterName(), i));
                }
            }};

        LOGGER.info("Scale up ZooKeeper to {}", scaleZkTo);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getZookeeper().setReplicas(scaleZkTo), testStorage.getNamespaceName());

        clients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), scaleZkTo);
        // check the new node is either in leader or follower state
        KafkaUtils.waitForZkMntr(testStorage.getNamespaceName(), testStorage.getClusterName(), ZK_SERVER_STATE, 0, 1, 2, 3, 4, 5, 6);


        String newTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        LOGGER.info("Creating new KafkaTopic {}, and producing/consuming data in to to verify Zookeeper handles it after scaling up", newTopicName);
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(newTopicName)
            .build();

        resourceManager.createResourceWithWait(clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String scaleUpTopicName = KafkaTopicUtils.generateRandomNameOfTopic();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), scaleUpTopicName, 1, 1, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(scaleUpTopicName)
            .build();

        resourceManager.createResourceWithWait(clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        // scale down
        LOGGER.info("Scale down ZooKeeper to {}", initialZkReplicas);
        // Get zk-3 uid before deletion
        String uid = kubeClient(testStorage.getNamespaceName()).getPodUid(newZkPodNames.get(3));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), k -> k.getSpec().getZookeeper().setReplicas(initialZkReplicas), testStorage.getNamespaceName());

        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), initialZkReplicas);

        // Wait for one zk pods will became leader and others follower state
        KafkaUtils.waitForZkMntr(testStorage.getNamespaceName(), testStorage.getClusterName(), ZK_SERVER_STATE, 0, 1, 2);

        clients.generateNewConsumerGroup();
        resourceManager.createResourceWithWait(clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        // Create new topic to ensure, that ZK is working properly
        String scaleDownTopicName = KafkaTopicUtils.generateRandomNameOfTopic();
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), scaleDownTopicName, 1, 1, testStorage.getNamespaceName()).build());

        clients = new KafkaClientsBuilder(clients)
            .withTopicName(scaleDownTopicName)
            .build();

        resourceManager.createResourceWithWait(clients.producerTlsStrimzi(testStorage.getClusterName()), clients.consumerTlsStrimzi(testStorage.getClusterName()));
        ClientUtils.waitForInstantClientSuccess(testStorage);

        //Test that the second pod has event 'Killing'
        assertThat(kubeClient(testStorage.getNamespaceName()).listEventsByResourceUid(uid), hasAllOfReasons(Killing));
    }

    @ParallelNamespaceTest
    @Tag(ROLLING_UPDATE)
    void testBrokerConfigurationChangeTriggerRollingUpdate() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = null;

        if (!Environment.isKRaftModeEnabled()) {
            controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());
        }

        // Changes to readiness probe should trigger a rolling update
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(6).build());
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);
        if (!Environment.isKRaftModeEnabled()) {
            assertThat(PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector()), is(controllerPods));
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
    void testClusterOperatorFinishAllRollingUpdates() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(Environment.TEST_SUITE_NAMESPACE, testStorage.getControllerSelector());

        // Changes to readiness probe should trigger a rolling update
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(6).build());
            if (!Environment.isKRaftModeEnabled()) {
                kafka.getSpec().getZookeeper().setReadinessProbe(new ProbeBuilder().withTimeoutSeconds(6).build());
            }
        }, Environment.TEST_SUITE_NAMESPACE);

        TestUtils.waitFor("rolling update starts", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> kubeClient(Environment.TEST_SUITE_NAMESPACE).listPods(Environment.TEST_SUITE_NAMESPACE).stream().filter(pod -> pod.getStatus().getPhase().equals("Running"))
                    .map(pod -> pod.getStatus().getPhase()).toList().size() < kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE).size());

        LabelSelector coLabelSelector = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName()).getSpec().getSelector();
        LOGGER.info("Deleting Cluster Operator Pod with labels {}", coLabelSelector);
        kubeClient(clusterOperator.getDeploymentNamespace()).deletePodsByLabelSelector(coLabelSelector);
        LOGGER.info("Cluster Operator Pod deleted");

        LOGGER.info("Rolling Update is taking place, starting with roll of Zookeeper Pods with labels {}", testStorage.getControllerSelector());
        RollingUpdateUtils.waitTillComponentHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getControllerSelector(), 3, controllerPods);

        LOGGER.info("Wait till first Kafka Pod rolls");
        RollingUpdateUtils.waitTillComponentHasStartedRolling(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector(), brokerPods);

        LOGGER.info("Deleting Cluster Operator Pod with labels {}, while Rolling update rolls Kafka Pods", coLabelSelector);
        kubeClient(clusterOperator.getDeploymentNamespace()).deletePodsByLabelSelector(coLabelSelector);
        LOGGER.info("Cluster Operator Pod deleted");

        LOGGER.info("Wait until Rolling Update finish successfully despite Cluster Operator being deleted in beginning of Rolling Update and also during Kafka Pods rolling");
        RollingUpdateUtils.waitTillComponentHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerSelector(), 3, brokerPods);
    }

    /**
     * @description This test case check that enabling metrics and metrics manipulation triggers Rolling Update.
     *
     * @steps
     *  1. - Deploy Kafka Cluster with Zookeeper and with disabled metrics configuration
     *     - Cluster is deployed
     *  2. - Change specification of Kafka Cluster by configuring metrics for Kafka, Zookeeper, and configuring metrics Exporter
     *     - Allowing metrics does not trigger Rolling Update
     *  3. - Setup or deploy necessary scraper, metric rules, and collectors and collect metrics
     *     - Metrics are successfully collected
     *  4. - Modify patterns in rules for collecting metrics in Zookeeper and Kafka by updating respective Config Maps
     *     - Respective changes do not trigger Rolling Update, Cluster remains stable and metrics are exposed according to new rules
     *  5. - Change specification of Kafka Cluster by removing any metric related configuration
     *     - Rolling Update is triggered and metrics are no longer present.
     *
     * @usecase
     *  - metrics
     *  - kafka-metrics-rolling-update
     *  - rolling-update
     */
    @IsolatedTest
    @Tag(ROLLING_UPDATE)
    @KRaftNotSupported("Zookeeper is not supported by KRaft mode and is used in this test class")
    @SuppressWarnings("checkstyle:MethodLength")
    void testMetricsChange() throws JsonProcessingException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        //Kafka
        Map<String, Object> kafkaRule = new HashMap<>();
        kafkaRule.put("pattern", "kafka.(\\w+)<type=(.+), name=(.+)><>Count");
        kafkaRule.put("name", "kafka_$1_$2_$3_count");
        kafkaRule.put("type", "COUNTER");

        Map<String, Object> kafkaMetrics = new HashMap<>();
        kafkaMetrics.put("lowercaseOutputName", true);
        kafkaMetrics.put("rules", Collections.singletonList(kafkaRule));

        final String metricsCMNameK = "k-metrics-cm";

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final String yaml = mapper.writeValueAsString(kafkaMetrics);
        ConfigMap metricsCMK = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(metricsCMNameK)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(singletonMap("metrics-config.yml", yaml))
            .build();

        JmxPrometheusExporterMetrics kafkaMetricsConfig = new JmxPrometheusExporterMetricsBuilder()
            .withNewValueFrom()
            .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                .withName(metricsCMNameK)
                .withKey("metrics-config.yml")
                .withOptional(true)
                .build())
            .endValueFrom()
            .build();

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

        String metricsCMNameZk = "zk-metrics-cm";
        ConfigMap metricsCMZk = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(metricsCMNameZk)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(singletonMap("metrics-config.yml", mapper.writeValueAsString(zookeeperMetrics)))
            .build();

        JmxPrometheusExporterMetrics zkMetricsConfig = new JmxPrometheusExporterMetricsBuilder()
            .withNewValueFrom()
            .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                .withName(metricsCMNameZk)
                .withKey("metrics-config.yml")
                .withOptional(true)
                .build())
            .endValueFrom()
            .build();

        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), metricsCMK);
        kubeClient().createConfigMapInNamespace(testStorage.getNamespaceName(), metricsCMZk);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withMetricsConfig(kafkaMetricsConfig)
                .endKafka()
                .editOrNewZookeeper()
                    .withMetricsConfig(zkMetricsConfig)
                .endZookeeper()
                .withNewKafkaExporter()
                .endKafkaExporter()
            .endSpec()
            .build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());

        resourceManager.createResourceWithWait(ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());

        final String metricsScraperPodName = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        MetricsCollector kafkaCollector = new MetricsCollector.Builder()
            .withNamespaceName(testStorage.getNamespaceName())
            .withScraperPodName(metricsScraperPodName)
            .withComponentName(testStorage.getClusterName())
            .withComponentType(ComponentType.Kafka)
            .build();

        MetricsCollector zkCollector = kafkaCollector.toBuilder()
            .withComponentType(ComponentType.Zookeeper)
            .build();

        LOGGER.info("Check if metrics are present in Pod of Kafka and ZooKeeper");
        kafkaCollector.collectMetricsFromPods();
        zkCollector.collectMetricsFromPods();

        assertThat(kafkaCollector.getCollectedData().values().toString().contains("kafka_"), is(true));
        assertThat(zkCollector.getCollectedData().values().toString().contains("replicaId"), is(true));

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

        metricsCMZk = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(metricsCMNameZk)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(singletonMap("metrics-config.yml", mapper.writeValueAsString(zookeeperMetrics)))
            .build();

        metricsCMK = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(metricsCMNameK)
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .withData(singletonMap("metrics-config.yml", mapper.writeValueAsString(kafkaMetrics)))
            .build();

        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), metricsCMK);
        kubeClient().updateConfigMapInNamespace(testStorage.getNamespaceName(), metricsCMZk);

        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getControllerComponentName());
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), testStorage.getBrokerComponentName());

        LOGGER.info("Check if Kafka and ZooKeeper Pods didn't roll");
        assertThat(PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector()), is(controllerPods));
        assertThat(PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector()), is(brokerPods));

        LOGGER.info("Check if Kafka and ZooKeeper metrics are changed");
        ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
        String kafkaMetricsConf = kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).withName(metricsCMNameK).get().getData().get("metrics-config.yml");
        String zkMetricsConf = kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).withName(metricsCMNameZk).get().getData().get("metrics-config.yml");
        Object kafkaMetricsJsonToYaml = yamlReader.readValue(kafkaMetricsConf, Object.class);
        Object zkMetricsJsonToYaml = yamlReader.readValue(zkMetricsConf, Object.class);
        ObjectMapper jsonWriter = new ObjectMapper();
        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(testStorage.getNamespaceName(), testStorage.getClusterName())) {
            assertThat(kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).withName(cmName).get().getData().get(
                    TestConstants.METRICS_CONFIG_JSON_NAME),
                is(jsonWriter.writeValueAsString(kafkaMetricsJsonToYaml)));
        }
        assertThat(kubeClient().getClient().configMaps().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.zookeeperMetricsAndLogConfigMapName(testStorage.getClusterName())).get().getData().get(
                TestConstants.METRICS_CONFIG_JSON_NAME),
            is(jsonWriter.writeValueAsString(zkMetricsJsonToYaml)));

        LOGGER.info("Check if metrics are present in Pod of Kafka and ZooKeeper");

        kafkaCollector.collectMetricsFromPods();
        zkCollector.collectMetricsFromPods();

        assertThat(kafkaCollector.getCollectedData().values().toString().contains("kafka_"), is(true));
        assertThat(zkCollector.getCollectedData().values().toString().contains("replicaId"), is(true));

        LOGGER.info("Removing metrics from Kafka and ZooKeeper and setting them to null");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().setMetricsConfig(null);
            kafka.getSpec().getZookeeper().setMetricsConfig(null);
        }, testStorage.getNamespaceName());

        LOGGER.info("Waiting for Kafka and ZooKeeper Pods to roll");
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), 3, controllerPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        LOGGER.info("Check if metrics do not exist in Pods");
        kafkaCollector.collectMetricsFromPodsWithoutWait().values().forEach(value -> assertThat(value, is("")));
        zkCollector.collectMetricsFromPodsWithoutWait().values().forEach(value -> assertThat(value, is("")));
    }

    /**
     * Modifies a Kafka node pool to have an unreasonable CPU request, triggering a rolling update,
     * and then recovers it to a normal state. CPU request is firstly increased, causing single pod
     * to enter a pending state. Afterward wait for the pod to stabilize before reducing the CPU
     * request back to a reasonable amount, allowing the node pool to recover.
     */
    private static void modifyNodePoolToUnscheduledAndRecover(final String controllerPoolName, final LabelSelector controllerPoolSelector, final TestStorage testStorage) {
        // change knp to unreasonable CPU request causing trigger of Rolling update
        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(controllerPoolName,
            knp -> {
                knp
                    .getSpec()
                    .setResources(
                        new ResourceRequirements(null, null, Map.of("cpu", new Quantity("100000m")))
                    );
            },
            testStorage.getNamespaceName());

        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), controllerPoolName));
        LOGGER.info("Verifying stability of {}/{} Pods except the one, which is in pending phase", controllerPoolName, testStorage.getNamespaceName());
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(testStorage.getClusterName(), controllerPoolName));

        KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(controllerPoolName,
            knp -> {
                knp
                    .getSpec()
                    .setResources(
                        new ResourceRequirements(null, null, Map.of("cpu", new Quantity("100m")))
                    );
            },
            testStorage.getNamespaceName());
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), controllerPoolSelector, 3);
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();
    }
}
