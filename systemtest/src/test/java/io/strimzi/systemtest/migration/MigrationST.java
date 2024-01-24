/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.migration;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Map;

import static io.strimzi.systemtest.TestConstants.MIGRATION;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(MIGRATION)
public class MigrationST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MigrationST.class);
    private static final String CONTINUOUS_SUFFIX = "-continuous";

    /**
     * @description This testcase is focused on migration process from ZK to KRaft.
     * It goes through whole process, together with checking that message transmission throughout the test will not be
     * disrupted.
     *
     * @steps
     *  1. - Deploys Kafka resource (with enabled NodePools and KRaft set to disabled) with Broker NodePool
     *  2. - Deploys Controller NodePool - it's created here, so we will firstly delete KafkaTopics in our ResourceManager;
     *       the Pods will not be created until the migration starts
     *  3. - Creates topics for continuous and immediate message transmission, TLS user
     *  4. - Starts continuous producer & consumer
     *  5. - Does immediate message transmission
     *  6. - Starts the migration
     *  7. - Annotating the Kafka resource with strimzi.io/kraft:migration
     *  8. - Controllers will be created and moved to RUNNING state
     *  9. - Two rolling updates of the Broker Pods
     *  10. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftPostMigration
     *  11. - Creates a new KafkaTopic and checks both ZK and KRaft metadata for presence of the KafkaTopic
     *  12. - Does immediate message transmission to the new KafkaTopic
     *  13. - Finishes the migration - annotates the Kafka resource with strimzi.io/kraft:enabled
     *  14. - Controller Pods will be rolled
     *  15. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaft
     *  16. - Removes LMFV and IBPV from Kafka configuration
     *  17. - Broker and Controller Pods will be rolled
     *  18. - Creates a new KafkaTopic and checks KRaft metadata for presence of the KafkaTopic
     *  19. - Does immediate message transmission to the new KafkaTopic
     *  20. - Waits until continuous clients are finished successfully
     *
     * @usecase
     *  - zk-to-kraft-migration
     */
    @IsolatedTest
    @SuppressWarnings("checkstyle:MethodLength")
    void testMigrationFromZkToKRaft(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext, TestConstants.CO_NAMESPACE);

        String midStepTopicName = testStorage.getTopicName() + "-midstep";
        String kraftTopicName = testStorage.getTopicName() + "-kraft";

        String continuousTopicName = testStorage.getTopicName() + CONTINUOUS_SUFFIX;
        String continuousProducerName = testStorage.getProducerName() + CONTINUOUS_SUFFIX;
        String continuousConsumerName = testStorage.getConsumerName() + CONTINUOUS_SUFFIX;
        int continuousMessageCount = 500;

        LabelSelector brokerSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getBrokerPoolName(), ProcessRoles.BROKER);
        LabelSelector controllerSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getControllerPoolName(), ProcessRoles.CONTROLLER);

        String clientsAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000\nacks=all";

        KafkaClients immediateClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .build();

        KafkaClients continuousClients = new KafkaClientsBuilder()
            .withProducerName(continuousProducerName)
            .withConsumerName(continuousConsumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(continuousTopicName)
            .withMessageCount(continuousMessageCount)
            .withDelayMs(1000)
            .withAdditionalConfig(clientsAdditionConfiguration)
            .build();

        LOGGER.info("Deploying Kafka resource with Broker NodePool");

        // create Kafka resource with ZK and Broker NodePool
        resourceManager.createResourceWithWait(extensionContext,
            KafkaNodePoolTemplates.kafkaNodePoolWithBrokerRoleAndPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled")
                .endMetadata()
                .editSpec()
                    .editOrNewKafka()
                        .addToConfig("default.replication.factor", 3)
                        .addToConfig("min.insync.replicas", 2)
                    .endKafka()
                .endSpec()
                .build());

        Map<String, String> brokerPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), brokerSelector);

        // the controller pods will not be up and running, because we are using the ZK nodes as controllers, they will be created once the migration starts
        // creating it here (before KafkaTopics) to correctly delete KafkaTopics and prevent stuck because UTO cannot connect to controllers
        resourceManager.createResourceWithoutWait(extensionContext,
            KafkaNodePoolTemplates.kafkaNodePoolWithControllerRoleAndPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build());

        // at this moment, everything should be ready, so we should ideally create some topics and send + receive the messages (to have some data present in Kafka + metadata about topics in ZK)
        LOGGER.info("Creating two topics for immediate and continuous message transmission and KafkaUser for the TLS");
        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(testStorage).build(),
            KafkaTopicTemplates.topic(testStorage.getClusterName(), continuousTopicName, 3, 3, 2, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        // sanity check that kafkaMetadataState shows ZooKeeper
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.ZooKeeper.name());

        // start continuous clients and do the immediate message transmission
        resourceManager.createResourceWithWait(extensionContext,
            continuousClients.producerStrimzi(),
            continuousClients.consumerStrimzi(),
            immediateClients.producerTlsStrimzi(testStorage.getClusterName()),
            immediateClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        // starting the migration
        LOGGER.info("Starting the migration process");

        LOGGER.info("1. applying the {} annotation with value: {}", Annotations.ANNO_STRIMZI_IO_KRAFT, "migration");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration"), testStorage.getNamespaceName());

        LOGGER.info("2. waiting for controller Pods to be up and running");
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, true);
        Map<String, String> controllerPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), controllerSelector);

        LOGGER.info("3. waiting for first rolling update of broker Pods - bringing to DualWrite mode");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), brokerSelector, brokerPodsSnapshot);

        LOGGER.info("4. waiting for second rolling update of broker Pods - removing dependency on ZK");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);

        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaftPostMigration.name());

        LOGGER.info("5. creating KafkaTopic: {} and checking if the metadata are in both ZK and KRaft", midStepTopicName);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), midStepTopicName, testStorage.getNamespaceName()).build());

        LOGGER.info("5a. checking if metadata about KafkaTopic: {} are in ZK", midStepTopicName);

        assertThatTopicIsPresentInZKMetadata(testStorage.getZookeeperSelector(), midStepTopicName);

        LOGGER.info("5b. checking if metadata about KafkaTopic: {} are in KRaft controller", midStepTopicName);

        assertThatTopicIsPresentInKRaftMetadata(controllerSelector, midStepTopicName);

        LOGGER.info("5c. checking if we are able to do a message transmission on KafkaTopic: {}", midStepTopicName);

        immediateClients = new KafkaClientsBuilder(immediateClients)
            .withTopicName(midStepTopicName)
            .build();

        resourceManager.createResourceWithWait(extensionContext,
            immediateClients.producerTlsStrimzi(testStorage.getClusterName()),
            immediateClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        LOGGER.info("6. finishing migration - applying {}: enabled annotation, controllers should be rolled", Annotations.ANNO_STRIMZI_IO_KRAFT);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"), testStorage.getNamespaceName());

        controllerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerPodsSnapshot);

        LOGGER.info("7. ZK related resources should be removed, waiting for SPS to be removed");

        waitForZooKeeperResourcesDeletion(testStorage);

        LOGGER.info("8. Everything related to ZK is deleted, waiting until .status.kafkaMetadataState in Kafka will contain KRaft state");

        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaft.name());

        LOGGER.info("9. removing LMFV and IBPV from Kafka config -> brokers and controllers should be rolled");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().getConfig().remove("log.message.format.version");
            kafka.getSpec().getKafka().getConfig().remove("inter.broker.protocol.version");
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerPodsSnapshot);

        LOGGER.info("10. creating KafkaTopic: {} and checking if the metadata are only in KRaft", kraftTopicName);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), kraftTopicName, testStorage.getNamespaceName()).build());

        LOGGER.info("10a. checking if metadata about KafkaTopic: {} are in KRaft controller", kraftTopicName);

        assertThatTopicIsPresentInKRaftMetadata(controllerSelector, kraftTopicName);

        LOGGER.info("10b. checking if we are able to do a message transmission on KafkaTopic: {}", kraftTopicName);

        immediateClients = new KafkaClientsBuilder(immediateClients)
            .withTopicName(kraftTopicName)
            .build();

        resourceManager.createResourceWithWait(extensionContext,
            immediateClients.producerTlsStrimzi(testStorage.getClusterName()),
            immediateClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());

        LOGGER.info("11. migration is completed, waiting for continuous clients to finish");
        ClientUtils.waitForClientsSuccess(continuousProducerName, continuousConsumerName, testStorage.getNamespaceName(), continuousMessageCount);
    }

    void waitForZooKeeperResourcesDeletion(TestStorage testStorage) {
        cmdKubeClient().namespace(testStorage.getNamespaceName())
            .execInCurrentNamespace(Level.INFO, "wait", "--for", "delete", "networkpolicy,serviceaccount,service,secret,configmap,pdb,sps,pvc,pod",
                "-l", Labels.STRIMZI_NAME_LABEL + "=" + testStorage.getZookeeperStatefulSetName(), "--timeout=300s");
    }

    void assertThatTopicIsPresentInZKMetadata(LabelSelector zkSelector, String topicName) {
        String zkPodName = kubeClient().listPods(zkSelector).get(0).getMetadata().getName();
        String commandOutput = cmdKubeClient().execInPod(zkPodName, "./bin/zookeeper-shell.sh", "localhost:12181", "ls", "/brokers/topics").out().trim();

        assertThat(String.join("KafkaTopic: %s is not present ZK metadata", topicName), commandOutput.contains(topicName), is(true));
    }

    void assertThatTopicIsPresentInKRaftMetadata(LabelSelector controllerSelector, String topicName) {
        String controllerPodName = kubeClient().listPods(controllerSelector).get(0).getMetadata().getName();
        String kafkaLogDirName = cmdKubeClient().execInPod(controllerPodName, "/bin/bash", "-c", "ls /var/lib/kafka/data | grep \"kafka-log[0-9]\" -o").out().trim();
        String commandOutput = cmdKubeClient().execInPod(controllerPodName, "/bin/bash", "-c", "./bin/kafka-dump-log.sh --cluster-metadata-decoder --skip-record-metadata" +
            " --files /var/lib/kafka/data/" + kafkaLogDirName + "/__cluster_metadata-0/00000000000000000000.log | grep " + topicName).out().trim();

        assertThat(String.join("KafkaTopic: %s is not present KRaft metadata", topicName), commandOutput.contains(topicName), is(true));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();
    }
}
