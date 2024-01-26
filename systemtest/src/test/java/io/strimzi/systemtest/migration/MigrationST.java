/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.migration;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
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

import java.util.List;
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

    private KafkaClients immediateClients;
    private KafkaClients continuousClients;
    private String postMigrationTopicName;
    private String kraftTopicName;
    private LabelSelector brokerSelector;
    private LabelSelector controllerSelector;
    private Map<String, String> brokerPodsSnapshot;
    private Map<String, String> controllerPodsSnapshot;

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
     *  7. - Annotates the Kafka resource with strimzi.io/kraft:migration
     *  8. - Controllers will be created and moved to RUNNING state
     *  9. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftMigration
     *  10. - Waits for first rolling update of Broker Pods - bringing the Brokers to DualWrite mode
     *  11. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftDualWriting
     *  12. - Waits for second rolling update of Broker Pods - removing dependency on ZK
     *  13. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftPostMigration
     *  14. - Creates a new KafkaTopic and checks both ZK and KRaft metadata for presence of the KafkaTopic
     *  15. - Does immediate message transmission to the new KafkaTopic
     *  16. - Finishes the migration - annotates the Kafka resource with strimzi.io/kraft:enabled
     *  17. - Controller Pods will be rolled
     *  18. - ZK related resources will be deleted
     *  19. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaft
     *  20. - Removes LMFV and IBPV from Kafka configuration
     *  21. - Broker and Controller Pods will be rolled
     *  22. - Creates a new KafkaTopic and checks KRaft metadata for presence of the KafkaTopic
     *  23. - Does immediate message transmission to the new KafkaTopic
     *  24. - Waits until continuous clients are finished successfully
     *
     * @usecase
     *  - zk-to-kraft-migration
     */
    @IsolatedTest
    @SuppressWarnings("checkstyle:MethodLength")
    void testMigrationFromZkToKRaft(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext, TestConstants.CO_NAMESPACE);

        setupMigrationTestCase(extensionContext, testStorage);
        doFirstPartOfMigration(extensionContext, testStorage);
        doSecondPartOfMigration(extensionContext, testStorage);
    }

    /**
     * @description This testcase is focused on rollback process after first part of the migration from ZK to KRaft is done.
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
     *  7. - Annotates the Kafka resource with strimzi.io/kraft:migration
     *  8. - Controllers will be created and moved to RUNNING state
     *  9. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftMigration
     *  10. - Waits for first rolling update of Broker Pods - bringing the Brokers to DualWrite mode
     *  11. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftDualWriting
     *  12. - Waits for second rolling update of Broker Pods - removing dependency on ZK
     *  13. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftPostMigration
     *  14. - Creates a new KafkaTopic and checks both ZK and KRaft metadata for presence of the KafkaTopic
     *  15. - Does immediate message transmission to the new KafkaTopic
     *  16. - Rolling back the migration - annotates the Kafka resource with strimzi.io/kraft:rollback
     *  17. - Waits for rolling update of Broker Pods - adding dependency on ZK back
     *  18. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftDualWriting
     *  19. - Deletes the Controller NodePool
     *  20. - Finishes rollback - annotates the Kafka resource with strimzi.io/kraft:disabled
     *  21. - Waits for rolling update of Broker Pods - rolling back from DualWrite mode to ZooKeeper
     *  22. - Checks that Kafka CR has .status.kafkaMetadataState set to ZooKeeper
     *  23. - Checks that __cluster_metadata topic doesn't exist in Kafka Brokers
     *  24. - Waits until continuous clients are finished successfully
     *
     * @usecase
     *  - zk-to-kraft-migration
     */
    @IsolatedTest
    void testRollbackDuringMigration(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext, TestConstants.CO_NAMESPACE);

        setupMigrationTestCase(extensionContext, testStorage);
        doFirstPartOfMigration(extensionContext, testStorage);
        doRollback(testStorage);
    }

    private void setupMigrationTestCase(ExtensionContext extensionContext, TestStorage testStorage) {
        postMigrationTopicName = testStorage.getTopicName() + "-midstep";
        kraftTopicName = testStorage.getTopicName() + "-kraft";

        String continuousTopicName = testStorage.getTopicName() + CONTINUOUS_SUFFIX;
        String continuousProducerName = testStorage.getProducerName() + CONTINUOUS_SUFFIX;
        String continuousConsumerName = testStorage.getConsumerName() + CONTINUOUS_SUFFIX;
        int continuousMessageCount = 500;

        brokerSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getBrokerPoolName(), ProcessRoles.BROKER);
        controllerSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getControllerPoolName(), ProcessRoles.CONTROLLER);

        String clientsAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000\nacks=all";

        immediateClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .build();

        continuousClients = new KafkaClientsBuilder()
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

        brokerPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), brokerSelector);

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
    }

    private void doFirstPartOfMigration(ExtensionContext extensionContext, TestStorage testStorage) {
        // starting the migration
        LOGGER.info("Starting the migration process");

        LOGGER.info("Applying the {} annotation with value: {}", Annotations.ANNO_STRIMZI_IO_KRAFT, "migration");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration"), testStorage.getNamespaceName());

        LOGGER.info("Waiting for controller Pods to be up and running");
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, true);
        controllerPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), controllerSelector);

        LOGGER.info("Waiting until .status.kafkaMetadataState in Kafka will contain KRaftMigration state");
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaftMigration.name());

        LOGGER.info("Waiting for first rolling update of broker Pods - bringing to DualWrite mode");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), brokerSelector, brokerPodsSnapshot);

        LOGGER.info("Waiting until .status.kafkaMetadataState in Kafka will contain KRaftDualWriting state");
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaftDualWriting.name());

        LOGGER.info("Waiting for second rolling update of broker Pods - removing dependency on ZK");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);

        LOGGER.info("Waiting until .status.kafkaMetadataState in Kafka will contain KRaftPostMigration state");
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaftPostMigration.name());

        createKafkaTopicAndCheckMetadataWithMessageTransmission(extensionContext, testStorage, postMigrationTopicName, true);
    }

    private void doSecondPartOfMigration(ExtensionContext extensionContext, TestStorage testStorage) {
        LOGGER.info("9. finishing migration - applying the {} annotation with value: {}, controllers should be rolled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"), testStorage.getNamespaceName());

        controllerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerPodsSnapshot);

        LOGGER.info("ZK related resources should be removed now, so waiting for all resources to be deleted");

        waitForZooKeeperResourcesDeletion(testStorage);

        LOGGER.info("Everything related to ZK is deleted, waiting until .status.kafkaMetadataState in Kafka will contain KRaft state");

        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaft.name());

        // the configuration of LMFV and IBPV is done (encapsulated) inside the KafkaTemplates.kafkaPersistent() method
        LOGGER.info("Removing LMFV and IBPV from Kafka config -> Brokers and Controllers should be rolled");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().getConfig().remove("log.message.format.version");
            kafka.getSpec().getKafka().getConfig().remove("inter.broker.protocol.version");
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerPodsSnapshot);

        createKafkaTopicAndCheckMetadataWithMessageTransmission(extensionContext, testStorage, kraftTopicName, false);

        LOGGER.info("Migration is completed, waiting for continuous clients to finish");
        ClientUtils.waitForClientsSuccess(continuousClients.getProducerName(), continuousClients.getConsumerName(), testStorage.getNamespaceName(), continuousClients.getMessageCount());
    }

    private void doRollback(TestStorage testStorage) {
        LOGGER.info("From {} state we are going to roll back to ZK", KafkaMetadataState.KRaftPostMigration.name());

        LOGGER.info("Rolling migration back - applying {}: rollback annotation", Annotations.ANNO_STRIMZI_IO_KRAFT);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback"), testStorage.getNamespaceName());

        LOGGER.info("Waiting for Broker Pods to be rolled - bringing back dependency on ZK");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);

        LOGGER.info("Waiting until .status.kafkaMetadataState contains {} state", KafkaMetadataState.KRaftDualWriting.name());
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaftDualWriting.name());

        LOGGER.info("Deleting Controller's NodePool");
        KafkaNodePool controllerPool = KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getControllerPoolName()).get();
        resourceManager.deleteResource(controllerPool);

        LOGGER.info("Applying {}: disabled annotation", Annotations.ANNO_STRIMZI_IO_KRAFT);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled"), testStorage.getNamespaceName());

        LOGGER.info("Waiting for Broker Pods to be rolled - rolling back from DualWrite mode to Zookeeper");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);

        LOGGER.info("Waiting until .status.kafkaMetadataState contains {} state", KafkaMetadataState.ZooKeeper.name());
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.ZooKeeper.name());

        LOGGER.info("Checking that __cluster_metadata topic does not exist in Kafka Brokers");
        assertThatClusterMetadataTopicNotPresentInBrokerPod(testStorage.getNamespaceName(), brokerSelector);

        LOGGER.info("Rollback completed, waiting until continuous messages transmission is finished");
        ClientUtils.waitForClientsSuccess(continuousClients.getProducerName(), continuousClients.getConsumerName(), testStorage.getNamespaceName(), continuousClients.getMessageCount());
    }

    private void createKafkaTopicAndCheckMetadataWithMessageTransmission(ExtensionContext extensionContext, TestStorage testStorage, String newTopicName, boolean checkZk) {
        LOGGER.info("Creating KafkaTopic: {} and checking if the metadata are in both ZK and KRaft", newTopicName);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        LOGGER.info("Checking if metadata about KafkaTopic: {} are in KRaft controller", newTopicName);

        assertThatTopicIsPresentInKRaftMetadata(testStorage.getNamespaceName(), controllerSelector, newTopicName);

        if (checkZk) {
            LOGGER.info("Checking if metadata about KafkaTopic: {} are in ZK", newTopicName);

            assertThatTopicIsPresentInZKMetadata(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), newTopicName);
        }

        LOGGER.info("Checking if we are able to do a message transmission on KafkaTopic: {}", newTopicName);

        immediateClients = new KafkaClientsBuilder(immediateClients)
            .withTopicName(newTopicName)
            .build();

        resourceManager.createResourceWithWait(extensionContext,
            immediateClients.producerTlsStrimzi(testStorage.getClusterName()),
            immediateClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    private void waitForZooKeeperResourcesDeletion(TestStorage testStorage) {
        cmdKubeClient().namespace(testStorage.getNamespaceName())
            .execInCurrentNamespace(Level.INFO, "wait", "--for", "delete", "networkpolicy,serviceaccount,service,secret,configmap,pdb,sps,pvc,pod",
                "-l", Labels.STRIMZI_NAME_LABEL + "=" + testStorage.getZookeeperStatefulSetName(), "--timeout=300s");
    }

    private void assertThatTopicIsPresentInZKMetadata(String namespaceName, LabelSelector zkSelector, String topicName) {
        String zkPodName = kubeClient().namespace(namespaceName).listPods(zkSelector).get(0).getMetadata().getName();
        String commandOutput = cmdKubeClient().namespace(namespaceName).execInPod(zkPodName, "./bin/zookeeper-shell.sh", "localhost:12181", "ls", "/brokers/topics").out().trim();

        assertThat(String.join("KafkaTopic: %s is not present ZK metadata", topicName), commandOutput.contains(topicName), is(true));
    }

    private void assertThatTopicIsPresentInKRaftMetadata(String namespaceName, LabelSelector controllerSelector, String topicName) {
        String controllerPodName = kubeClient().namespace(namespaceName).listPods(controllerSelector).get(0).getMetadata().getName();
        String kafkaLogDirName = cmdKubeClient().namespace(namespaceName).execInPod(controllerPodName, "/bin/bash", "-c", "ls /var/lib/kafka/data | grep \"kafka-log[0-9]\" -o").out().trim();
        String commandOutput = cmdKubeClient().namespace(namespaceName).execInPod(controllerPodName, "/bin/bash", "-c", "./bin/kafka-dump-log.sh --cluster-metadata-decoder --skip-record-metadata" +
            " --files /var/lib/kafka/data/" + kafkaLogDirName + "/__cluster_metadata-0/00000000000000000000.log | grep " + topicName).out().trim();

        assertThat(String.join("KafkaTopic: %s is not present KRaft metadata", topicName), commandOutput.contains(topicName), is(true));
    }

    private void assertThatClusterMetadataTopicNotPresentInBrokerPod(String namespaceName, LabelSelector brokerSelector) {
        List<Pod> brokerPods = kubeClient().namespace(namespaceName).listPods(brokerSelector);

        for (Pod brokerPod : brokerPods) {
            String kafkaLogDirName = KafkaUtils.getKafkaLogFolderNameInPod(namespaceName, brokerPod.getMetadata().getName());

            String commandOutput = cmdKubeClient().namespace(namespaceName).execInPod(brokerPod.getMetadata().getName(), "/bin/bash", "-c", "ls /var/lib/kafka/data/" + kafkaLogDirName).out().trim();

            assertThat(String.join("__cluster_metadata topic is present in Kafka Pod: %s, but it shouldn't", brokerPod.getMetadata().getName()),
                !commandOutput.contains("__cluster_metadata"), is(true));
        }
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();
    }
}
