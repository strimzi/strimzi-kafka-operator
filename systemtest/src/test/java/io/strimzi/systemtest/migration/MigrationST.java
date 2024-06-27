/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.migration;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.KRaftMetadataStorage;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.api.kafka.model.user.acl.AclResourcePatternType;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
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
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.strimzi.systemtest.TestConstants.MIGRATION;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(MIGRATION)
public class MigrationST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MigrationST.class);
    private static final String CONTINUOUS_SUFFIX = "-continuous";
    private static final int METADATA_VOLUME_ID = 0;
    private static final String METADATA_FOLDER_NAME_JBOD = "data-" + METADATA_VOLUME_ID;
    private static final String METADATA_FOLDER_NAME = "data";
    private String metadataFolderName;
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
     * This is a parametrized test with following arguments:
     * <ul>
     *     <li>$1 - Delete Zookeeper claim: true, false</li>
     *     <li>$2 - Delete Cluster Operator during the process: false, true</li>
     *     <li>$3 - Use JBOD storage configuration: true, false</li>
     * </ul>
     *
     * Based on the checks, the steps can be a bit different.
     *
     * @steps
     *  1. - Deploys Kafka resource (with enabled NodePools and KRaft set to disabled) with Broker NodePool (with particular storage - JBOD/single PV)
     *  2. - Creates topics for continuous and immediate message transmission, TLS user
     *  3. - Starts continuous producer & consumer
     *  4. - Does immediate message transmission
     *  5. - Starts the migration
     *  6. - Creates Controller NodePool (with particular storage - JBOD/single PV) - the Pods will not be created until the migration starts (after we apply the migration annotation)
     *  7. - Annotates the Kafka resource with strimzi.io/kraft:migration
     *  8. - Controllers will be created and moved to RUNNING state
     *  9. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftMigration
     *  ------------------------------- delete CO during process is set to true -------------------------------
     *  10. - Waits for one of the broker Pods to start rolling update
     *  11. - Deletes the ClusterOperator Pod
     *  -------------------------------------------------------------------------------------------------------
     *  12. - Waits for first rolling update of Broker Pods to be finished - bringing the Brokers to DualWrite mode
     *  13. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftDualWriting
     *  ------------------------------- delete CO during process is set to true -------------------------------
     *  14. - Waits for one of the broker Pods to start rolling update
     *  15. - Deletes the ClusterOperator Pod
     *  -------------------------------------------------------------------------------------------------------
     *  16. - Waits for second rolling update of Broker Pods to be finished - removing dependency on ZK
     *  17. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftPostMigration
     *  18. - Creates a new KafkaTopic and checks both ZK and KRaft metadata for presence of the KafkaTopic
     *  19. - Does immediate message transmission to the new KafkaTopic
     *  20. - Finishes the migration - annotates the Kafka resource with strimzi.io/kraft:enabled
     *  ------------------------------- delete CO during process is set to true -------------------------------
     *  21. - Waits for one of the controller Pods to start rolling update
     *  22. - Deletes the ClusterOperator Pod
     *  -------------------------------------------------------------------------------------------------------
     *  23. - Waits for rolling update of Controller Pods will be finished
     *  24. - ZK related resources will be deleted (except of the PVCs)
     *  25. - Check that ZK PVCs are not deleted
     *  26. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaft
     *  27. - Removes LMFV and IBPV from Kafka configuration
     *  28. - Broker and Controller Pods will be rolled
     *  29. - Creates a new KafkaTopic and checks KRaft metadata for presence of the KafkaTopic
     *  30. - Does immediate message transmission to the new KafkaTopic
     *  31. - Waits until continuous clients are finished successfully
     *
     * @usecase
     *  - zk-to-kraft-migration
     */
    @ParameterizedTest(name = "with: ZK delete claim - {0}, CO deletion during process - {1}, JBOD storage - {2}")
    @MethodSource("getArgumentsForMigrationParametrizedTests")
    void testMigration(Boolean zkDeleteClaim, Boolean deleteCoDuringProcess, Boolean withJbodStorage) {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);

        setupMigrationTestCase(testStorage, zkDeleteClaim, withJbodStorage);
        doFirstPartOfMigration(testStorage, deleteCoDuringProcess, withJbodStorage);
        doSecondPartOfMigration(testStorage, deleteCoDuringProcess, zkDeleteClaim);
    }

    /**
     * @description This testcase is focused on rollback process after first part of the migration from ZK to KRaft is done.
     * It goes through whole process, together with checking that message transmission throughout the test will not be
     * disrupted.
     *
     * This is a parametrized test with following arguments:
     * <ul>
     *     <li>$1 - Delete Cluster Operator during the process: false, true</li>
     *     <li>$2 - Use JBOD storage configuration: true, false</li>
     * </ul>
     *
     * Based on the checks, the steps can be a bit different.
     * @steps
     *  1. - Deploys Kafka resource (with enabled NodePools and KRaft set to disabled) with Broker NodePool (with particular storage - JBOD/single PV)
     *  2. - Creates topics for continuous and immediate message transmission, TLS user
     *  3. - Starts continuous producer & consumer
     *  4. - Does immediate message transmission
     *  5. - Starts the migration
     *  6. - Creates Controller NodePool (with particular storage - JBOD/single PV) - the Pods will not be created until the migration starts (after we apply the migration annotation)
     *  7. - Annotates the Kafka resource with strimzi.io/kraft:migration
     *  8. - Controllers will be created and moved to RUNNING state
     *  9. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftMigration
     *  ------------------------------- delete CO during process is set to true -------------------------------
     *  10. - Waits for one of the Broker Pods to start rolling update
     *  11. - Deletes the ClusterOperator Pod
     *  -------------------------------------------------------------------------------------------------------
     *  12. - Waits for first rolling update of Broker Pods to be finished - bringing the Brokers to DualWrite mode
     *  13. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftDualWriting
     *  ------------------------------- delete CO during process is set to true -------------------------------
     *  14. - Waits for one of the Broker Pods to start rolling update
     *  15. - Deletes the ClusterOperator Pod
     *  -------------------------------------------------------------------------------------------------------
     *  16. - Waits for second rolling update of Broker Pods to be finished - removing dependency on ZK
     *  17. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftPostMigration
     *  18. - Creates a new KafkaTopic and checks both ZK and KRaft metadata for presence of the KafkaTopic
     *  19. - Does immediate message transmission to the new KafkaTopic
     *  16. - Rolling back the migration - annotates the Kafka resource with strimzi.io/kraft:rollback
     *  ------------------------------- delete CO during process is set to true -------------------------------
     *  17. - Waits for one of the Broker Pods to start rolling update
     *  18. - Deletes the ClusterOperator Pod
     *  -------------------------------------------------------------------------------------------------------
     *  19. - Waits for rolling update of Broker Pods - adding dependency on ZK back
     *  20. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftDualWriting
     *  19. - Deletes the Controller NodePool
     *  20. - Finishes rollback - annotates the Kafka resource with strimzi.io/kraft:disabled
     *  ------------------------------- delete CO during process is set to true -------------------------------
     *  21. - Waits for one of the Broker Pods to start rolling update
     *  22. - Deletes the ClusterOperator Pod
     *  -------------------------------------------------------------------------------------------------------
     *  23. - Waits for rolling update of Broker Pods - rolling back from DualWrite mode to ZooKeeper
     *  24. - Checks that Kafka CR has .status.kafkaMetadataState set to ZooKeeper
     *  25. - Checks that __cluster_metadata topic doesn't exist in Kafka Brokers
     *  26. - Waits until continuous clients are finished successfully
     *
     * @usecase
     *  - zk-to-kraft-migration
     */
    @ParameterizedTest(name = "with: CO deletion during process - {0}, JBOD storage - {1}")
    @MethodSource("getArgumentsForRollbackParametrizedTests")
    void testRollback(Boolean deleteCoDuringProcess, Boolean withJbodStorage) {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);

        setupMigrationTestCase(testStorage, true, withJbodStorage);
        doFirstPartOfMigration(testStorage, deleteCoDuringProcess, withJbodStorage);
        doRollback(testStorage, deleteCoDuringProcess);
    }

    /**
     * @description This testcase is focused on full circle of the migration process. The test starts with the migration,
     * then, after the first phase, the migration is rolled back to the KRaftDualWriting (half-way of rollback). After that
     * the `strimzi.io/kraft:migration` annotation is applied again and we are waiting for the movement to KRaftPostMigration state, and
     * continue with the migration to the end. The test-case should verify that even if the migration is rolled back to KRaftDualWriting phase, it can
     * be continued again without issues.
     *
     * @steps
     *  1. - Deploys Kafka resource (with enabled NodePools and KRaft set to disabled) with Broker NodePool
     *  2. - Creates topics for continuous and immediate message transmission, TLS user
     *  3. - Starts continuous producer & consumer
     *  4. - Does immediate message transmission
     *  5. - Starts the migration
     *  6. - Creates Controller NodePool - the Pods will not be created until the migration starts (after we apply the migration annotation)
     *  7. - Annotates the Kafka resource with strimzi.io/kraft:migration
     *  8. - Controllers will be created and moved to RUNNING state
     *  9. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftMigration
     *  10. - Waits for first rolling update of Broker Pods - bringing the Brokers to DualWrite mode
     *  11. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftDualWriting
     *  12. - Waits for second rolling update of Broker Pods - removing dependency on ZK
     *  13. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftPostMigration
     *  14. - Creates a new KafkaTopic and checks both ZK and KRaft metadata for presence of the KafkaTopic
     *  15. - Does immediate message transmission to the new KafkaTopic
     *  --------------------------------------------------------------------------------------------------------------------
     *  16. - Rolling back the migration to KRaftDualWriting - annotates the Kafka resource with strimzi.io/kraft:rollback
     *  17. - Waits for rolling update of Broker Pods - adding dependency on ZK back
     *  18. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftDualWriting
     *  --------------------------------------------------------------------------------------------------------------------
     *  19. - Continuing with migration - annotates the Kafka resource with strimzi.io/kraft:migration
     *  20. - Waits for rolling update of Broker Pods - removing dependency on ZK
     *  21. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaftPostMigration
     *  22. - Creates a new KafkaTopic and checks both ZK and KRaft metadata for presence of the KafkaTopic
     *  23. - Does immediate message transmission to the new KafkaTopic
     *  24. - Finishes the migration - annotates the Kafka resource with strimzi.io/kraft:enabled
     *  25. - Controller Pods will be rolled
     *  26. - ZK related resources will be deleted
     *  27. - Checks that Kafka CR has .status.kafkaMetadataState set to KRaft
     *  28. - Removes LMFV and IBPV from Kafka configuration
     *  29. - Broker and Controller Pods will be rolled
     *  30. - Creates a new KafkaTopic and checks KRaft metadata for presence of the KafkaTopic
     *  31. - Does immediate message transmission to the new KafkaTopic
     *  32. - Waits until continuous clients are finished successfully
     *
     * @usecase
     *  - zk-to-kraft-migration
     */
    @IsolatedTest
    void testMigrationWithRollback() {
        TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);
        boolean zkDeleteClaim = true;
        boolean deleteCoDuringProcess = false;
        boolean withJbodStorage = false;

        setupMigrationTestCase(testStorage, zkDeleteClaim, withJbodStorage);
        doFirstPartOfMigration(testStorage, deleteCoDuringProcess, withJbodStorage);
        doFirstPartOfRollback(testStorage, deleteCoDuringProcess);

        LOGGER.info("Applying the {} annotation with value: {}", Annotations.ANNO_STRIMZI_IO_KRAFT, "migration");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration"), testStorage.getNamespaceName());

        moveToKRaftPostMigrationState(testStorage, postMigrationTopicName + "-2");
        doSecondPartOfMigration(testStorage, deleteCoDuringProcess, zkDeleteClaim);
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    private void setupMigrationTestCase(TestStorage testStorage, boolean zkDeleteClaim, boolean withJbodStorage) {
        // we assume that users will have broker NodePool named "kafka", so we will name it completely same to follow this use-case
        String brokerPoolName = "kafka";

        metadataFolderName = withJbodStorage ? METADATA_FOLDER_NAME_JBOD : METADATA_FOLDER_NAME;
        postMigrationTopicName = testStorage.getTopicName() + "-post-migration";
        kraftTopicName = testStorage.getTopicName() + "-kraft";
        String immediateConsumerGroup = ClientUtils.generateRandomConsumerGroup();

        String continuousTopicName = testStorage.getTopicName() + CONTINUOUS_SUFFIX;
        String continuousProducerName = testStorage.getProducerName() + CONTINUOUS_SUFFIX;
        String continuousConsumerName = testStorage.getConsumerName() + CONTINUOUS_SUFFIX;
        String continuousUserName = testStorage.getUsername() + CONTINUOUS_SUFFIX;
        String continuousConsumerGroupName = ClientUtils.generateRandomConsumerGroup();
        int continuousMessageCount = 500;

        brokerSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getClusterName(), brokerPoolName, ProcessRoles.BROKER);
        controllerSelector = KafkaNodePoolResource.getLabelSelector(testStorage.getClusterName(), testStorage.getControllerPoolName(), ProcessRoles.CONTROLLER);

        String clientsAdditionConfiguration = "delivery.timeout.ms=40000\nrequest.timeout.ms=5000\nacks=all\n";

        immediateClients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .withUsername(testStorage.getUsername())
            .withConsumerGroup(immediateConsumerGroup)
            .build();

        continuousClients = new KafkaClientsBuilder()
            .withProducerName(continuousProducerName)
            .withConsumerName(continuousConsumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(continuousTopicName)
            .withMessageCount(continuousMessageCount)
            .withDelayMs(500)
            .withConsumerGroup(continuousConsumerGroupName)
            .withUsername(continuousUserName)
            .withAdditionalConfig(clientsAdditionConfiguration)
            .build();

        LOGGER.info("Deploying Kafka resource with Broker NodePool");

        // create Kafka resource with ZK and Broker NodePool
        resourceManager.createResourceWithWait(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), brokerPoolName, testStorage.getClusterName(), 3)
                    .editSpec()
                        .withStorage(getStorageBasedOnTestCase(withJbodStorage))
                    .endSpec()
                    .build(),
                KafkaTemplates.kafkaPersistentNodePools(testStorage.getClusterName(), 3, 3)
                    .editMetadata()
                        .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                        .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled")
                    .endMetadata()
                    .editSpec()
                        .editOrNewKafka()
                            .withListeners(
                                new GenericKafkaListenerBuilder()
                                    .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .withNewKafkaListenerAuthenticationScramSha512Auth()
                                    .endKafkaListenerAuthenticationScramSha512Auth()
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .withNewKafkaListenerAuthenticationTlsAuth()
                                    .endKafkaListenerAuthenticationTlsAuth()
                                    .build())
                            .withNewKafkaAuthorizationSimple()
                            .endKafkaAuthorizationSimple()
                            .addToConfig("default.replication.factor", 3)
                            .addToConfig("min.insync.replicas", 2)
                        .endKafka()
                        .editZookeeper()
                            .withNewPersistentClaimStorage()
                            .withSize("1Gi")
                            .withDeleteClaim(zkDeleteClaim)
                            .endPersistentClaimStorage()
                        .endZookeeper()
                        .editOrNewEntityOperator()
                            .editOrNewTemplate()
                                .editOrNewTopicOperatorContainer()
                                    .addNewEnv()
                                        .withName("STRIMZI_USE_FINALIZERS")
                                        .withValue("false")
                                    .endEnv()
                                .endTopicOperatorContainer()
                            .endTemplate()
                        .endEntityOperator()
                    .endSpec()
                    .build());

        brokerPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), brokerSelector);

        // at this moment, everything should be ready, so we should ideally create some topics and send + receive the messages (to have some data present in Kafka + metadata about topics in ZK)
        LOGGER.info("Creating two topics for immediate and continuous message transmission and KafkaUser for the TLS");
        resourceManager.createResourceWithWait(
                KafkaTopicTemplates.topic(testStorage).build(),
                KafkaTopicTemplates.topic(testStorage.getClusterName(), continuousTopicName, 3, 3, 2, testStorage.getNamespaceName()).build(),
                KafkaUserTemplates.tlsUser(testStorage)
                    .editOrNewSpec()
                        .withNewKafkaUserAuthorizationSimple()
                            .addNewAcl()
                                .withNewAclRuleTopicResource()
                                    .withPatternType(AclResourcePatternType.PREFIX)
                                    .withName(testStorage.getTopicName())
                                .endAclRuleTopicResource()
                                .withOperations(AclOperation.READ, AclOperation.WRITE, AclOperation.DESCRIBE, AclOperation.CREATE)
                            .endAcl()
                            .addNewAcl()
                                .withNewAclRuleGroupResource()
                                    .withPatternType(AclResourcePatternType.LITERAL)
                                    .withName(immediateConsumerGroup)
                                .endAclRuleGroupResource()
                                .withOperations(AclOperation.READ)
                            .endAcl()
                        .endKafkaUserAuthorizationSimple()
                    .endSpec()
                    .build(),
                KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), continuousUserName)
                    .editOrNewSpec()
                        .withNewKafkaUserAuthorizationSimple()
                            .addNewAcl()
                                .withNewAclRuleTopicResource()
                                    .withPatternType(AclResourcePatternType.LITERAL)
                                    .withName(continuousTopicName)
                                .endAclRuleTopicResource()
                                .withOperations(AclOperation.READ, AclOperation.WRITE, AclOperation.DESCRIBE, AclOperation.CREATE)
                            .endAcl()
                            .addNewAcl()
                                .withNewAclRuleGroupResource()
                                    .withPatternType(AclResourcePatternType.LITERAL)
                                    .withName(continuousConsumerGroupName)
                                .endAclRuleGroupResource()
                                .withOperations(AclOperation.READ)
                            .endAcl()
                        .endKafkaUserAuthorizationSimple()
                    .endSpec()
                    .build()
        );

        // sanity check that kafkaMetadataState shows ZooKeeper
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.ZooKeeper);

        // do the immediate message transmission
        resourceManager.createResourceWithWait(
            immediateClients.producerTlsStrimzi(testStorage.getClusterName()),
            immediateClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    private void doFirstPartOfMigration(TestStorage testStorage, boolean deleteCoDuringProcess, boolean withJbodStorage) {
        // starting the migration
        LOGGER.info("Starting the migration process");

        LOGGER.info("Starting continuous clients");
        resourceManager.createResourceWithWait(
            continuousClients.producerScramShaPlainStrimzi(),
            continuousClients.consumerScramShaPlainStrimzi()
        );

        LOGGER.info("Creating controller NodePool");
        // the controller pods will not be up and running, because we are using the ZK nodes as controllers, they will be created once the migration starts
        // creating it here (before KafkaTopics) to correctly delete KafkaTopics and prevent stuck because UTO cannot connect to controllers
        resourceManager.createResourceWithoutWait(KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3)
            .editSpec()
                .withStorage(getStorageBasedOnTestCase(withJbodStorage))
                // for controllers we have 256Mi set in the memory limits/requests, but during migration, more memory is needed
                .withResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity("384Mi"))
                    .addToRequests("memory", new Quantity("384Mi"))
                    .build())
            .endSpec()
            .build());

        LOGGER.info("Applying the {} annotation with value: {}", Annotations.ANNO_STRIMZI_IO_KRAFT, "migration");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "migration"), testStorage.getNamespaceName());

        LOGGER.info("Waiting for controller Pods to be up and running");
        PodUtils.waitForPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, true);
        controllerPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), controllerSelector);

        LOGGER.info("Waiting until .status.kafkaMetadataState in Kafka will contain KRaftMigration state");
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaftMigration);

        if (deleteCoDuringProcess) {
            LOGGER.info("Waiting for first Broker Pod starts with rolling update, so CO can be deleted");
            RollingUpdateUtils.waitTillComponentHasStartedRolling(testStorage.getNamespaceName(), brokerSelector, brokerPodsSnapshot);
            LOGGER.info("Deleting ClusterOperator's Pod - it should be recreated and continue with the first rolling update of broker Pods");
            deleteClusterOperator();
        }

        LOGGER.info("Waiting for first rolling update of broker Pods - bringing to DualWrite mode");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), brokerSelector, brokerPodsSnapshot);

        LOGGER.info("Waiting until .status.kafkaMetadataState in Kafka will contain KRaftDualWriting state");
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaftDualWriting);

        if (deleteCoDuringProcess) {
            LOGGER.info("Waiting for first Broker Pod starts with rolling update, so CO can be deleted");
            RollingUpdateUtils.waitTillComponentHasStartedRolling(testStorage.getNamespaceName(), brokerSelector, brokerPodsSnapshot);
            LOGGER.info("Deleting ClusterOperator's Pod - it should be recreated and continue with the second rolling update of broker Pods");
            deleteClusterOperator();
        }

        moveToKRaftPostMigrationState(testStorage, postMigrationTopicName);
    }

    private void moveToKRaftPostMigrationState(TestStorage testStorage, String postMigrationTopicName) {
        LOGGER.info("Waiting for second rolling update of broker Pods - removing dependency on ZK");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);

        LOGGER.info("Waiting until .status.kafkaMetadataState in Kafka will contain KRaftPostMigration state");
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaftPostMigration);

        createKafkaTopicAndCheckMetadataWithMessageTransmission(testStorage, postMigrationTopicName, true);
    }

    private void doSecondPartOfMigration(TestStorage testStorage, boolean deleteCoDuringProcess, boolean zkDeleteClaim) {
        LOGGER.info("Finishing migration - applying the {} annotation with value: {}, controllers should be rolled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"), testStorage.getNamespaceName());

        if (deleteCoDuringProcess) {
            LOGGER.info("Waiting for first controller Pod starts with rolling update, so CO can be deleted");
            RollingUpdateUtils.waitTillComponentHasStartedRolling(testStorage.getNamespaceName(), controllerSelector, controllerPodsSnapshot);
            LOGGER.info("Deleting ClusterOperator's Pod - it should be recreated and continue with the rolling update of the controller Pods");
            deleteClusterOperator();
        }

        controllerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerPodsSnapshot);

        LOGGER.info("ZK related resources should be removed now, so waiting for all resources to be deleted");

        waitForZooKeeperResourcesDeletion(testStorage, zkDeleteClaim);

        LOGGER.info("Everything related to ZK is deleted, waiting until .status.kafkaMetadataState in Kafka will contain KRaft state");

        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaft);

        // the configuration of LMFV and IBPV is done (encapsulated) inside the KafkaTemplates.kafkaPersistent() method
        LOGGER.info("Removing LMFV and IBPV from Kafka config -> Brokers and Controllers should be rolled");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), kafka -> {
            kafka.getSpec().getKafka().getConfig().remove("log.message.format.version");
            kafka.getSpec().getKafka().getConfig().remove("inter.broker.protocol.version");
        }, testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerPodsSnapshot);

        createKafkaTopicAndCheckMetadataWithMessageTransmission(testStorage, kraftTopicName, false);

        LOGGER.info("Migration is completed, waiting for continuous clients to finish");
        ClientUtils.waitForClientsSuccess(continuousClients.getProducerName(), continuousClients.getConsumerName(), testStorage.getNamespaceName(), continuousClients.getMessageCount());
    }

    private void doRollback(TestStorage testStorage, boolean deleteCoDuringProcess) {
        doFirstPartOfRollback(testStorage, deleteCoDuringProcess);
        doSecondPartOfRollback(testStorage, deleteCoDuringProcess);
    }

    private void doFirstPartOfRollback(TestStorage testStorage, boolean deleteCoDuringProcess) {
        LOGGER.info("Checking that __cluster_metadata topic does exist in Kafka Brokers");
        assertThatClusterMetadataTopicPresentInBrokerPod(testStorage.getNamespaceName(), brokerSelector, true);

        LOGGER.info("From {} state we are going to roll back to ZK", KafkaMetadataState.KRaftPostMigration.name());

        LOGGER.info("Rolling migration back - applying the {} annotation with value: {}", Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "rollback"), testStorage.getNamespaceName());

        if (deleteCoDuringProcess) {
            LOGGER.info("Waiting for first broker Pod starts with rolling update, so CO can be deleted");
            RollingUpdateUtils.waitTillComponentHasStartedRolling(testStorage.getNamespaceName(), brokerSelector, brokerPodsSnapshot);
            LOGGER.info("Deleting ClusterOperator's Pod - it should be recreated and continue with the rolling update of the broker Pods");
            deleteClusterOperator();
        }

        LOGGER.info("Waiting for Broker Pods to be rolled - bringing back dependency on ZK");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);

        LOGGER.info("Waiting until .status.kafkaMetadataState contains {} state", KafkaMetadataState.KRaftDualWriting.name());
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.KRaftDualWriting);
    }

    private void doSecondPartOfRollback(TestStorage testStorage, boolean deleteCoDuringProcess) {
        LOGGER.info("Deleting Controller's NodePool");
        KafkaNodePool controllerPool = KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getControllerPoolName()).get();
        resourceManager.deleteResource(controllerPool);

        LOGGER.info("Finishing the rollback - applying the {} annotation with value: {}", Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(),
            kafka -> kafka.getMetadata().getAnnotations().put(Annotations.ANNO_STRIMZI_IO_KRAFT, "disabled"), testStorage.getNamespaceName());

        if (deleteCoDuringProcess) {
            LOGGER.info("Waiting for first broker Pod starts with rolling update, so CO can be deleted");
            RollingUpdateUtils.waitTillComponentHasStartedRolling(testStorage.getNamespaceName(), brokerSelector, brokerPodsSnapshot);
            LOGGER.info("Deleting ClusterOperator's Pod - it should be recreated and continue with the rolling update of the broker Pods");
            deleteClusterOperator();
        }

        LOGGER.info("Waiting for Broker Pods to be rolled - rolling back from DualWrite mode to Zookeeper");
        brokerPodsSnapshot = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPodsSnapshot);

        LOGGER.info("Waiting until .status.kafkaMetadataState contains {} state", KafkaMetadataState.ZooKeeper.name());
        KafkaUtils.waitUntilKafkaStatusContainsKafkaMetadataState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaMetadataState.ZooKeeper);

        LOGGER.info("Checking that __cluster_metadata topic does not exist in Kafka Brokers");
        assertThatClusterMetadataTopicPresentInBrokerPod(testStorage.getNamespaceName(), brokerSelector, false);

        LOGGER.info("Rollback completed, waiting until continuous messages transmission is finished");
        ClientUtils.waitForClientsSuccess(continuousClients.getProducerName(), continuousClients.getConsumerName(), testStorage.getNamespaceName(), continuousClients.getMessageCount());
    }

    private void createKafkaTopicAndCheckMetadataWithMessageTransmission(TestStorage testStorage, String newTopicName, boolean checkZk) {
        LOGGER.info("Creating KafkaTopic: {} and checking if the metadata are in both ZK and KRaft", newTopicName);
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), newTopicName, testStorage.getNamespaceName()).build());

        LOGGER.info("Checking if metadata about KafkaTopic: {} are in KRaft controller", newTopicName);

        assertThatTopicIsPresentInKRaftMetadata(testStorage.getNamespaceName(), controllerSelector, newTopicName);

        if (checkZk) {
            LOGGER.info("Checking if metadata about KafkaTopic: {} are in ZK", newTopicName);
            assertThatTopicIsPresentInZKMetadata(testStorage.getNamespaceName(),
                    KafkaResource.getLabelSelector(testStorage.getClusterName(), KafkaResources.zookeeperComponentName(testStorage.getClusterName())), newTopicName);
        }

        LOGGER.info("Checking if we are able to do a message transmission on KafkaTopic: {}", newTopicName);

        immediateClients = new KafkaClientsBuilder(immediateClients)
            .withTopicName(newTopicName)
            .build();

        resourceManager.createResourceWithWait(
                immediateClients.producerTlsStrimzi(testStorage.getClusterName()),
                immediateClients.consumerTlsStrimzi(testStorage.getClusterName())
        );

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), testStorage.getNamespaceName(), testStorage.getMessageCount());
    }

    private void waitForZooKeeperResourcesDeletion(TestStorage testStorage, boolean deleteClaim) {
        List<String> listOfZkResources = new ArrayList<>(List.of("networkpolicy", "serviceaccount", "service", "secret", "configmap", "poddisruptionbudget", "strimzipodset", "pod"));

        if (deleteClaim) {
            listOfZkResources.add("persistentvolumeclaim");
        }

        LOGGER.info("Waiting until all ZK resources: {} will be deleted", String.join(",", listOfZkResources));

        try {
            cmdKubeClient().namespace(testStorage.getNamespaceName())
                .execInCurrentNamespace(Level.INFO, "wait", "--for", "delete", String.join(",", listOfZkResources),
                    "-l", Labels.STRIMZI_NAME_LABEL + "=" + KafkaResources.zookeeperComponentName(testStorage.getClusterName()), "--timeout=300s");
        } catch (KubeClusterException e) {
            if (!e.getMessage().contains("no matching resources found")) {
                // in case that the exception contains message about "no matching resources found", the ZK resources were deleted before execution of the command
                // otherwise we should throw the exception
                throw e;
            }
        }

        LOGGER.info("All ZK related resources were deleted");

        if (!deleteClaim) {
            LOGGER.info("Checking that ZK PVCs are not deleted, because deleteClaim was set to: false");
            List<PersistentVolumeClaim> zkPvcs = kubeClient().listPersistentVolumeClaims(testStorage.getNamespaceName(), KafkaResources.zookeeperComponentName(testStorage.getClusterName()));
            assertThat("Zookeeper PVCs were deleted even if the deleteClaim was set to false", !zkPvcs.isEmpty(), is(true));
            zkPvcs.forEach(pvc -> kubeClient().deletePersistentVolumeClaim(testStorage.getNamespaceName(), pvc.getMetadata().getName()));
        }
    }

    private void deleteClusterOperator() {
        LOGGER.info("Deleting Cluster Operator Pod");
        Pod coPod = KubeClusterResource.kubeClient().listPodsByPrefixInName(clusterOperator.getDeploymentNamespace(), clusterOperator.getClusterOperatorName()).get(0);
        KubeClusterResource.kubeClient().deletePod(clusterOperator.getDeploymentNamespace(), coPod);
        LOGGER.info("Cluster Operator Pod deleted");
    }

    /**
     * Based on {@param withJbodStorage} it returns the {@link Storage} - either with just one Persistent Volume, or with
     * JBOD storage containing two Persistent Volumes.
     *
     * @param withJbodStorage   boolean parameter determining if the JBOD storage should be used in the test case
     *
     * @return  {@link Storage} for the particular testcase (single PV/JBOD)
     */
    private Storage getStorageBasedOnTestCase(boolean withJbodStorage) {
        String pvSize = "1Gi";

        PersistentClaimStorage volume = new PersistentClaimStorageBuilder()
            .withDeleteClaim(true)
            .withId(METADATA_VOLUME_ID)
            .withKraftMetadata(KRaftMetadataStorage.SHARED)
            .withSize(pvSize)
            .build();

        if (withJbodStorage) {
            PersistentClaimStorage secondVolume = new PersistentClaimStorageBuilder()
                .withDeleteClaim(true)
                .withId(1)
                .withSize(pvSize)
                .build();

            return new JbodStorageBuilder()
                .withVolumes(volume, secondVolume)
                .build();
        }

        return volume;
    }

    /**
     * Method returning simple stream of arguments for migration parametrized tests.
     * First argument determines if the ZK delete claim should be set to true/false, second if
     * the CO should be deleted during the migration/rollback process, and third if the JBOD storage should be used.
     *
     * @return  {@link Stream} of {@link Arguments} for each test case.
     */
    private Stream<Arguments> getArgumentsForMigrationParametrizedTests() {
        // ZK delete claim: true, delete CO during process: false, with JBOD storage: true
        Arguments firstTest = Arguments.of(true, false, true);
        // ZK delete claim: false, delete CO during process: true, with JBOD storage: false
        Arguments secondTest = Arguments.of(false, true, false);

        return Stream.of(firstTest, secondTest);
    }

    /**
     * Method returning simple stream of arguments for rollback parametrized tests.
     * First argument determines if the CO should be deleted during the migration/rollback process
     * and second if the JBOD storage should be used.
     *
     * @return  {@link Stream} of {@link Arguments} for each test case.
     */
    private Stream<Arguments> getArgumentsForRollbackParametrizedTests() {
        // delete CO during process: false, with JBOD storage: true
        Arguments firstTest = Arguments.of(false, true);
        // delete CO during process: true, with JBOD storage: false
        Arguments secondTest = Arguments.of(true, false);

        return Stream.of(firstTest, secondTest);
    }

    private void assertThatTopicIsPresentInZKMetadata(String namespaceName, LabelSelector zkSelector, String topicName) {
        String zkPodName = kubeClient().namespace(namespaceName).listPods(zkSelector).get(0).getMetadata().getName();

        TestUtils.waitFor(String.join("KafkaTopic: %s to be present in ZK metadata"), TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                try {
                    String commandOutput = cmdKubeClient().namespace(namespaceName).execInPod(zkPodName, "./bin/zookeeper-shell.sh", "localhost:12181", "ls", "/brokers/topics").out().trim();
                    return commandOutput.contains(topicName);
                } catch (Exception e) {
                    LOGGER.warn("Exception caught during command execution, message: {}", e.getMessage());
                    return false;
                }
            });
    }

    private void assertThatTopicIsPresentInKRaftMetadata(String namespaceName, LabelSelector controllerSelector, String topicName) {
        String controllerPodName = kubeClient().namespace(namespaceName).listPods(controllerSelector).get(0).getMetadata().getName();
        String kafkaLogDirName = KafkaUtils.getKafkaLogFolderNameInPod(namespaceName, controllerPodName, metadataFolderName);

        TestUtils.waitFor(String.join("KafkaTopic: %s to be present in KRaft metadata"), TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                try {
                    String commandOutput = cmdKubeClient().namespace(namespaceName).execInPod(controllerPodName, "/bin/bash", "-c", "./bin/kafka-dump-log.sh --cluster-metadata-decoder --skip-record-metadata" +
                        " --files /var/lib/kafka/" + metadataFolderName + "/" + kafkaLogDirName + "/__cluster_metadata-0/00000000000000000000.log | grep " + topicName).out().trim();
                    return commandOutput.contains(topicName);
                } catch (Exception e) {
                    LOGGER.warn("Exception caught during command execution, message: {}", e.getMessage());
                    return false;
                }
            });
    }

    private void assertThatClusterMetadataTopicPresentInBrokerPod(String namespaceName, LabelSelector brokerSelector, boolean clusterMetadataShouldExist) {
        List<Pod> brokerPods = kubeClient().namespace(namespaceName).listPods(brokerSelector);

        for (Pod brokerPod : brokerPods) {
            String kafkaLogDirName = KafkaUtils.getKafkaLogFolderNameInPod(namespaceName, brokerPod.getMetadata().getName(), metadataFolderName);

            String commandOutput = cmdKubeClient().namespace(namespaceName).execInPod(brokerPod.getMetadata().getName(), "/bin/bash", "-c", "ls /var/lib/kafka/" + metadataFolderName + "/" + kafkaLogDirName).out().trim();

            assertThat(String.join("__cluster_metadata topic is present in Kafka Pod: %s, but it shouldn't", brokerPod.getMetadata().getName()),
                commandOutput.contains("__cluster_metadata"), is(clusterMetadataShouldExist));
        }
    }

    @BeforeAll
    void setup() {
        // skip if Kafka version is lower than 3.7.0
        // TODO: remove once support for Kafka 3.6.x is removed - https://github.com/strimzi/strimzi-kafka-operator/issues/9921
        assumeTrue(TestKafkaVersion.compareDottedVersions(Environment.ST_KAFKA_VERSION, "3.7.0") >= 0);
        assumeTrue(Environment.isKafkaNodePoolsEnabled() && Environment.isKRaftModeEnabled());
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }
}
