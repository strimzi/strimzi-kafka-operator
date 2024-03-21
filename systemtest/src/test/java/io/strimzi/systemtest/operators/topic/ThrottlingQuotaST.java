/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.annotations.UTONotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestConstants.ARM64_UNSUPPORTED;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test checks for throttling quotas set for user
 * on creation & deletion of topics and create partition operations.
 */
@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@Tag(ARM64_UNSUPPORTED) // Due to https://github.com/strimzi/test-clients/issues/75
public class ThrottlingQuotaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ThrottlingQuotaST.class);

    private static final int TIMEOUT_REQUEST_MS = 240000;
    private static final String THROTTLING_ERROR_MSG =
        "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.";
    private TestStorage sharedTestStorage;

    private AdminClient adminClient;

    @ParallelTest
    @UTONotSupported("https://github.com/strimzi/strimzi-kafka-operator/issues/8864")
    void testThrottlingQuotasDuringAllTopicOperations() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        int numOfTopics = 25;
        int numOfPartitions = 100;
        int numOfReplicas = 1;

        int iterations = numOfTopics / 5;

        LOGGER.info("Creating {} Topics with {} partitions, we should hit the quota", numOfTopics, numOfPartitions);

        String commandOutput = adminClient.createTopics(testStorage.getTopicName(), numOfTopics, numOfPartitions, numOfReplicas);
        assertThat(commandOutput, containsString(THROTTLING_ERROR_MSG));

        KafkaTopicUtils.deleteAllKafkaTopicsByPrefixWithWait(testStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForDeletionOfTopicsWithPrefix(testStorage.getTopicName(), adminClient);

        numOfPartitions = 5;

        LOGGER.info("Creating {} Topics with {} partitions, the quota should not be exceeded", numOfTopics, numOfPartitions);

        commandOutput = adminClient.createTopics(testStorage.getTopicName(), numOfTopics, numOfPartitions, numOfReplicas);
        assertThat(commandOutput, containsString("successfully created"));

        commandOutput = adminClient.listTopics();

        for (int i = 0; i < numOfTopics; i++) {
            assertThat(commandOutput, containsString(testStorage.getTopicName() + "-" + i));
        }

        int partitionAlter = 25;

        LOGGER.info("Altering {} Topics - setting partitions to {} - we should hit the quota", numOfTopics, partitionAlter);

        commandOutput = adminClient.alterPartitionsForTopicsInRange(testStorage.getTopicName(), partitionAlter, numOfTopics, 0);
        assertThat(commandOutput, containsString(THROTTLING_ERROR_MSG));

        // we need to set higher partitions - for case when we alter some topics before hitting the quota to 25 partitions
        partitionAlter = 30;
        int numOfTopicsIter = 5;

        for (int i = 0; i < iterations; i++) {
            LOGGER.info("Altering {} Topics with offset {} - setting partitions to {} - we should not hit the quota", numOfTopicsIter, numOfTopicsIter * i, partitionAlter);

            commandOutput = adminClient.alterPartitionsForTopicsInRange(testStorage.getTopicName(), partitionAlter, numOfTopicsIter, numOfTopicsIter * i);
            assertThat(commandOutput, containsString("successfully altered"));
        }

        // delete few topics
        LOGGER.info("Deleting first {} Topics, we will not hit the quota", numOfTopicsIter);
        commandOutput = adminClient.deleteTopicsWithPrefixAndCount(testStorage.getTopicName(), numOfTopicsIter);
        assertThat(commandOutput, containsString("successfully deleted"));

        int remainingTopics = numOfTopics - numOfTopicsIter;

        LOGGER.info("Trying to remove all remaining {} Topics with offset of {} - we should hit the quota", remainingTopics, numOfTopicsIter);
        commandOutput = adminClient.deleteTopicsWithPrefixAndCountFromIndex(testStorage.getTopicName(), remainingTopics, numOfTopicsIter);
        assertThat(commandOutput, containsString(THROTTLING_ERROR_MSG));

        LOGGER.info("Because we hit quota, removing the remaining Topics through console");
        KafkaTopicUtils.deleteAllKafkaTopicsByPrefixWithWait(testStorage.getNamespaceName(), testStorage.getTopicName());
        // we need to wait for all KafkaTopics to be deleted from Kafka before proceeding - using Kafka pod cli (with AdminClient props)
        KafkaTopicUtils.waitForDeletionOfTopicsWithPrefix(testStorage.getTopicName(), adminClient);

        // List topics after deletion
        commandOutput = adminClient.listTopics();
        assertThat(commandOutput, not(containsString(testStorage.getTopicName())));
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();

        sharedTestStorage = new TestStorage(ResourceManager.getTestContext(), Environment.TEST_SUITE_NAMESPACE);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 3).build()
            )
        );
        // Deploy kafka with ScramSHA512
        LOGGER.info("Deploying shared Kafka across all test cases in {} Namespace", sharedTestStorage.getNamespaceName());
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(sharedTestStorage.getClusterName(), 3)
            .editMetadata()
                .withNamespace(sharedTestStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
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
                .endKafka()
            .endSpec()
            .build()
        );

        resourceManager.createResourceWithWait(KafkaUserTemplates.defaultUser(sharedTestStorage.getNamespaceName(), sharedTestStorage.getClusterName(), sharedTestStorage.getUsername())
            .editOrNewSpec()
                .withNewQuotas()
                    .withControllerMutationRate(1.0)
                .endQuotas()
                .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(
            AdminClientTemplates.scramShaAdminClient(sharedTestStorage.getNamespaceName(),
                sharedTestStorage.getUsername(),
                sharedTestStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()),
                CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG + "=" + TIMEOUT_REQUEST_MS
            )
        );

        String adminClientPodName = kubeClient().listPods(sharedTestStorage.getNamespaceName(), TestConstants.ADMIN_CLIENT_LABEL_SELECTOR).get(0).getMetadata().getName();

        adminClient = new AdminClient(sharedTestStorage.getNamespaceName(), adminClientPodName);
        adminClient.configureFromEnv();
    }
}
