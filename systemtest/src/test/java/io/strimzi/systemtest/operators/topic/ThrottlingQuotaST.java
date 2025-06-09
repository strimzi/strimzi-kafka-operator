/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test checks for throttling quotas set for user
 * on creation & deletion of topics and create partition operations.
 */
@Tag(REGRESSION)
public class ThrottlingQuotaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ThrottlingQuotaST.class);

    private static final int TIMEOUT_REQUEST_MS = 240000;
    private static final String THROTTLING_ERROR_MSG =
        "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.";
    private TestStorage sharedTestStorage;

    private AdminClient limitedAdminClient;
    // this Admin client does not have assigned quota therefore is able to perform clean ups and checks
    private AdminClient unlimitedAdminClient;

    @ParallelTest
    void testThrottlingQuotasDuringAllTopicOperations() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        int numOfTopics = 25;
        int numOfPartitions = 100;
        int numOfReplicas = 1;

        int iterations = numOfTopics / 5;

        LOGGER.info("Creating {} Topics with {} partitions, we should hit the quota", numOfTopics, numOfPartitions);

        String commandOutput = limitedAdminClient.createTopics(testStorage.getTopicName(), numOfTopics, numOfPartitions, numOfReplicas);
        assertThat(commandOutput, containsString(THROTTLING_ERROR_MSG));

        unlimitedAdminClient.deleteTopicsWithPrefix(testStorage.getTopicName());
        KafkaTopicUtils.waitForDeletionOfTopicsWithPrefix(testStorage.getTopicName(), unlimitedAdminClient);
        LOGGER.info("All created topics are cleaned");

        numOfPartitions = 5;

        LOGGER.info("Creating {} Topics with {} partitions, the quota should not be exceeded", numOfTopics, numOfPartitions);

        commandOutput = limitedAdminClient.createTopics(testStorage.getTopicName(), numOfTopics, numOfPartitions, numOfReplicas);
        assertThat(commandOutput, containsString("successfully created"));

        for (int i = 0; i < numOfTopics; i++) {
            assertThat(commandOutput, containsString(testStorage.getTopicName() + "-" + i));
        }

        int partitionAlter = 25;

        LOGGER.info("Altering {} Topics - setting partitions to {} - we should hit the quota", numOfTopics, partitionAlter);

        commandOutput = limitedAdminClient.alterPartitionsForTopicsInRange(testStorage.getTopicName(), partitionAlter, numOfTopics, 0);
        assertThat(commandOutput, containsString(THROTTLING_ERROR_MSG));

        // we need to set higher partitions - for case when we alter some topics before hitting the quota to 25 partitions
        partitionAlter = 30;
        int numOfTopicsIter = 5;

        for (int i = 0; i < iterations; i++) {
            LOGGER.info("Altering {} Topics with offset {} - setting partitions to {} - we should not hit the quota", numOfTopicsIter, numOfTopicsIter * i, partitionAlter);

            commandOutput = limitedAdminClient.alterPartitionsForTopicsInRange(testStorage.getTopicName(), partitionAlter, numOfTopicsIter, numOfTopicsIter * i);
            assertThat(commandOutput, containsString("successfully altered"));
        }

        // delete few topics
        LOGGER.info("Deleting first {} Topics, we will not hit the quota", numOfTopicsIter);
        commandOutput = limitedAdminClient.deleteTopicsWithPrefixAndCount(testStorage.getTopicName(), numOfTopicsIter);
        assertThat(commandOutput, containsString("successfully deleted"));

        int remainingTopics = numOfTopics - numOfTopicsIter;

        LOGGER.info("Trying to remove all remaining {} Topics with offset of {} - we should hit the quota", remainingTopics, numOfTopicsIter);
        commandOutput = limitedAdminClient.deleteTopicsWithPrefixAndCountFromIndex(testStorage.getTopicName(), remainingTopics, numOfTopicsIter);
        assertThat(commandOutput, containsString(THROTTLING_ERROR_MSG));

        LOGGER.info("Removing the remaining Topics through console");
        unlimitedAdminClient.deleteTopicsWithPrefix(testStorage.getTopicName());
        KafkaTopicUtils.waitForDeletionOfTopicsWithPrefix(testStorage.getTopicName(), unlimitedAdminClient);

        // List topics after deletion
        commandOutput = unlimitedAdminClient.listTopics();
        assertThat(commandOutput, not(containsString(testStorage.getTopicName())));
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        sharedTestStorage = new TestStorage(KubeResourceManager.get().getTestContext(), Environment.TEST_SUITE_NAMESPACE);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 3).build()
        );
        // Deploy kafka with ScramSHA512
        LOGGER.info("Deploying shared Kafka across all test cases in {} Namespace", sharedTestStorage.getNamespaceName());
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(sharedTestStorage.getNamespaceName(), sharedTestStorage.getClusterName(), 3)
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

        // deploy quota limited admin client and respective Kafka User

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.defaultUser(sharedTestStorage.getNamespaceName(), sharedTestStorage.getUsername(), sharedTestStorage.getClusterName())
            .editOrNewSpec()
                .withNewQuotas()
                    .withControllerMutationRate(1.0)
                .endQuotas()
                .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
            .endSpec()
            .build()
        );

        KubeResourceManager.get().createResourceWithWait(
            AdminClientTemplates.scramShaOverPlainAdminClient(
                sharedTestStorage.getNamespaceName(),
                sharedTestStorage.getUsername(),
                sharedTestStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()),
                CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG + "=" + TIMEOUT_REQUEST_MS
            ));
        limitedAdminClient = AdminClientUtils.getConfiguredAdminClient(sharedTestStorage.getNamespaceName(), sharedTestStorage.getAdminName());

        // deploy unlimited admin client and respective user for purpose of cleaning up resources

        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.defaultUser(sharedTestStorage.getNamespaceName(), sharedTestStorage.getUsername() + "-unlimited", sharedTestStorage.getClusterName())
            .editOrNewSpec()
                .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(
            AdminClientTemplates.scramShaOverPlainAdminClient(
                sharedTestStorage.getNamespaceName(),
                sharedTestStorage.getUsername() + "-unlimited",
                sharedTestStorage.getAdminName() + "-unlimited",
                KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()),
                CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG + "=" + TIMEOUT_REQUEST_MS
            ));
        unlimitedAdminClient = AdminClientUtils.getConfiguredAdminClient(sharedTestStorage.getNamespaceName(), sharedTestStorage.getAdminName() + "-unlimited");
    }
}
