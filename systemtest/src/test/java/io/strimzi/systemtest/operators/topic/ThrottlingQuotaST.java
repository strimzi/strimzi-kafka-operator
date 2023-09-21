/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftWithoutUTONotSupported;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.annotations.UTONotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.AdminClientOperation;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaAdminClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaAdminClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test checks for throttling quotas set for user
 * on creation & deletion of topics and create partition operations.
 */
@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
public class ThrottlingQuotaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ThrottlingQuotaST.class);

    private static final String THROTTLING_ERROR_MSG =
        "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.";
    private TestStorage sharedTestStorage;

    private KafkaAdminClientsBuilder adminClientsBuilder;
    private String scraperPodName = "";

    @ParallelTest
    @KRaftWithoutUTONotSupported
    @UTONotSupported("https://github.com/strimzi/strimzi-kafka-operator/issues/8864")
    void testThrottlingQuotasDuringAllTopicOperations(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);

        final String createAdminName = "create-" + testStorage.getAdminName();
        final String alterAdminName = "alter-" + testStorage.getAdminName();
        final String deleteAdminName = "delete-" + testStorage.getAdminName();
        final String listAdminName = "list-" + testStorage.getAdminName();
        final String plainBootstrapName = KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName());

        int numOfTopics = 25;
        int numOfPartitions = 100;

        int iterations = numOfTopics / 5;

        KafkaAdminClients createTopicJob = adminClientsBuilder
            .withAdminName(createAdminName)
            .withTopicName(testStorage.getTopicName())
            .withTopicCount(numOfTopics)
            .withPartitions(numOfPartitions)
            .withAdminOperation(AdminClientOperation.CREATE_TOPICS)
            .build();

        LOGGER.info("Creating {} Topics with {} partitions, we should hit the quota", numOfTopics, numOfPartitions);

        resourceManager.createResourceWithWait(extensionContext, createTopicJob.defaultAdmin());
        ClientUtils.waitForClientContainsMessage(createAdminName, testStorage.getNamespaceName(), THROTTLING_ERROR_MSG);

        KafkaTopicUtils.deleteAllKafkaTopicsByPrefixWithWait(testStorage.getNamespaceName(), testStorage.getTopicName());
        // we need to wait for all KafkaTopics to be deleted from Kafka before proceeding - using Kafka pod cli (with AdminClient props)
        KafkaTopicUtils.waitForTopicsByPrefixDeletionUsingPodCli(testStorage.getNamespaceName(),
            testStorage.getTopicName(), plainBootstrapName, scraperPodName, createTopicJob.getAdditionalConfig());

        numOfPartitions = 5;

        createTopicJob = new KafkaAdminClientsBuilder(createTopicJob)
            .withPartitions(numOfPartitions)
            .build();

        LOGGER.info("Creating {} Topics with {} partitions, the quota should not be exceeded", numOfTopics, numOfPartitions);

        resourceManager.createResourceWithWait(extensionContext, createTopicJob.defaultAdmin());
        ClientUtils.waitForClientContainsMessage(createAdminName, testStorage.getNamespaceName(), "All topics created");

        KafkaAdminClients listTopicJob = new KafkaAdminClientsBuilder(createTopicJob)
            .withAdminName(listAdminName)
            .withTopicName("")
            .withAdminOperation(AdminClientOperation.LIST_TOPICS)
            .build();

        LOGGER.info("Listing Topics after creation");
        resourceManager.createResourceWithWait(extensionContext, listTopicJob.defaultAdmin());

        List<String> topicNames = new ArrayList<>();

        for (int i = 0; i < numOfTopics; i++) {
            topicNames.add(testStorage.getTopicName() + "-" + i);
        }

        ClientUtils.waitForClientContainsAllMessages(listAdminName, testStorage.getNamespaceName(), topicNames, true);

        int partitionAlter = 25;

        KafkaAdminClients alterTopicsJob = new KafkaAdminClientsBuilder(createTopicJob)
            .withAdminName(alterAdminName)
            .withPartitions(partitionAlter)
            .withAdminOperation(AdminClientOperation.ALTER_TOPICS)
            .build();

        LOGGER.info("Altering {} Topics - setting partitions to {} - we should hit the quota", numOfTopics, partitionAlter);

        // because we are not hitting the quota, this should pass without a problem
        resourceManager.createResourceWithWait(extensionContext, alterTopicsJob.defaultAdmin());
        ClientUtils.waitForClientContainsMessage(alterAdminName, testStorage.getNamespaceName(), THROTTLING_ERROR_MSG);

        // we need to set higher partitions - for case when we alter some topics before hitting the quota to 25 partitions
        partitionAlter = 30;
        int numOfTopicsIter = 5;

        alterTopicsJob = new KafkaAdminClientsBuilder(alterTopicsJob)
            .withPartitions(partitionAlter)
            .withTopicCount(numOfTopicsIter)
            .build();

        for (int i = 0; i < iterations; i++) {
            alterTopicsJob = new KafkaAdminClientsBuilder(alterTopicsJob)
                .withTopicCount(numOfTopicsIter)
                .withTopicOffset(numOfTopicsIter * i)
                .build();

            LOGGER.info("Altering {} Topics with offset {} - setting partitions to {} - we should not hit the quota", numOfTopicsIter, numOfTopicsIter * i, partitionAlter);
            resourceManager.createResourceWithWait(extensionContext, alterTopicsJob.defaultAdmin());
            ClientUtils.waitForClientContainsMessage(alterAdminName, testStorage.getNamespaceName(), "All topics altered");
        }

        // delete few topics
        KafkaAdminClients deleteTopicsJob = adminClientsBuilder
            .withTopicName(testStorage.getTopicName())
            .withAdminName(deleteAdminName)
            .withAdminOperation(AdminClientOperation.DELETE_TOPICS)
            .withTopicCount(numOfTopicsIter)
            .build();

        LOGGER.info("Deleting first {} Topics, we will not hit the quota", numOfTopicsIter);
        resourceManager.createResourceWithWait(extensionContext, deleteTopicsJob.defaultAdmin());
        ClientUtils.waitForClientContainsMessage(deleteAdminName, testStorage.getNamespaceName(), "Successfully removed all " + numOfTopicsIter);

        int remainingTopics = numOfTopics - numOfTopicsIter;

        deleteTopicsJob = new KafkaAdminClientsBuilder(deleteTopicsJob)
            .withTopicCount(remainingTopics)
            .withTopicOffset(numOfTopicsIter)
            .build();

        LOGGER.info("Trying to remove all remaining {} Topics with offset of {} - we should hit the quota", remainingTopics, numOfTopicsIter);
        resourceManager.createResourceWithWait(extensionContext, deleteTopicsJob.defaultAdmin());
        ClientUtils.waitForClientContainsMessage(deleteAdminName, testStorage.getNamespaceName(), THROTTLING_ERROR_MSG);

        LOGGER.info("Because we hit quota, removing the remaining Topics through console");
        KafkaTopicUtils.deleteAllKafkaTopicsByPrefixWithWait(testStorage.getNamespaceName(), testStorage.getTopicName());
        // we need to wait for all KafkaTopics to be deleted from Kafka before proceeding - using Kafka pod cli (with AdminClient props)
        KafkaTopicUtils.waitForTopicsByPrefixDeletionUsingPodCli(testStorage.getNamespaceName(),
            testStorage.getTopicName(), plainBootstrapName, scraperPodName, createTopicJob.getAdditionalConfig());

        // List topics after deletion
        resourceManager.createResourceWithWait(extensionContext, listTopicJob.defaultAdmin());
        ClientUtils.waitForClientSuccess(listAdminName, testStorage.getNamespaceName(), 0, false);

        String listPodName = PodUtils.getPodNameByPrefix(testStorage.getNamespaceName(), listAdminName);
        String afterDeletePodLogs = kubeClient().logsInSpecificNamespace(testStorage.getNamespaceName(), listPodName);

        assertFalse(afterDeletePodLogs.contains(testStorage.getTopicName()));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();

        sharedTestStorage = new TestStorage(extensionContext, Environment.TEST_SUITE_NAMESPACE);

        // Deploy kafka with ScramSHA512
        LOGGER.info("Deploying shared Kafka across all test cases in {} Namespace", sharedTestStorage.getNamespaceName());
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(sharedTestStorage.getClusterName(), 3)
            .editMetadata()
                .withNamespace(sharedTestStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                            .withNewKafkaListenerAuthenticationTlsAuth()
                            .endKafkaListenerAuthenticationTlsAuth()
                            .build())
                .endKafka()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(sharedTestStorage.getNamespaceName(), sharedTestStorage.getScraperName()).build()
        );

        scraperPodName = kubeClient().listPodsByPrefixInName(sharedTestStorage.getNamespaceName(), sharedTestStorage.getScraperName()).get(0).getMetadata().getName();

        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.defaultUser(sharedTestStorage.getNamespaceName(), sharedTestStorage.getClusterName(), sharedTestStorage.getUsername())
            .editOrNewSpec()
                .withNewQuotas()
                    .withControllerMutationRate(1.0)
                .endQuotas()
                .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
            .endSpec()
            .build());

        adminClientsBuilder = new KafkaAdminClientsBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()))
            .withNamespaceName(sharedTestStorage.getNamespaceName())
            .withAdditionalConfig(KafkaAdminClients.getAdminClientScramConfig(sharedTestStorage.getNamespaceName(), sharedTestStorage.getUsername(), 240000));
    }
}
