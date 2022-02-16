/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.AdminClientOperations;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaAdminClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaAdminClientsBuilder;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.GLOBAL_TIMEOUT;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@Tag(REGRESSION)
@Tag(INTERNAL_CLIENTS_USED)
@ParallelSuite
public class ThrottlingQuotaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ThrottlingQuotaST.class);
    private static final String CLUSTER_NAME = "quota-cluster";

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(ThrottlingQuotaST.class.getSimpleName()).stream().findFirst().get();
    private final String classTopicPrefix = "quota-topic-test";

    /**
     * Test checks for throttling quotas set for user
     * on creation & deletion of topics and create partition operations.
     */
    @ParallelTest
    void testThrottlingQuotasCreateTopic(ExtensionContext extensionContext) {
        final String kafkaUsername = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String createAdminName = "create-admin-" + mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String topicNamePrefix = classTopicPrefix + "-create";
        int topicsCountOverQuota = 500;
        setupKafkaUserInNamespace(extensionContext, kafkaUsername);

        KafkaAdminClients adminClientJob = new KafkaAdminClientsBuilder()
                .withAdminName(createAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicsCountOverQuota)
                .withNamespaceName(namespace)
                .withTopicOperation(AdminClientOperations.CREATE_TOPICS.toString())
                .withAdditionalConfig(KafkaAdminClients.getAdminClientScramConfig(namespace, kafkaUsername, 240000))
                .build();
        resourceManager.createResource(extensionContext, adminClientJob.defaultAdmin());
        String createPodName = kubeClient(namespace).listPodNamesInSpecificNamespace(namespace, "job-name", createAdminName).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(
                namespace,
                createPodName,
                "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.",
                GLOBAL_TIMEOUT
        );
        JobUtils.deleteJobWithWait(namespace, createAdminName);

        // Teardown delete created topics
        KafkaTopicUtils.deleteAllKafkaTopicsWithPrefix(namespace, topicNamePrefix);
    }

    @ParallelTest
    void testThrottlingQuotasDeleteTopic(ExtensionContext extensionContext) {
        final String kafkaUsername = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String createAdminName = "create-admin-" + mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String deleteAdminName = "delete-admin-" + mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String topicNamePrefix = classTopicPrefix + "-delete";
        int topicsCountOverQuota = 500;
        setupKafkaUserInNamespace(extensionContext, kafkaUsername);

        // Create many topics in multiple rounds using starting offset
        KafkaAdminClients createAdminClientJob = new KafkaAdminClientsBuilder()
                .withAdminName(createAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(100)
                .withNamespaceName(namespace)
                .withTopicOperation(AdminClientOperations.CREATE_TOPICS.toString())
                .withAdditionalConfig(KafkaAdminClients.getAdminClientScramConfig(namespace, kafkaUsername, 240000))
                .build();
        String createPodName;
        int offset = 100;
        int iterations = topicsCountOverQuota / 100;
        for (int i = 0; i < iterations; i++) {
            LOGGER.info("Executing {}/{} iteration.", i + 1, iterations);
            createAdminClientJob = new KafkaAdminClientsBuilder(createAdminClientJob).withTopicOffset(i * offset).build();

            resourceManager.createResource(extensionContext, createAdminClientJob.defaultAdmin());
            ClientUtils.waitForClientSuccess(createAdminName, namespace, 100);
            createPodName = kubeClient(namespace).listPodNamesInSpecificNamespace(namespace, "job-name", createAdminName).get(0);
            PodUtils.waitUntilMessageIsInPodLogs(namespace, createPodName, "All topics created", Constants.GLOBAL_TIMEOUT);
            JobUtils.deleteJobWithWait(namespace, createAdminName);
        }

        // Test delete all topics at once - should fail on Throttling Quota limit
        KafkaAdminClients deleteAdminClientJob = new KafkaAdminClientsBuilder()
                .withAdminName(deleteAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicsCountOverQuota)
                .withNamespaceName(namespace)
                .withTopicOperation(AdminClientOperations.DELETE_TOPICS.toString())
                .withAdditionalConfig(KafkaAdminClients.getAdminClientScramConfig(namespace, kafkaUsername, 240000))
                .build();
        resourceManager.createResource(extensionContext, deleteAdminClientJob.defaultAdmin());
        String deletePodName = kubeClient(namespace).listPodNamesInSpecificNamespace(namespace, "job-name", deleteAdminName).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(
                namespace,
                deletePodName,
                "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.",
                Constants.GLOBAL_TIMEOUT
        );
        JobUtils.deleteJobWithWait(namespace, deleteAdminName);

        // Teardown - delete all (remaining) topics (within Quota limits) in multiple rounds using starting offsets
        for (int i = 0; i < iterations; i++) {
            LOGGER.info("Executing {}/{} iteration for {}.", i + 1, iterations, deleteAdminName);
            deleteAdminClientJob = new KafkaAdminClientsBuilder(deleteAdminClientJob)
                    .withTopicOffset(i * offset)
                    .withTopicCount(100)
                    .withAdditionalConfig("")
                    .build();
            resourceManager.createResource(extensionContext, deleteAdminClientJob.defaultAdmin());
            ClientUtils.waitForClientSuccess(deleteAdminName, namespace, 10);
            JobUtils.deleteJobWithWait(namespace, deleteAdminName);
        }
    }

    @ParallelTest
    void testThrottlingQuotasCreateAlterPartitions(ExtensionContext extensionContext) {
        final String kafkaUsername = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String createAdminName = "create-admin-" + mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String alterAdminName = "alter-admin-" + mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String topicNamePrefix = classTopicPrefix + "-partitions";
        int topicsCount = 50;
        int topicPartitions = 100;
        setupKafkaUserInNamespace(extensionContext, kafkaUsername);

        KafkaAdminClients adminClientJob = new KafkaAdminClientsBuilder()
                .withAdminName(createAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicsCount)
                .withPartitions(topicPartitions)
                .withNamespaceName(namespace)
                .withTopicOperation(AdminClientOperations.CREATE_TOPICS.toString())
                .withAdditionalConfig(KafkaAdminClients.getAdminClientScramConfig(namespace, kafkaUsername, 240000))
                .build();
        resourceManager.createResource(extensionContext, adminClientJob.defaultAdmin());
        String createPodName = kubeClient(namespace).listPodNamesInSpecificNamespace(namespace, "job-name", createAdminName).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(
                namespace,
                createPodName,
                "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.",
                Constants.GLOBAL_TIMEOUT
        );
        JobUtils.deleteJobWithWait(namespace, createAdminName);

        // Delete created topics (as they were created in random order, we have to use KafkaTopic instead of this client)
        KafkaTopicUtils.deleteAllKafkaTopicsWithPrefix(namespace, topicNamePrefix);

        // Throttling quota after performed 'alter' partitions on existing topic
        int topicAlter = 20;
        adminClientJob = new KafkaAdminClientsBuilder(adminClientJob)
                .withTopicCount(topicAlter)
                .withPartitions(1)
                .build();
        resourceManager.createResource(extensionContext, adminClientJob.defaultAdmin());
        createPodName = kubeClient(namespace).listPodNamesInSpecificNamespace(namespace, "job-name", createAdminName).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(namespace, createPodName, "All topics created", GLOBAL_TIMEOUT);
        JobUtils.deleteJobWithWait(namespace, createAdminName);

        // All topics altered
        adminClientJob = new KafkaAdminClientsBuilder(adminClientJob)
            .withAdminName(alterAdminName)
            .withTopicCount(topicAlter)
            .withPartitions(500)
            .withTopicOperation(AdminClientOperations.UPDATE_TOPICS.toString())
            .build();
        resourceManager.createResource(extensionContext, adminClientJob.defaultAdmin());
        String alterPodName = kubeClient(namespace).listPodNamesInSpecificNamespace(namespace, "job-name", alterAdminName).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(
                namespace,
                alterPodName,
                "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.",
                GLOBAL_TIMEOUT
        );
        JobUtils.deleteJobWithWait(namespace, alterAdminName);

        // Teardown - delete all (remaining) topics (within Quota limits)
        String teardownClientName = "teardown-delete";
        KafkaAdminClients deleteAdminClientJob = new KafkaAdminClientsBuilder()
                .withAdminName(teardownClientName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicAlter)
                .withNamespaceName(namespace)
                .withTopicOperation(AdminClientOperations.DELETE_TOPICS.toString())
                .build();
        resourceManager.createResource(extensionContext, deleteAdminClientJob.defaultAdmin());
        ClientUtils.waitForClientSuccess(teardownClientName, namespace, 10);
        JobUtils.deleteJobWithWait(namespace, teardownClientName);
    }

    @ParallelTest
    void testKafkaAdminTopicOperations(ExtensionContext extensionContext) {
        final String kafkaUsername = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String createAdminName = "create-admin-" + mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String deleteAdminName = "delete-admin-" + mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String listAdminName = "list-admin-" + mapWithKafkaClientNames.get(extensionContext.getDisplayName());
        final String topicNamePrefix = classTopicPrefix + "-simple";
        int topicsCountBelowQuota = 100;

        setupKafkaUserInNamespace(extensionContext, kafkaUsername);
        // Create 'topicsCountBelowQuota' topics
        KafkaAdminClients adminClientJob = new KafkaAdminClientsBuilder()
                .withAdminName(createAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicsCountBelowQuota)
                .withNamespaceName(namespace)
                .withTopicOperation(AdminClientOperations.CREATE_TOPICS.toString())
                .withAdditionalConfig(KafkaAdminClients.getAdminClientScramConfig(namespace, kafkaUsername, 240000))
                .build();
        resourceManager.createResource(extensionContext, adminClientJob.defaultAdmin());
        ClientUtils.waitForClientSuccess(createAdminName, namespace, topicsCountBelowQuota);
        String createPodName = kubeClient(namespace).listPodNamesInSpecificNamespace(namespace, "job-name", createAdminName).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(namespace, createPodName, "All topics created");

        // List 'topicsCountBelowQuota' topics
        KafkaAdminClients adminClientListJob = new KafkaAdminClientsBuilder()
                .withAdminName(listAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withNamespaceName(namespace)
                .withTopicOperation(AdminClientOperations.LIST_TOPICS.toString())
                .withAdditionalConfig(KafkaAdminClients.getAdminClientScramConfig(namespace, kafkaUsername, 600000))
                .build();
        resourceManager.createResource(extensionContext, adminClientListJob.defaultAdmin());
        ClientUtils.waitForClientSuccess(listAdminName, namespace, 0);
        String listPodName = kubeClient().listPodNamesInSpecificNamespace(namespace, "job-name", listAdminName).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(namespace, listPodName, topicNamePrefix + "-" + (topicsCountBelowQuota - 1));
        JobUtils.deleteJobWithWait(namespace, listAdminName);

        // Delete 'topicsCountBelowQuota' topics
        adminClientJob = new KafkaAdminClientsBuilder(adminClientJob)
                .withAdminName(deleteAdminName)
                .withTopicOperation(AdminClientOperations.DELETE_TOPICS.toString())
                .build();
        resourceManager.createResource(extensionContext, adminClientJob.defaultAdmin());
        ClientUtils.waitForClientSuccess(deleteAdminName, namespace, 10);
        String deletePodName = kubeClient(namespace).listPodNamesInSpecificNamespace(namespace, "job-name", deleteAdminName).get(0);
        PodUtils.waitUntilMessageIsInPodLogs(namespace, deletePodName, "Successfully removed all " + topicsCountBelowQuota);

        // List topics after deletion
        resourceManager.createResource(extensionContext, adminClientListJob.defaultAdmin());
        ClientUtils.waitForClientSuccess(listAdminName, namespace, 0);
        listPodName = kubeClient(namespace).listPodNamesInSpecificNamespace(namespace, "job-name", listAdminName).get(0);
        String afterDeletePodLogs = kubeClient(namespace).logsInSpecificNamespace(namespace, listPodName);
        assertThat(afterDeletePodLogs.contains(topicNamePrefix), is(false));
        assertThat(afterDeletePodLogs, not(containsString(topicNamePrefix)));

        JobUtils.deleteJobWithWait(namespace, createAdminName);
        JobUtils.deleteJobWithWait(namespace, listAdminName);
        JobUtils.deleteJobWithWait(namespace, deleteAdminName);
    }

    void setupKafkaInNamespace(ExtensionContext extensionContext) {
        // Deploy kafka with ScramSHA512
        LOGGER.info("Deploying shared Kafka across all test cases in {} namespace", namespace);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(CLUSTER_NAME, 1)
            .editMetadata()
                .withNamespace(namespace)
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
            .build());
    }

    void setupKafkaUserInNamespace(ExtensionContext extensionContext, String kafkaUsername) {
        // Deploy KafkaUser with defined Quotas
        KafkaUser kafkaUserWQuota = KafkaUserTemplates.defaultUser(namespace, CLUSTER_NAME, kafkaUsername)
            .editOrNewSpec()
                .withNewQuotas()
                    .withControllerMutationRate(1.0)
                .endQuotas()
                .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
            .endSpec()
            .build();
        resourceManager.createResource(extensionContext, kafkaUserWQuota);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        setupKafkaInNamespace(extensionContext);
    }

    @AfterAll
    void clearTestResources() {
        LOGGER.info("Tearing down resources after all test");
        JobUtils.removeAllJobs(namespace);
        KafkaTopicUtils.deleteAllKafkaTopicsWithPrefix(namespace, classTopicPrefix);
    }
}
