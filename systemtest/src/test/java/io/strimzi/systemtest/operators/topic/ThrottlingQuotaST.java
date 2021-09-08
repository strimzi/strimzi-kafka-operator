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
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaAdminClient;
import io.strimzi.systemtest.resources.crd.kafkaclients.AdminClientOperations;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

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
public class ThrottlingQuotaST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ThrottlingQuotaST.class);
    static final String NAMESPACE = "throttling-quota-cluster-test";
    static final String CLUSTER_NAME = "quota-cluster";
    final String classTopicPrefix = "quota-topic-test";

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

        KafkaAdminClient adminClientJob = new KafkaAdminClient.Builder()
                .withAdminName(createAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicsCountOverQuota)
                .withNamespaceName(NAMESPACE)
                .withTopicOperation(AdminClientOperations.CREATE_TOPICS.toString())
                .withAdditionalConfig(getAdminClientConfig(kafkaUsername, 240000))
                .build();
        resourceManager.createResource(extensionContext, true, adminClientJob.defaultAdmin().build());
        String createPodName = kubeClient().listPods("job-name", createAdminName).get(0).getMetadata().getName();
        PodUtils.waitUntilMessageIsInPodLogs(
                createPodName,
                "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.",
                GLOBAL_TIMEOUT
        );
        JobUtils.deleteJobWithWait(NAMESPACE, createAdminName);

        // Teardown delete created topics
        KafkaTopicUtils.deleteAllKafkaTopicsWithPrefix(NAMESPACE, topicNamePrefix);
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
        KafkaAdminClient createAdminClientJob;
        String createPodName;
        int offset = 100;
        int iterations = topicsCountOverQuota / 100;
        for (int i = 0; i < iterations; i++) {
            LOGGER.info("Executing {}/{} iteration.", i + 1, iterations);
            createAdminClientJob = new KafkaAdminClient.Builder()
                .withAdminName(createAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(100)
                .withTopicOffset(i * offset)
                .withNamespaceName(NAMESPACE)
                .withTopicOperation(AdminClientOperations.CREATE_TOPICS.toString())
                .withAdditionalConfig(getAdminClientConfig(kafkaUsername, 240000))
                .build();
            resourceManager.createResource(extensionContext, true, createAdminClientJob.defaultAdmin().build());
            ClientUtils.waitForClientSuccess(createAdminName, NAMESPACE, 100);
            createPodName = kubeClient().listPods("job-name", createAdminName).get(0).getMetadata().getName();
            PodUtils.waitUntilMessageIsInPodLogs(createPodName, "All topics created", Constants.GLOBAL_TIMEOUT);
            JobUtils.deleteJobWithWait(NAMESPACE, createAdminName);
        }

        // Test delete all topics at once - should fail on Throttling Quota limit
        KafkaAdminClient deleteAdminClientJob = new KafkaAdminClient.Builder()
                .withAdminName(deleteAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicsCountOverQuota)
                .withNamespaceName(NAMESPACE)
                .withTopicOperation(AdminClientOperations.DELETE_TOPICS.toString())
                .withAdditionalConfig(getAdminClientConfig(kafkaUsername, 240000))
                .build();
        resourceManager.createResource(extensionContext, deleteAdminClientJob.defaultAdmin().build());
        String deletePodName = kubeClient().listPods("job-name", deleteAdminName).get(0).getMetadata().getName();
        PodUtils.waitUntilMessageIsInPodLogs(
                deletePodName,
                "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.",
                Constants.GLOBAL_TIMEOUT
        );
        JobUtils.deleteJobWithWait(NAMESPACE, deleteAdminName);

        // Teardown - delete all (remaining) topics (within Quota limits) in multiple rounds using starting offsets
        for (int i = 0; i < iterations; i++) {
            LOGGER.info("Executing {}/{} iteration for {}.", i + 1, iterations, deleteAdminName);
            deleteAdminClientJob = deleteAdminClientJob.toBuilder()
                    .withTopicOffset(i * offset)
                    .withTopicCount(100)
                    .withAdditionalConfig("")
                    .build();
            resourceManager.createResource(extensionContext, deleteAdminClientJob.defaultAdmin().build());
            ClientUtils.waitForClientSuccess(deleteAdminName, NAMESPACE, 10);
            JobUtils.deleteJobWithWait(NAMESPACE, deleteAdminName);
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

        KafkaAdminClient adminClientJob = new KafkaAdminClient.Builder()
                .withAdminName(createAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicsCount)
                .withPartitions(topicPartitions)
                .withNamespaceName(NAMESPACE)
                .withTopicOperation(AdminClientOperations.CREATE_TOPICS.toString())
                .withAdditionalConfig(getAdminClientConfig(kafkaUsername, 240000))
                .build();
        resourceManager.createResource(extensionContext, true, adminClientJob.defaultAdmin().build());
        String createPodName = kubeClient().listPods("job-name", createAdminName).get(0).getMetadata().getName();
        PodUtils.waitUntilMessageIsInPodLogs(
                createPodName,
                "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.",
                Constants.GLOBAL_TIMEOUT
        );
        JobUtils.deleteJobWithWait(NAMESPACE, createAdminName);

        // Delete created topics (as they were created in random order, we have to use KafkaTopic instead of this client)
        KafkaTopicUtils.deleteAllKafkaTopicsWithPrefix(NAMESPACE, topicNamePrefix);

        // Throttling quota after performed 'alter' partitions on existing topic
        int topicAlter = 20;
        adminClientJob = adminClientJob.toBuilder()
                .withTopicCount(topicAlter)
                .withPartitions(1)
                .build();
        resourceManager.createResource(extensionContext, true, adminClientJob.defaultAdmin().build());
        createPodName = kubeClient().listPods("job-name", createAdminName).get(0).getMetadata().getName();
        PodUtils.waitUntilMessageIsInPodLogs(createPodName, "All topics created", GLOBAL_TIMEOUT);
        JobUtils.deleteJobWithWait(NAMESPACE, createAdminName);

        // All topics altered
        adminClientJob = adminClientJob.toBuilder()
            .withAdminName(alterAdminName)
            .withTopicCount(topicAlter)
            .withPartitions(500)
            .withTopicOperation(AdminClientOperations.UPDATE_TOPICS.toString())
            .build();
        resourceManager.createResource(extensionContext, true, adminClientJob.defaultAdmin().build());
        String alterPodName = kubeClient().listPods("job-name", alterAdminName).get(0).getMetadata().getName();
        PodUtils.waitUntilMessageIsInPodLogs(
                alterPodName,
                "org.apache.kafka.common.errors.ThrottlingQuotaExceededException: The throttling quota has been exceeded.",
                GLOBAL_TIMEOUT
        );
        JobUtils.deleteJobWithWait(NAMESPACE, alterAdminName);

        // Teardown - delete all (remaining) topics (within Quota limits)
        String teardownClientName = "teardown-delete";
        KafkaAdminClient deleteAdminClientJob = new KafkaAdminClient.Builder()
                .withAdminName(teardownClientName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicAlter)
                .withNamespaceName(NAMESPACE)
                .withTopicOperation(AdminClientOperations.DELETE_TOPICS.toString())
                .build();
        resourceManager.createResource(extensionContext, deleteAdminClientJob.defaultAdmin().build());
        ClientUtils.waitForClientSuccess(teardownClientName, NAMESPACE, 10);
        JobUtils.deleteJobWithWait(NAMESPACE, teardownClientName);
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
        KafkaAdminClient adminClientJob = new KafkaAdminClient.Builder()
                .withAdminName(createAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(topicNamePrefix)
                .withTopicCount(topicsCountBelowQuota)
                .withNamespaceName(NAMESPACE)
                .withTopicOperation(AdminClientOperations.CREATE_TOPICS.toString())
                .withAdditionalConfig(getAdminClientConfig(kafkaUsername, 240000))
                .build();
        resourceManager.createResource(extensionContext, adminClientJob.defaultAdmin().build());
        ClientUtils.waitForClientSuccess(createAdminName, NAMESPACE, topicsCountBelowQuota);
        String createPodName = kubeClient().listPods("job-name", createAdminName).get(0).getMetadata().getName();
        PodUtils.waitUntilMessageIsInPodLogs(createPodName, "All topics created");

        // List 'topicsCountBelowQuota' topics
        KafkaAdminClient adminClientListJob = new KafkaAdminClient.Builder()
                .withAdminName(listAdminName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withNamespaceName(NAMESPACE)
                .withTopicOperation(AdminClientOperations.LIST_TOPICS.toString())
                .withAdditionalConfig(getAdminClientConfig(kafkaUsername, 600000))
                .build();
        resourceManager.createResource(extensionContext, adminClientListJob.defaultAdmin().build());
        ClientUtils.waitForClientSuccess(listAdminName, NAMESPACE, 0);
        String listPodName = kubeClient().listPods("job-name", listAdminName).get(0).getMetadata().getName();
        PodUtils.waitUntilMessageIsInPodLogs(listPodName, topicNamePrefix + "-" + (topicsCountBelowQuota - 1));
        JobUtils.deleteJobWithWait(NAMESPACE, listAdminName);

        // Delete 'topicsCountBelowQuota' topics
        adminClientJob = adminClientJob.toBuilder()
                .withAdminName(deleteAdminName)
                .withTopicOperation(AdminClientOperations.DELETE_TOPICS.toString())
                .build();
        resourceManager.createResource(extensionContext, adminClientJob.defaultAdmin().build());
        ClientUtils.waitForClientSuccess(deleteAdminName, NAMESPACE, 10);
        String deletePodName = kubeClient().listPods("job-name", deleteAdminName).get(0).getMetadata().getName();
        PodUtils.waitUntilMessageIsInPodLogs(deletePodName, "Successfully removed all " + topicsCountBelowQuota);

        // List topics after deletion
        resourceManager.createResource(extensionContext, adminClientListJob.defaultAdmin().build());
        ClientUtils.waitForClientSuccess(listAdminName, NAMESPACE, 0);
        listPodName = kubeClient().listPods("job-name", listAdminName).get(0).getMetadata().getName();
        String afterDeletePodLogs = kubeClient().logs(listPodName);
        assertThat(afterDeletePodLogs.contains(topicNamePrefix), is(false));
        assertThat(afterDeletePodLogs, not(containsString(topicNamePrefix)));

        JobUtils.deleteJobWithWait(NAMESPACE, createAdminName);
        JobUtils.deleteJobWithWait(NAMESPACE, listAdminName);
        JobUtils.deleteJobWithWait(NAMESPACE, deleteAdminName);
    }

    private String getAdminClientConfig(String kafkaUsername, int timeoutMs) {
        final String saslJaasConfigEncrypted = kubeClient().getSecret(NAMESPACE, kafkaUsername).getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = new String(Base64.getDecoder().decode(saslJaasConfigEncrypted), StandardCharsets.US_ASCII);
        return "sasl.mechanism=SCRAM-SHA-512\n" +
                "security.protocol=" + SecurityProtocol.SASL_PLAINTEXT + "\n" +
                "sasl.jaas.config=" + saslJaasConfigDecrypted + "\n" +
                "request.timeout.ms=" + timeoutMs;
    }

    void setupKafkaInNamespace(ExtensionContext extensionContext) {
        // Deploy kafka with ScramSHA512
        LOGGER.info("Deploying shared kafka across all test cases in {} namespace", NAMESPACE);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(CLUSTER_NAME, 1)
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
        KafkaUtils.waitForKafkaReady(NAMESPACE, CLUSTER_NAME);
    }

    void setupKafkaUserInNamespace(ExtensionContext extensionContext, String kafkaUsername) {
        // Deploy KafkaUser with defined Quotas
        KafkaUser kafkaUserWQuota = KafkaUserTemplates.defaultUser(NAMESPACE, CLUSTER_NAME, kafkaUsername)
            .editOrNewSpec()
                .withNewQuotas()
                    .withControllerMutationRate(1.0)
                .endQuotas()
                .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
            .endSpec()
            .build();
        resourceManager.createResource(extensionContext, true, kafkaUserWQuota);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(NAMESPACE)
            .createInstallation()
            .runInstallation();
        setupKafkaInNamespace(extensionContext);
    }

    @AfterAll
    void clearTestResources() {
        LOGGER.info("Tearing down resources after all test");
        JobUtils.removeAllJobs(NAMESPACE);
        KafkaTopicUtils.deleteAllKafkaTopicsWithPrefix(NAMESPACE, classTopicPrefix);
    }
}
