/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@SuiteDoc(
    description = @Desc("NOTE: STs in this class will not properly work on `minikube` clusters (and maybe not on other clusters that use local storage), because the calculation of currently used storage is based on the local storage, which can be shared across multiple Docker containers. To properly run this suite, you should use cluster with proper storage."),
    beforeTestSteps = {
        @Step(value = "Deploy default cluster operator with the required configurations", expected = "Cluster operator is deployed")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class QuotasST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(QuotasST.class);

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test to check Kafka Quotas Plugin for disk space."),
        steps = {
            @Step(value = "Assume the cluster is not Minikube or MicroShift", expected = "Cluster is appropriate for the test"),
            @Step(value = "Create necessary resources for Kafka and nodes", expected = "Resources are created and Kafka is set up with quotas plugin"),
            @Step(value = "Send messages without any user; observe quota enforcement", expected = "Producer stops after reaching the minimum available bytes"),
            @Step(value = "Check Kafka logs for quota enforcement message", expected = "Kafka logs contain the expected quota enforcement message"),
            @Step(value = "Send messages with excluded user and observe the behavior", expected = "Messages are sent successfully without hitting the quota"),
            @Step(value = "Clean up resources", expected = "Resources are deleted successfully")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testKafkaQuotasPluginIntegration() {
        assumeFalse(cluster.isMinikube() || cluster.isMicroShift());

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String excludedPrincipal = "User:" + testStorage.getUsername();
        final String minAvailableBytes = "800000000";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaPersistent(testStorage.getNamespaceName(), testStorage.getClusterName(), 1, 1)
                .editSpec()
                    .editKafka()
                        .addToConfig("client.quota.callback.static.storage.check-interval", "5")
                        .withNewQuotasPluginStrimziQuotas()
                            .addToExcludedPrincipals(excludedPrincipal)
                            .withProducerByteRate(10000000L)
                            .withConsumerByteRate(10000000L)
                            .withMinAvailableBytesPerVolume(Long.valueOf(minAvailableBytes))
                        .endQuotasPluginStrimziQuotas()
                        .withListeners(
                            new GenericKafkaListenerBuilder()
                                .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName("scramsha")
                                .withPort(9095)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withNewKafkaListenerAuthenticationScramSha512Auth()
                                .endKafkaListenerAuthenticationScramSha512Auth()
                                .build()
                        )
                    .endKafka()
                .endSpec()
                .build()
        );
        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build()
        );

        KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(100000)
            .withMessage(String.join("", Collections.nCopies(10000, "#")))
            .withAdditionalConfig("delivery.timeout.ms=10000\nrequest.timeout.ms=10000\n")
            .build();

        LOGGER.info("Sending messages without any user, we should hit the quota");
        resourceManager.createResourceWithWait(clients.producerStrimzi());
        // Kafka Quotas Plugin should stop producer after it reaches the minimum available bytes
        JobUtils.waitForJobContainingLogMessage(testStorage.getNamespaceName(), testStorage.getProducerName(), "Failed to send messages");
        JobUtils.deleteJobWithWait(testStorage.getNamespaceName(), testStorage.getProducerName());

        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        String kafkaLog = kubeClient().logsInSpecificNamespace(testStorage.getNamespaceName(), brokerPodName);

        String belowLimitLog = String.format("below the limit of %s", minAvailableBytes);

        assertThat("Kafka log doesn't contain '" + belowLimitLog + "' log", kafkaLog, CoreMatchers.containsString(belowLimitLog));

        LOGGER.info("Sending messages with user that is specified in list of excluded principals, we should be able to send the messages without problem");

        clients = ClientUtils.getInstantScramShaClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9095").build();

        resourceManager.createResourceWithWait(clients.producerScramShaPlainStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(REGRESSION)
    @TestDoc(
        description = @Desc("Test verifying bandwidth limitations with Kafka quotas plugin."),
        steps = {
            @Step(value = "Set excluded principal", expected = "Principal is set"),
            @Step(value = "Create Kafka resources including node pools and persistent Kafka with quotas enabled", expected = "Kafka resources are created successfully with quotas setup"),
            @Step(value = "Create Kafka topic and user with SCRAM-SHA authentication", expected = "Kafka topic and SCRAM-SHA user are created successfully"),
            @Step(value = "Send messages with normal user", expected = "Messages are sent and duration is measured"),
            @Step(value = "Send messages with excluded user", expected = "Messages are sent and duration is measured"),
            @Step(value = "Assert that time taken for normal user is greater than for excluded user", expected = "Assertion is successful")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testKafkaQuotasPluginWithBandwidthLimitation() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String excludedPrincipal = "User:" + testStorage.getUsername();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafkaPersistent(testStorage.getNamespaceName(), testStorage.getClusterName(), 1, 1)
                .editSpec()
                    .editKafka()
                    .addToConfig("client.quota.callback.static.storage.check-interval", "5")
                    .withNewQuotasPluginStrimziQuotas()
                        .addToExcludedPrincipals(excludedPrincipal)
                        .withProducerByteRate(1000L)    // 1 kB/s
                        .withConsumerByteRate(1000L)    // 1 kB/s
                    .endQuotasPluginStrimziQuotas()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName("scramsha")
                            .withPort(9095)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                            .build()
                    )
                    .endKafka()
                .endSpec()
                .build()
        );
        resourceManager.createResourceWithWait(
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName()).build(),
            KafkaUserTemplates.scramShaUser(testStorage).build()
        );

        KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(100)
            .withMessage(String.join("", Collections.nCopies(2000, "#")))
            .build();

        LOGGER.info("Sending messages with normal user, quota applies");
        long startTimeNormal = System.currentTimeMillis();
        resourceManager.createResourceWithWait(clients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);
        long endTimeNormal = System.currentTimeMillis();
        long durationNormal = endTimeNormal - startTimeNormal;
        LOGGER.info("Time taken for normal user: {} ms", durationNormal);

        // Measure time for excluded user
        LOGGER.info("Sending messages with excluded user, no quota applies");
        clients = ClientUtils.getInstantScramShaClientBuilder(testStorage, KafkaResources.bootstrapServiceName(testStorage.getClusterName()) + ":9095").build();

        long startTimeExcluded = System.currentTimeMillis();
        resourceManager.createResourceWithWait(clients.producerScramShaPlainStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);
        long endTimeExcluded = System.currentTimeMillis();
        long durationExcluded = endTimeExcluded - startTimeExcluded;
        LOGGER.info("Time taken for excluded user: {} ms", durationExcluded);

        // Assert that time taken with normal user is greater than with excluded user
        assertThat("Time taken for normal user should be greater than time taken for excluded user", durationNormal, Matchers.greaterThan(durationExcluded));
    }

    @AfterEach
    void afterEach() {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(Environment.TEST_SUITE_NAMESPACE, ResourceManager.getTestContext());
        kubeClient().getClient().persistentVolumeClaims().inNamespace(namespaceName).delete();
    }

    @BeforeAll
    void setup() {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }
}
