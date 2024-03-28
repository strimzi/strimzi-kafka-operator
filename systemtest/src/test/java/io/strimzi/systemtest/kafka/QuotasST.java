/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;

import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.QUOTAS_PLUGIN;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * NOTE: STs in this class will not properly work on `minikube` clusters (and maybe not on other clusters that uses local
 * storage), because the calculation of currently used storage is based
 * on the local storage, which can be shared across multiple Docker containers.
 * To properly run this suite, you should use cluster with proper storage.
 */
@Tag(QUOTAS_PLUGIN)
public class QuotasST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(QuotasST.class);

    /**
     * Test to check Kafka Quotas Plugin for disk space
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
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
            KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1)
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
            KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build(),
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
