/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QuotasST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(QuotasST.class);

    public static final String NAMESPACE = "quotas";
    /**
     * Test to check Kafka Quotas Plugin for disk space
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaQuotasPluginIntegration(ExtensionContext extensionContext) {
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        final String producerName = "quotas-producer";
        final String consumerName = "quotas-consumer";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 1)
            .editSpec()
                .editKafka()
                    .addToConfig("client.quota.callback.class", "io.strimzi.kafka.quotas.StaticQuotaCallback")
                    .addToConfig("client.quota.callback.static.storage.hard", "55000000")
                    .addToConfig("client.quota.callback.static.storage.soft", "50000000")
                    .addToConfig("client.quota.callback.static.storage.check-interval", "5")
                    .withNewPersistentClaimStorage()
                        .withNewSize("1Gi")
                    .endPersistentClaimStorage()
                .endKafka()
            .endSpec()
            .build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        // Send more messages than disk can store to see if the integration works
        KafkaBasicExampleClients basicClients = new KafkaBasicExampleClients.Builder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(topicName)
            .withMessageCount(100000000)
            .withDelayMs(0)
            .withMessage(String.join("", Collections.nCopies(1000, "#")))
            .build();

        resourceManager.createResource(extensionContext, basicClients.producerStrimzi().build());
        // Kafka Quotas Plugin should stop producer in around 10-20 seconds with configured throughput
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(producerName, NAMESPACE, 120_000));

        String kafkaLog = kubeClient(namespaceName).logs(KafkaResources.kafkaPodName(clusterName, 0));
        String softLimitLog = "disk is beyond soft limit";
        String hardLimitLog = "disk is full";
        assertThat("Kafka log doesn't contain '" + softLimitLog + "' log", kafkaLog, CoreMatchers.containsString(softLimitLog));
        assertThat("Kafka log doesn't contain '" + hardLimitLog + "' log", kafkaLog, CoreMatchers.containsString(hardLimitLog));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterWideClusterOperator(extensionContext, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    @AfterEach
    void afterEach(ExtensionContext extensionContext) throws Exception {
        final String namespaceName = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
        kubeClient(namespaceName).getClient().persistentVolumeClaims().inNamespace(namespaceName).delete();
    }
}
