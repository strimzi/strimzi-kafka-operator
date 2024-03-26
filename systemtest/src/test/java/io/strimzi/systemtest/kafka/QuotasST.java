/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.test.WaitException;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;

import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.QUOTAS_PLUGIN;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * NOTE: STs in this class will not properly work on `minikube` clusters (and maybe not on other clusters that uses local
 * storage), because the calculation of currently used storage is based
 * on the local storage, which can be shared across multiple Docker containers.
 * To properly run this suite, you should use cluster with proper storage.
 */
@Tag(QUOTAS_PLUGIN)
public class QuotasST extends AbstractST {

    /**
     * Test to check Kafka Quotas Plugin for disk space
     */
    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testKafkaQuotasPluginIntegration() {
        assumeFalse(cluster.isMinikube() || cluster.isMicroShift());

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1)
            .editSpec()
                .editKafka()
                    .addToConfig("client.quota.callback.class", "io.strimzi.kafka.quotas.StaticQuotaCallback")
                    .addToConfig("client.quota.callback.static.storage.hard", "55000000")
                    .addToConfig("client.quota.callback.static.storage.soft", "20000000")
                    .addToConfig("client.quota.callback.static.storage.check-interval", "5")
                .endKafka()
            .endSpec()
            .build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), testStorage.getNamespaceName()).build());

        final KafkaClients clients = ClientUtils.getInstantPlainClientBuilder(testStorage)
            .withMessageCount(100000)
            .withMessage(String.join("", Collections.nCopies(1000, "#")))
            .build();

        resourceManager.createResourceWithWait(clients.producerStrimzi());
        // Kafka Quotas Plugin should stop producer in around 10-20 seconds with configured throughput
        assertThrows(WaitException.class, () -> JobUtils.waitForJobFailure(testStorage.getProducerName(), Environment.TEST_SUITE_NAMESPACE, 120_000));

        String brokerPodName = kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBrokerSelector()).get(0).getMetadata().getName();
        String kafkaLog = kubeClient().logsInSpecificNamespace(testStorage.getNamespaceName(), brokerPodName);
        String softLimitLog = "disk is beyond soft limit";
        String hardLimitLog = "disk is full";
        assertThat("Kafka log doesn't contain '" + softLimitLog + "' log", kafkaLog, CoreMatchers.containsString(softLimitLog));
        assertThat("Kafka log doesn't contain '" + hardLimitLog + "' log", kafkaLog, CoreMatchers.containsString(hardLimitLog));
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
