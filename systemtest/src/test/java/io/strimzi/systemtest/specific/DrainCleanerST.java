/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.resources.draincleaner.SetupDrainCleaner;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumer;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(REGRESSION)
@MicroShiftNotSupported
public class DrainCleanerST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DrainCleanerST.class);
    private static final SetupDrainCleaner DRAIN_CLEANER = new SetupDrainCleaner();

    @Tag(REGRESSION)
    @IsolatedTest
    void testDrainCleanerWithComponents() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext(), TestConstants.DRAIN_CLEANER_NAMESPACE);

        final int replicas = 3;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), replicas).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), replicas).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getClusterName(), replicas).build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getTopicName(), testStorage.getClusterName()).build());

        DRAIN_CLEANER.createDrainCleaner();
        // allow NetworkPolicies for the webhook in case that we have "default to deny all" mode enabled
        NetworkPolicyUtils.allowNetworkPolicySettingsForWebhook(TestConstants.DRAIN_CLEANER_NAMESPACE, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME, Map.of(TestConstants.APP_POD_LABEL, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME));

        final KafkaProducerConsumer continuousKafkaProducerConsumer = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getContinuousProducerName())
            .withConsumerName(testStorage.getContinuousConsumerName())
            .withNamespaceName(TestConstants.DRAIN_CLEANER_NAMESPACE)
            .withTopicName(testStorage.getContinuousTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getContinuousMessageCount())
            .withDelayMs(1000)
            .build();

        KubeResourceManager.get().createResourceWithWait(
            continuousKafkaProducerConsumer.getProducer().getJob(),
            continuousKafkaProducerConsumer.getConsumer().getJob()
        );

        List<String> brokerPods = PodUtils.listPodNames(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getBrokerSelector());

        String kafkaPodName = brokerPods.get(0);

        Map<String, String> kafkaPod = PodUtils.podSnapshot(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getBrokerSelector()).entrySet()
            .stream().filter(snapshot -> snapshot.getKey().equals(kafkaPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        evictPodWithName(kafkaPodName);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getBrokerSelector(), replicas, kafkaPod);

        ClientUtils.waitForClientsSuccess(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getContinuousConsumerName(), testStorage.getContinuousProducerName(), testStorage.getContinuousMessageCount());
    }

    private void evictPodWithName(String podName) {
        LOGGER.info("Evicting Pod: {}", podName);

        try {
            KubeResourceManager.get().kubeClient().getClient().pods().inNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE).withName(podName).evict();
        } catch (KubernetesClientException e)   {
            if (e.getCode() == 500 && e.getMessage().contains("The pod will be rolled by the Strimzi Cluster Operator"))    {
                LOGGER.info("Eviction request for pod {} was denied by the Drain Cleaner", podName);
            } else {
                throw e;
            }
        }
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespaceName(TestConstants.DRAIN_CLEANER_NAMESPACE)
                .withNamespacesToWatch(TestConstants.DRAIN_CLEANER_NAMESPACE)
                .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_DEFAULT)
                .build()
            )
            .install();
    }
}
