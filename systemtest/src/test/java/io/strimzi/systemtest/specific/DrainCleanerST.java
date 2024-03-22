/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.draincleaner.SetupDrainCleaner;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

@Tag(REGRESSION)
@MicroShiftNotSupported
public class DrainCleanerST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DrainCleanerST.class);
    private static SetupDrainCleaner drainCleaner = new SetupDrainCleaner();

    @Tag(REGRESSION)
    @IsolatedTest
    void testDrainCleanerWithComponents() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.DRAIN_CLEANER_NAMESPACE);

        final int replicas = 3;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), replicas).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), replicas).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), replicas)
            .editMetadata()
                .withNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE)
            .endMetadata()
            .build());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), TestConstants.DRAIN_CLEANER_NAMESPACE).build());

        drainCleaner.createDrainCleaner();
        // allow NetworkPolicies for the webhook in case that we have "default to deny all" mode enabled
        NetworkPolicyResource.allowNetworkPolicySettingsForWebhook(TestConstants.DRAIN_CLEANER_NAMESPACE, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME, Map.of(TestConstants.APP_POD_LABEL, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME));

        final KafkaClients continuousClients = ClientUtils.getContinuousPlainClientBuilder(testStorage)
            .withNamespaceName(TestConstants.DRAIN_CLEANER_NAMESPACE)
            .build();

        resourceManager.createResourceWithWait(
            continuousClients.producerStrimzi(),
            continuousClients.consumerStrimzi());

        List<String> brokerPods = kubeClient().listPodNames(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getBrokerSelector());

        if (!Environment.isKRaftModeEnabled()) {
            String zkPodName = KafkaResources.zookeeperPodName(testStorage.getClusterName(), 0);

            Map<String, String> zkPod = PodUtils.podSnapshot(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getControllerSelector()).entrySet()
                    .stream().filter(snapshot -> snapshot.getKey().equals(zkPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            evictPodWithName(zkPodName);
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getControllerSelector(), replicas, zkPod);
        }

        String kafkaPodName = brokerPods.get(0);

        Map<String, String> kafkaPod = PodUtils.podSnapshot(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getBrokerSelector()).entrySet()
            .stream().filter(snapshot -> snapshot.getKey().equals(kafkaPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        evictPodWithName(kafkaPodName);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getBrokerSelector(), replicas, kafkaPod);

        ClientUtils.waitForClientsSuccess(testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.DRAIN_CLEANER_NAMESPACE, 200);
    }

    private void evictPodWithName(String podName) {
        LOGGER.info("Evicting Pod: {}", podName);

        try {
            kubeClient().getClient().pods().inNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE).withName(podName).evict();
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
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(ResourceManager.getTestContext())
            .withNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE)
            .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_DEFAULT)
            .createInstallation()
            .runInstallation();
    }
}
