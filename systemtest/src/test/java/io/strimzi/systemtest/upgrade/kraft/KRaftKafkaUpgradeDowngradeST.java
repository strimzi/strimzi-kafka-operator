/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.kraft;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.KRAFT_UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * This test class contains tests for Kafka upgrade/downgrade from version X to X +/- 1, running in KRaft mode.
 * Metadata for upgrade/downgrade procedure are loaded from kafka-versions.yaml in root dir of this repository.
 */
@Tag(KRAFT_UPGRADE)
public class KRaftKafkaUpgradeDowngradeST extends AbstractKRaftUpgradeST {
    private static final Logger LOGGER = LogManager.getLogger(KRaftKafkaUpgradeDowngradeST.class);
    private final int continuousClientsMessageCount = 500;

    @IsolatedTest
    void testKafkaClusterUpgrade() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();

        for (int x = 0; x < sortedVersions.size() - 1; x++) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x + 1);

            // If it is an upgrade test we keep the metadata version as the lower version number
            String metadataVersion = initialVersion.metadataVersion();

            runVersionChange(initialVersion, newVersion, testStorage, metadataVersion, 3, 3);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForClientsSuccess(testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @IsolatedTest
    void testKafkaClusterDowngrade() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();

        for (int x = sortedVersions.size() - 1; x > 0; x--) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x - 1);

            // If it is a downgrade then we make sure that we are using the lowest metadataVersion from the whole list
            String metadataVersion = sortedVersions.get(0).metadataVersion();
            runVersionChange(initialVersion, newVersion, testStorage, metadataVersion, 3, 3);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForClientsSuccess(testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @IsolatedTest
    void testUpgradeWithNoMetadataVersionSet() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();

        for (int x = 0; x < sortedVersions.size() - 1; x++) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x + 1);

            runVersionChange(initialVersion, newVersion, testStorage, null, 3, 3);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForClientsSuccess(testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @BeforeAll
    void setupEnvironment() {
        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, String.join(",",
            TestConstants.USE_KRAFT_MODE), null));

        clusterOperator
            .defaultInstallation()
            .withExtraEnvVars(coEnvVars)
            .createInstallation()
            .runInstallation();
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    void runVersionChange(TestKafkaVersion initialVersion, TestKafkaVersion newVersion, TestStorage testStorage, String initMetadataVersion, int controllerReplicas, int brokerReplicas) {
        boolean isUpgrade = initialVersion.isUpgrade(newVersion);
        Map<String, String> controllerPods;
        Map<String, String> brokerPods;

        boolean sameMinorVersion = initialVersion.metadataVersion().equals(newVersion.metadataVersion());

        if (KafkaResource.kafkaClient().inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName).get() == null) {
            LOGGER.info("Deploying initial Kafka version {} with metadataVersion={}", initialVersion.version(), initMetadataVersion);

            KafkaBuilder kafka = KafkaTemplates.kafkaPersistent(clusterName, controllerReplicas, brokerReplicas)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withVersion(initialVersion.version())
                        .withConfig(null)
                    .endKafka()
                .endSpec();

            // Do not set metadataVersion if it's not passed to method
            if (initMetadataVersion != null) {
                kafka
                    .editSpec()
                        .editKafka()
                            .withMetadataVersion(initMetadataVersion)
                        .endKafka()
                    .endSpec();
            }

            resourceManager.createResourceWithWait(
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(TestConstants.CO_NAMESPACE, CONTROLLER_NODE_NAME, clusterName, controllerReplicas).build(),
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(TestConstants.CO_NAMESPACE, BROKER_NODE_NAME, clusterName, brokerReplicas).build(),
                kafka.build()
            );

            // ##############################
            // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
            // ##############################
            // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
            resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(clusterName, testStorage.getContinuousTopicName(), 3, 3, 2, TestConstants.CO_NAMESPACE).build());
            String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

            KafkaClients kafkaBasicClientJob = ClientUtils.getContinuousPlainClientBuilder(testStorage)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withMessageCount(continuousClientsMessageCount)
                .withAdditionalConfig(producerAdditionConfiguration)
                .build();

            resourceManager.createResourceWithWait(kafkaBasicClientJob.producerStrimzi());
            resourceManager.createResourceWithWait(kafkaBasicClientJob.consumerStrimzi());
            // ##############################
        }

        LOGGER.info("Deployment of initial Kafka version (" + initialVersion.version() + ") complete");

        String controllerVersionResult = KafkaResource.kafkaClient().inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName).get().getStatus().getKafkaVersion();
        LOGGER.info("Pre-change Kafka version: " + controllerVersionResult);

        controllerPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, controllerSelector);
        brokerPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, brokerSelector);

        LOGGER.info("Updating Kafka CR version field to " + newVersion.version());

        // Change the version in Kafka CR
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setVersion(newVersion.version());
        }, TestConstants.CO_NAMESPACE);

        LOGGER.info("Waiting for readiness of new Kafka version (" + newVersion.version() + ") to complete");

        // Wait for the controllers' version change roll
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, controllerSelector, controllerReplicas, controllerPods);
        LOGGER.info("1st Controllers roll (image change) is complete");

        // Wait for the brokers' version change roll
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, brokerSelector, brokerReplicas, brokerPods);
        LOGGER.info("1st Brokers roll (image change) is complete");

        String currentMetadataVersion = KafkaResource.kafkaClient().inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName).get().getSpec().getKafka().getMetadataVersion();

        LOGGER.info("Deployment of Kafka (" + newVersion.version() + ") complete");

        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);

        String controllerPodName = kubeClient().listPodsByPrefixInName(TestConstants.CO_NAMESPACE, KafkaResource.getStrimziPodSetName(clusterName, CONTROLLER_NODE_NAME)).get(0).getMetadata().getName();
        String brokerPodName = kubeClient().listPodsByPrefixInName(TestConstants.CO_NAMESPACE, KafkaResource.getStrimziPodSetName(clusterName, BROKER_NODE_NAME)).get(0).getMetadata().getName();

        // Extract the Kafka version number from the jars in the lib directory
        controllerVersionResult = KafkaUtils.getVersionFromKafkaPodLibs(controllerPodName);
        LOGGER.info("Post-change Kafka version query returned: " + controllerVersionResult);

        assertThat("Kafka container had version " + controllerVersionResult + " where " + newVersion.version() +
            " was expected", controllerVersionResult, is(newVersion.version()));

        // Extract the Kafka version number from the jars in the lib directory
        String brokerVersionResult = KafkaUtils.getVersionFromKafkaPodLibs(brokerPodName);
        LOGGER.info("Post-change Kafka version query returned: " + brokerVersionResult);

        assertThat("Kafka container had version " + brokerVersionResult + " where " + newVersion.version() +
            " was expected", brokerVersionResult, is(newVersion.version()));

        if (isUpgrade && !sameMinorVersion) {
            LOGGER.info("Updating Kafka config attribute 'metadataVersion' from '{}' to '{}' version", initialVersion.metadataVersion(), newVersion.metadataVersion());

            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
                LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().toString());

                kafka.getSpec().getKafka().setMetadataVersion(newVersion.metadataVersion());

                LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().toString());
            }, TestConstants.CO_NAMESPACE);

            LOGGER.info("Metadata version changed, it doesn't require rolling update, so the Pods should be stable");
            PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);
            assertFalse(RollingUpdateUtils.componentHasRolled(TestConstants.CO_NAMESPACE, controllerSelector, controllerPods));
            assertFalse(RollingUpdateUtils.componentHasRolled(TestConstants.CO_NAMESPACE, brokerSelector, brokerPods));
        }

        if (!isUpgrade) {
            LOGGER.info("Verifying that metadataVersion attribute updated correctly to version {}", initMetadataVersion);
            assertThat(Crds.kafkaOperation(kubeClient().getClient()).inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName)
                .get().getStatus().getKafkaMetadataVersion().contains(initMetadataVersion), is(true));
        } else {
            if (currentMetadataVersion != null) {
                LOGGER.info("Verifying that metadataVersion attribute updated correctly to version {}", newVersion.metadataVersion());
                assertThat(Crds.kafkaOperation(kubeClient().getClient()).inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName)
                    .get().getStatus().getKafkaMetadataVersion().contains(newVersion.metadataVersion()), is(true));
            }
        }

        LOGGER.info("Waiting till Kafka Cluster {}/{} with specified version {} has the same version in status and specification", TestConstants.CO_NAMESPACE, clusterName, newVersion.version());
        KafkaUtils.waitUntilStatusKafkaVersionMatchesExpectedVersion(clusterName, TestConstants.CO_NAMESPACE, newVersion.version());
    }
}
