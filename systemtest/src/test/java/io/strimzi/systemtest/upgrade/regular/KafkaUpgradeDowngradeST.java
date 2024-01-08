/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.regular;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.upgrade.AbstractUpgradeST;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * This test class contains tests for Kafka upgrade/downgrade from version X to X +/- 1.
 * Metadata for upgrade/downgrade procedure are loaded from kafka-versions.yaml in root dir of this repository.
 */
@Tag(UPGRADE)
@KRaftNotSupported("Strimzi and Kafka downgrade is not supported with KRaft mode")
public class KafkaUpgradeDowngradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaUpgradeDowngradeST.class);

    private final String continuousTopicName = "continuous-topic";
    private final int continuousClientsMessageCount = 1000;

    @IsolatedTest
    void testKafkaClusterUpgrade(ExtensionContext testContext) {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();

        String producerName = clusterName + "-producer";
        String consumerName = clusterName + "-consumer";

        for (int x = 0; x < sortedVersions.size() - 1; x++) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x + 1);

            // If it is an upgrade test we keep the message format as the lower version number
            String logMsgFormat = initialVersion.messageVersion();
            String interBrokerProtocol = initialVersion.protocolVersion();
            runVersionChange(initialVersion, newVersion, producerName, consumerName, logMsgFormat, interBrokerProtocol, 3, 3, testContext);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForClientsSuccess(producerName, consumerName, TestConstants.CO_NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @IsolatedTest
    void testKafkaClusterDowngrade(ExtensionContext testContext) {
        final TestStorage testStorage = storageMap.get(testContext);
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();

        String clusterName = testStorage.getClusterName();
        String producerName = clusterName + "-producer";
        String consumerName = clusterName + "-consumer";

        for (int x = sortedVersions.size() - 1; x > 0; x--) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x - 1);

            // If it is a downgrade then we make sure to use the lower version number for the message format
            String logMsgFormat = newVersion.messageVersion();
            String interBrokerProtocol = newVersion.protocolVersion();
            runVersionChange(initialVersion, newVersion, producerName, consumerName, logMsgFormat, interBrokerProtocol, 3, 3, testContext);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForClientsSuccess(producerName, consumerName, TestConstants.CO_NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @IsolatedTest
    void testKafkaClusterDowngradeToOlderMessageFormat(ExtensionContext testContext) {
        final TestStorage testStorage = storageMap.get(testContext);
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();

        String clusterName = testStorage.getClusterName();
        String producerName = clusterName + "-producer";
        String consumerName = clusterName + "-consumer";

        String initLogMsgFormat = sortedVersions.get(0).messageVersion();
        String initInterBrokerProtocol = sortedVersions.get(0).protocolVersion();

        for (int x = sortedVersions.size() - 1; x > 0; x--) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x - 1);

            runVersionChange(initialVersion, newVersion, producerName, consumerName, initLogMsgFormat, initInterBrokerProtocol, 3, 3, testContext);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForClientsSuccess(producerName, consumerName, TestConstants.CO_NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @IsolatedTest
    void testUpgradeWithNoMessageAndProtocolVersionsSet(ExtensionContext testContext) {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();

        String producerName = clusterName + "-producer";
        String consumerName = clusterName + "-consumer";

        for (int x = 0; x < sortedVersions.size() - 1; x++) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x + 1);

            runVersionChange(initialVersion, newVersion, producerName, consumerName, null, null, 3, 3, testContext);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForClientsSuccess(producerName, consumerName, TestConstants.CO_NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @IsolatedTest
    void testUpgradeWithoutLogMessageFormatVersionSet(ExtensionContext extensionContext) {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();

        String producerName = clusterName + "-producer";
        String consumerName = clusterName + "-consumer";

        for (int x = 0; x < sortedVersions.size() - 1; x++) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x + 1);

            // If it is an upgrade test we keep the message format as the lower version number
            String interBrokerProtocol = initialVersion.protocolVersion();
            runVersionChange(initialVersion, newVersion, producerName, consumerName, null, interBrokerProtocol, 3, 3, extensionContext);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitForClientsSuccess(producerName, consumerName, TestConstants.CO_NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @BeforeAll
    void setupEnvironment(final ExtensionContext extensionContext) {
        clusterOperator.defaultInstallation(extensionContext).createInstallation().runInstallation();
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    void runVersionChange(TestKafkaVersion initialVersion, TestKafkaVersion newVersion, String producerName, String consumerName, String initLogMsgFormat, String initInterBrokerProtocol, int kafkaReplicas, int zkReplicas, ExtensionContext testContext) {
        boolean isUpgrade = initialVersion.isUpgrade(newVersion);
        Map<String, String> kafkaPods;

        boolean sameMinorVersion = initialVersion.protocolVersion().equals(newVersion.protocolVersion());

        if (KafkaResource.kafkaClient().inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName).get() == null) {
            LOGGER.info("Deploying initial Kafka version {} with logMessageFormat={} and interBrokerProtocol={}", initialVersion.version(), initLogMsgFormat, initInterBrokerProtocol);
            KafkaBuilder kafka = KafkaTemplates.kafkaPersistent(clusterName, kafkaReplicas, zkReplicas)
                .editSpec()
                    .editKafka()
                        .withVersion(initialVersion.version())
                        .withConfig(null)
                    .endKafka()
                .endSpec();

            // Do not set log.message.format.version if it's not passed to method
            if (initLogMsgFormat != null) {
                kafka
                    .editSpec()
                        .editKafka()
                            .addToConfig("log.message.format.version", initLogMsgFormat)
                        .endKafka()
                    .endSpec();
            }
            // Do not set inter.broker.protocol.version if it's not passed to method
            if (initInterBrokerProtocol != null) {
                kafka
                    .editSpec()
                        .editKafka()
                            .addToConfig("inter.broker.protocol.version", initInterBrokerProtocol)
                        .endKafka()
                    .endSpec();
            }
            resourceManager.createResourceWithWait(testContext, kafka.build());

            // ##############################
            // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
            // ##############################
            // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
            resourceManager.createResourceWithWait(testContext, KafkaTopicTemplates.topic(clusterName, continuousTopicName, 3, 3, 2, TestConstants.CO_NAMESPACE).build());
            String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

            KafkaClients kafkaBasicClientJob = new KafkaClientsBuilder()
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withTopicName(continuousTopicName)
                .withMessageCount(continuousClientsMessageCount)
                .withAdditionalConfig(producerAdditionConfiguration)
                .withDelayMs(1000)
                .build();

            resourceManager.createResourceWithWait(testContext, kafkaBasicClientJob.producerStrimzi());
            resourceManager.createResourceWithWait(testContext, kafkaBasicClientJob.consumerStrimzi());
            // ##############################

        } else {
            LOGGER.info("Initial Kafka version (" + initialVersion.version() + ") is already ready");
            kafkaPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, kafkaSelector);

            // Wait for log.message.format.version and inter.broker.protocol.version change
            if (!sameMinorVersion
                    && !isUpgrade
                    && !testContext.getDisplayName().contains("DowngradeToOlderMessageFormat")) {

                // In case that init config was set, which means that CR was updated and CO won't do any changes
                KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
                    LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
                    Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
                    config.put("log.message.format.version", newVersion.messageVersion());
                    config.put("inter.broker.protocol.version", newVersion.protocolVersion());
                    kafka.getSpec().getKafka().setConfig(config);
                    LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
                }, TestConstants.CO_NAMESPACE);

                RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, kafkaSelector, kafkaReplicas, kafkaPods);
            }
        }

        LOGGER.info("Deployment of initial Kafka version (" + initialVersion.version() + ") complete");

        String zkVersionCommand = "ls libs | grep -Po 'zookeeper-\\K\\d+.\\d+.\\d+' | head -1";
        String zkResult = cmdKubeClient().execInPodContainer(KafkaResources.zookeeperPodName(clusterName, 0),
                "zookeeper", "/bin/bash", "-c", zkVersionCommand).out().trim();
        LOGGER.info("Pre-change ZooKeeper version query returned: " + zkResult);

        String kafkaVersionResult = KafkaResource.kafkaClient().inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName).get().getStatus().getKafkaVersion();
        LOGGER.info("Pre-change Kafka version: " + kafkaVersionResult);

        Map<String, String> zkPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, zkSelector);
        kafkaPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, kafkaSelector);
        LOGGER.info("Updating Kafka CR version field to " + newVersion.version());

        // Change the version in Kafka CR
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            kafka.getSpec().getKafka().setVersion(newVersion.version());
        }, TestConstants.CO_NAMESPACE);

        LOGGER.info("Waiting for readiness of new Kafka version (" + newVersion.version() + ") to complete");

        // Wait for the zk version change roll
        zkPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, zkSelector, zkReplicas, zkPods);
        LOGGER.info("1st ZooKeeper roll (image change) is complete");

        // Wait for the kafka broker version change roll
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, kafkaSelector, kafkaPods);
        LOGGER.info("1st Kafka roll (image change) is complete");

        Object currentLogMessageFormat = KafkaResource.kafkaClient().inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName).get().getSpec().getKafka().getConfig().get("log.message.format.version");
        Object currentInterBrokerProtocol = KafkaResource.kafkaClient().inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName).get().getSpec().getKafka().getConfig().get("inter.broker.protocol.version");

        if (isUpgrade && !sameMinorVersion) {
            LOGGER.info("Kafka version is increased, two RUs remaining for increasing IBPV and LMFV");

            if (currentInterBrokerProtocol == null) {
                kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, kafkaSelector, kafkaPods);
                LOGGER.info("Kafka roll (inter.broker.protocol.version) is complete");
            }

            // Only Kafka versions before 3.0.0 require the second roll
            if (currentLogMessageFormat == null && TestKafkaVersion.compareDottedVersions(newVersion.protocolVersion(), "3.0") < 0) {
                kafkaPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, kafkaSelector, kafkaReplicas, kafkaPods);
                LOGGER.info("Kafka roll (log.message.format.version) is complete");
            }
        }

        LOGGER.info("Deployment of Kafka (" + newVersion.version() + ") complete");

        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, KafkaResources.kafkaStatefulSetName(clusterName));

        // Extract the zookeeper version number from the jars in the lib directory
        zkResult = cmdKubeClient().execInPodContainer(KafkaResources.zookeeperPodName(clusterName, 0),
                "zookeeper", "/bin/bash", "-c", zkVersionCommand).out().trim();
        LOGGER.info("Post-change ZooKeeper version query returned: " + zkResult);

        assertThat("ZooKeeper container had version " + zkResult + " where " + newVersion.zookeeperVersion() +
                " was expected", zkResult, is(newVersion.zookeeperVersion()));

        // Extract the Kafka version number from the jars in the lib directory
        kafkaVersionResult = KafkaUtils.getVersionFromKafkaPodLibs(KafkaResource.getKafkaPodName(clusterName, 0));
        LOGGER.info("Post-change Kafka version query returned: " + kafkaVersionResult);

        assertThat("Kafka container had version " + kafkaVersionResult + " where " + newVersion.version() +
                " was expected", kafkaVersionResult, is(newVersion.version()));

        if (isUpgrade && !sameMinorVersion) {
            LOGGER.info("Updating Kafka config attribute 'log.message.format.version' from '{}' to '{}' version", initialVersion.messageVersion(), newVersion.messageVersion());
            LOGGER.info("Updating Kafka config attribute 'inter.broker.protocol.version' from '{}' to '{}' version", initialVersion.protocolVersion(), newVersion.protocolVersion());

            KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
                LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
                Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
                config.put("log.message.format.version", newVersion.messageVersion());
                config.put("inter.broker.protocol.version", newVersion.protocolVersion());
                kafka.getSpec().getKafka().setConfig(config);
                LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
            }, TestConstants.CO_NAMESPACE);

            if (currentLogMessageFormat != null || currentInterBrokerProtocol != null) {
                LOGGER.info("Change of configuration is done manually - rolling update");
                // Wait for the kafka broker version of log.message.format.version change roll
                RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, kafkaSelector, kafkaReplicas, kafkaPods);
                LOGGER.info("Kafka roll (log.message.format.version change) is complete");
            } else {
                LOGGER.info("Cluster Operator already changed the configuration, there should be no rolling update");
                PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, KafkaResources.kafkaStatefulSetName(clusterName));
                assertFalse(RollingUpdateUtils.componentHasRolled(TestConstants.CO_NAMESPACE, kafkaSelector, kafkaPods));
            }
        }

        if (!isUpgrade) {
            LOGGER.info("Verifying that log.message.format attribute updated correctly to version {}", initLogMsgFormat);
            assertThat(Crds.kafkaOperation(kubeClient().getClient()).inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName)
                    .get().getSpec().getKafka().getConfig().get("log.message.format.version"), is(initLogMsgFormat));
            LOGGER.info("Verifying that inter.broker.protocol.version attribute updated correctly to version {}", initInterBrokerProtocol);
            assertThat(Crds.kafkaOperation(kubeClient().getClient()).inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName)
                    .get().getSpec().getKafka().getConfig().get("inter.broker.protocol.version"), is(initInterBrokerProtocol));
        } else {
            if (currentLogMessageFormat != null && currentInterBrokerProtocol != null) {
                LOGGER.info("Verifying that log.message.format attribute updated correctly to version {}", newVersion.messageVersion());
                assertThat(Crds.kafkaOperation(kubeClient().getClient()).inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName)
                        .get().getSpec().getKafka().getConfig().get("log.message.format.version"), is(newVersion.messageVersion()));
                LOGGER.info("Verifying that inter.broker.protocol.version attribute updated correctly to version {}", newVersion.protocolVersion());
                assertThat(Crds.kafkaOperation(kubeClient().getClient()).inNamespace(TestConstants.CO_NAMESPACE).withName(clusterName)
                        .get().getSpec().getKafka().getConfig().get("inter.broker.protocol.version"), is(newVersion.protocolVersion()));
            }
        }

        LOGGER.info("Waiting till Kafka Cluster {}/{} with specified version {} has the same version in status and specification", TestConstants.CO_NAMESPACE, clusterName, newVersion.version());
        KafkaUtils.waitUntilStatusKafkaVersionMatchesExpectedVersion(clusterName, TestConstants.CO_NAMESPACE, newVersion.version());
    }
}
