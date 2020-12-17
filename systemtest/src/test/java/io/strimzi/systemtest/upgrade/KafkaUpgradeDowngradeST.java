/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(UPGRADE)
public class KafkaUpgradeDowngradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaUpgradeDowngradeST.class);

    public static final String NAMESPACE = "zookeeper-upgrade-test";

    private final String continuousTopicName = "continuous-topic";
    private final int continuousClientsMessageCount = 1000;
    private final String producerName = "hello-world-producer";
    private final String consumerName = "hello-world-consumer";

    @Test
    void testKafkaClusterUpgrade(ExtensionContext testContext) {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getKafkaVersions();

        for (int x = 0; x < sortedVersions.size() - 1; x++) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x + 1);

            // If it is an upgrade test we keep the message format as the lower version number
            String logMsgFormat = initialVersion.messageVersion();
            String interBrokerProtocol = initialVersion.protocolVersion();
            runVersionChange(initialVersion, newVersion, logMsgFormat, interBrokerProtocol, 3, 3, testContext);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitTillContinuousClientsFinish(producerName, consumerName, NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @Test
    void testKafkaClusterDowngrade(ExtensionContext testContext) {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getKafkaVersions();

        for (int x = sortedVersions.size() - 1; x > 0; x--) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x - 1);

            // If it is a downgrade then we make sure to use the lower version number for the message format
            String logMsgFormat = newVersion.messageVersion();
            String interBrokerProtocol = newVersion.protocolVersion();
            runVersionChange(initialVersion, newVersion, logMsgFormat, interBrokerProtocol, 3, 3, testContext);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitTillContinuousClientsFinish(producerName, consumerName, NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    @Test
    void testKafkaClusterDowngradeToOlderMessageFormat(ExtensionContext testContext) {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getKafkaVersions();

        String initLogMsgFormat = sortedVersions.get(0).messageVersion();

        if (initLogMsgFormat.charAt(2) + 1 >= sortedVersions.get(sortedVersions.size() - 1).messageVersion().charAt(2)) {
            StringBuilder tmp = new StringBuilder(initLogMsgFormat);
            tmp.setCharAt(2, (char) (initLogMsgFormat.charAt(2) - 1));
            initLogMsgFormat = tmp.toString();
        }

        String interBrokerProtocol = sortedVersions.get(0).protocolVersion();

        if (interBrokerProtocol.charAt(2) + 1 >= sortedVersions.get(sortedVersions.size() - 1).messageVersion().charAt(2)) {
            StringBuilder tmp = new StringBuilder(interBrokerProtocol);
            tmp.setCharAt(2, (char) (interBrokerProtocol.charAt(2) - 1));
            interBrokerProtocol = tmp.toString();
        }

        for (int x = sortedVersions.size() - 1; x > 0; x--) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x - 1);

            runVersionChange(initialVersion, newVersion, initLogMsgFormat, interBrokerProtocol, 3, 3, testContext);
        }

        // ##############################
        // Validate that continuous clients finished successfully
        // ##############################
        ClientUtils.waitTillContinuousClientsFinish(producerName, consumerName, NAMESPACE, continuousClientsMessageCount);
        // ##############################
    }

    void runVersionChange(TestKafkaVersion initialVersion, TestKafkaVersion newVersion, String initLogMsgFormat, String initInterBrokerProtocol, int kafkaReplicas, int zkReplicas, ExtensionContext testContext) {
        Map<String, String> kafkaPods;

        boolean sameMinorVersion = initialVersion.protocolVersion().equals(newVersion.protocolVersion());

        if (KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get() == null) {
            LOGGER.info("Deploying initial Kafka version {} with logMessageFormat={} and interBrokerProtocol={}", initialVersion.version(), initLogMsgFormat, initInterBrokerProtocol);
            KafkaResource.kafkaPersistent(CLUSTER_NAME, kafkaReplicas, zkReplicas)
                .editSpec()
                    .editKafka()
                        .withVersion(initialVersion.version())
                        .addToConfig("log.message.format.version", initLogMsgFormat)
                        .addToConfig("inter.broker.protocol.version", initInterBrokerProtocol)
                    .endKafka()
                .endSpec()
                .done();

            // ##############################
            // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
            // ##############################
            // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
            KafkaTopicResource.topic(CLUSTER_NAME, continuousTopicName, 3, 3, 2).done();
            String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

            KafkaBasicExampleClients kafkaBasicClientJob = new KafkaBasicExampleClients.Builder()
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withTopicName(continuousTopicName)
                .withMessageCount(continuousClientsMessageCount)
                .withAdditionalConfig(producerAdditionConfiguration)
                .withDelayMs(1000)
                .build();

            kafkaBasicClientJob.producerStrimzi().done();
            kafkaBasicClientJob.consumerStrimzi().done();
            // ##############################

        } else {
            LOGGER.info("Initial Kafka version (" + initialVersion.version() + ") is already ready");
            kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

            // Wait for log.message.format.version and inter.broker.protocol.version change
            if (!sameMinorVersion
                    && testContext.getDisplayName().contains("Downgrade")
                    && !testContext.getDisplayName().contains("DowngradeToOlderMessageFormat"))   {
                KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
                    LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
                    Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
                    config.put("log.message.format.version", newVersion.messageVersion());
                    config.put("inter.broker.protocol.version", newVersion.protocolVersion());
                    kafka.getSpec().getKafka().setConfig(config);
                    LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
                });
                StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);
            }
        }

        LOGGER.info("Deployment of initial Kafka version (" + initialVersion.version() + ") complete");

        String zkVersionCommand = "ls libs | grep -Po 'zookeeper-\\K\\d+.\\d+.\\d+' | head -1";
        String zkResult = cmdKubeClient().execInPodContainer(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0),
                "zookeeper", "/bin/bash", "-c", zkVersionCommand).out().trim();
        LOGGER.info("Pre-change Zookeeper version query returned: " + zkResult);

        String kafkaVersionResult = KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        LOGGER.info("Pre-change Kafka version query returned: " + kafkaVersionResult);

        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Updating Kafka CR version field to " + newVersion.version());

        // Get the Kafka resource from K8s
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setVersion(newVersion.version());
        });

        LOGGER.info("Waiting for deployment of new Kafka version (" + newVersion.version() + ") to complete");

        // Wait for the zk version change roll
        zkPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), zkReplicas, zkPods);
        LOGGER.info("1st Zookeeper roll (image change) is complete");

        // Wait for the kafka broker version change roll
        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), kafkaPods);
        LOGGER.info("1st Kafka roll (image change) is complete");

        if (testContext.getDisplayName().contains("Downgrade")) {
            kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), kafkaReplicas, kafkaPods);
            LOGGER.info("2nd Kafka roll (update) is complete");
        }

        LOGGER.info("Deployment of Kafka (" + newVersion.version() + ") complete");

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        // Extract the zookeeper version number from the jars in the lib directory
        zkResult = cmdKubeClient().execInPodContainer(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0),
                "zookeeper", "/bin/bash", "-c", zkVersionCommand).out().trim();
        LOGGER.info("Post-change Zookeeper version query returned: " + zkResult);

        assertThat("Zookeeper container had version " + zkResult + " where " + newVersion.zookeeperVersion() +
                " was expected", zkResult, is(newVersion.zookeeperVersion()));

        // Extract the Kafka version number from the jars in the lib directory
        kafkaVersionResult = KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        LOGGER.info("Post-change Kafka version query returned: " + kafkaVersionResult);

        assertThat("Kafka container had version " + kafkaVersionResult + " where " + newVersion.version() +
                " was expected", kafkaVersionResult, is(newVersion.version()));


        if (testContext.getDisplayName().contains("Upgrade") && !sameMinorVersion) {
            LOGGER.info("Updating kafka config attribute 'log.message.format.version' from '{}' to '{}' version", initialVersion.messageVersion(), newVersion.messageVersion());
            LOGGER.info("Updating kafka config attribute 'inter.broker.protocol.version' from '{}' to '{}' version", initialVersion.protocolVersion(), newVersion.protocolVersion());

            KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
                LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
                Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
                config.put("log.message.format.version", newVersion.messageVersion());
                config.put("inter.broker.protocol.version", newVersion.protocolVersion());
                kafka.getSpec().getKafka().setConfig(config);
                LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
            });

            // Wait for the kafka broker version of log.message.format.version change roll
            StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), kafkaReplicas, kafkaPods);
            LOGGER.info("Kafka roll (log.message.format.version change) is complete");
        }

        if (testContext.getDisplayName().contains("Downgrade")) {
            LOGGER.info("Verifying that log.message.format attribute updated correctly to version {}", initLogMsgFormat);
            assertThat(Crds.kafkaOperation(kubeClient(NAMESPACE).getClient()).inNamespace(NAMESPACE).withName(CLUSTER_NAME)
                    .get().getSpec().getKafka().getConfig().get("log.message.format.version"), is(initLogMsgFormat));
            LOGGER.info("Verifying that inter.broker.protocol.version attribute updated correctly to version {}", initInterBrokerProtocol);
            assertThat(Crds.kafkaOperation(kubeClient(NAMESPACE).getClient()).inNamespace(NAMESPACE).withName(CLUSTER_NAME)
                    .get().getSpec().getKafka().getConfig().get("inter.broker.protocol.version"), is(initInterBrokerProtocol));
        } else {
            LOGGER.info("Verifying that log.message.format attribute updated correctly to version {}", newVersion.messageVersion());
            assertThat(Crds.kafkaOperation(kubeClient(NAMESPACE).getClient()).inNamespace(NAMESPACE).withName(CLUSTER_NAME)
                    .get().getSpec().getKafka().getConfig().get("log.message.format.version"), is(newVersion.messageVersion()));
            LOGGER.info("Verifying that inter.broker.protocol.version attribute updated correctly to version {}", newVersion.protocolVersion());
            assertThat(Crds.kafkaOperation(kubeClient(NAMESPACE).getClient()).inNamespace(NAMESPACE).withName(CLUSTER_NAME)
                    .get().getSpec().getKafka().getConfig().get("inter.broker.protocol.version"), is(newVersion.protocolVersion()));
        }
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 060-Deployment
        BundleResource.clusterOperator(NAMESPACE).done();
    }
}
