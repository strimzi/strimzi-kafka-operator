/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(UPGRADE)
public class ZookeeperUpgradeST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(ZookeeperUpgradeST.class);

    public static final String NAMESPACE = "zookeeper-upgrade-test";

    @Test
    void testKafkaClusterUpgrade(TestInfo testinfo) {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getKafkaVersions();

        for (int x = 0; x < sortedVersions.size() - 1; x++) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x + 1);

            runVersionChange(initialVersion, newVersion, 3, 3, testinfo);
        }
    }

    @Test
    void testKafkaClusterDowngrade(TestInfo testInfo) {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getKafkaVersions();

        for (int x = sortedVersions.size() - 1; x >= 0; x--) {
            TestKafkaVersion initialVersion = sortedVersions.get(x);
            TestKafkaVersion newVersion = sortedVersions.get(x - 1);

            runVersionChange(initialVersion, newVersion, 3, 3, testInfo);
        }
    }

    void runVersionChange(TestKafkaVersion initialVersion, TestKafkaVersion newVersion, int kafkaReplicas, int zkReplicas, TestInfo testInfo) {
        Map<String, String> kafkaPods;
        String logMsgFormat;
        if (initialVersion.compareTo(newVersion) < 0) {
            // If it is an upgrade test we keep the message format as the lower version number
            logMsgFormat = initialVersion.messageVersion();
        } else {
            // If it is a downgrade then we make sure to use the lower version number for the message format
            logMsgFormat = newVersion.messageVersion();
        }

        boolean sameMajorVersion = initialVersion.protocolVersion().equals(newVersion.protocolVersion());

        if (KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get() == null) {
            LOGGER.info("Deploying initial Kafka version (" + initialVersion.version() + ")");
            KafkaResource.kafkaPersistent(CLUSTER_NAME, kafkaReplicas, zkReplicas)
                .editSpec()
                    .editKafka()
                        .withVersion(initialVersion.version())
                        .addToConfig("log.message.format.version", logMsgFormat)
                    .endKafka()
                .endSpec()
                .done();
        } else {
            LOGGER.info("Initial Kafka version (" + initialVersion.version() + ") is already ready");
            kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

            // Wait for log.message.format.version change
            if (!sameMajorVersion) {
                KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
                    LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
                    Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
                    config.put("log.message.format.version", newVersion.messageVersion());
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

        String kafkaVersionCommand = "ls libs | grep -Po 'kafka_\\d+.\\d+-\\K(\\d+.\\d+.\\d+)(?=.*jar)' | head -1";
        String kafkaResult = cmdKubeClient().execInPodContainer(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                "kafka", "/bin/bash", "-c", kafkaVersionCommand).out().trim();
        LOGGER.info("Pre-change Kafka version query returned: " + kafkaResult);

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

        if (initialVersion.zookeeperVersion().contains("3.4")) {
            // Wait for another Kafka rolling cause dynamic configuration (?)
            kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), kafkaPods);
            LOGGER.info("2nd Kafka roll (update) is complete");
        }

        if (testInfo.getDisplayName().contains("Upgrade") && initialVersion.zookeeperVersion().contains("3.4")) {
            // Two rolling updates are need if we upgrade from 3.4.x to 3.5.x, because of dynamic configuration and snapshots checks
            StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), zkReplicas, zkPods);
            LOGGER.info("2nd Zookeeper roll (update) is complete");
        } else if (testInfo.getDisplayName().contains("Downgrade")) {
            kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), kafkaReplicas, kafkaPods);
            LOGGER.info("3rd Kafka roll (update) is complete");
        }

        LOGGER.info("Deployment of Kafka (" + newVersion.version() + ") complete");

        PodUtils.waitUntilPodsStability(kubeClient().listPodsByPrefixInName(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)));

        // Extract the zookeeper version number from the jars in the lib directory
        zkResult = cmdKubeClient().execInPodContainer(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0),
                "zookeeper", "/bin/bash", "-c", zkVersionCommand).out().trim();
        LOGGER.info("Post-change Zookeeper version query returned: " + zkResult);

        assertThat("Zookeeper container had version " + zkResult + " where " + newVersion.zookeeperVersion() +
                " was expected", zkResult, is(newVersion.zookeeperVersion()));

        // Extract the Kafka version number from the jars in the lib directory
        kafkaResult = cmdKubeClient().execInPodContainer(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                "kafka", "/bin/bash", "-c", kafkaVersionCommand).out().trim();
        LOGGER.info("Post-change Kafka version query returned: " + kafkaResult);

        assertThat("Kafka container had version " + kafkaResult + " where " + newVersion.version() +
                " was expected", kafkaResult, is(newVersion.version()));


        if (testInfo.getDisplayName().contains("Upgrade") && !sameMajorVersion) {
            LOGGER.info("Updating kafka config attribute 'log.message.format.version' from '{}' to '{}' version", initialVersion.version(), newVersion.version());
            LOGGER.info("Verifying that log.message.format attribute updated correctly to version {}", newVersion.messageVersion());

            KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
                LOGGER.info("Kafka config before updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
                Map<String, Object> config = kafka.getSpec().getKafka().getConfig();
                config.put("log.message.format.version", newVersion.messageVersion());
                kafka.getSpec().getKafka().setConfig(config);
                LOGGER.info("Kafka config after updating '{}'", kafka.getSpec().getKafka().getConfig().toString());
            });

            // Wait for the kafka broker version of log.message.format.version change roll
            StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), kafkaReplicas, kafkaPods);
            LOGGER.info("Kafka roll (log.message.format.version change) is complete");
        }

        LOGGER.info("Verifying that log.message.format attribute updated correctly to version {}", newVersion.messageVersion());
        assertThat(Crds.kafkaOperation(kubeClient(NAMESPACE).getClient()).inNamespace(NAMESPACE).withName(CLUSTER_NAME)
                .get().getSpec().getKafka().getConfig().get("log.message.format.version"), is(newVersion.messageVersion()));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
    }
}
