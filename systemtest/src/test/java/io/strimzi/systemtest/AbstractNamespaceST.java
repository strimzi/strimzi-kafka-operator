/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.List;

public abstract class AbstractNamespaceST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractNamespaceST.class);

    static final String CO_NAMESPACE = "co-namespace-test";
    static final String SECOND_NAMESPACE = "second-namespace-test";
    static final String TOPIC_NAME = "my-topic";
    static final String USER_NAME = "my-user";
    private static final String TOPIC_EXAMPLES_DIR = "../examples/topic/kafka-topic.yaml";

    static Resources secondNamespaceResources;

    void checkKafkaInDiffNamespaceThanCO() {
        String kafkaName = kafkaClusterName(CLUSTER_NAME + "-second");

        secondNamespaceResources.kafkaEphemeral(CLUSTER_NAME + "-second", 3).done();

        LOGGER.info("Waiting for creation {} in namespace {}", kafkaName, SECOND_NAMESPACE);
        setNamespace(SECOND_NAMESPACE);
        StUtils.waitForAllStatefulSetPodsReady(kafkaName, 3);
    }

    void checkMirrorMakerForKafkaInDifNamespaceThanCO() {
        String kafkaName = CLUSTER_NAME + "-target";
        String kafkaSourceName = kafkaClusterName(CLUSTER_NAME);
        String kafkaTargetName = kafkaClusterName(kafkaName);

        secondNamespaceResources.kafkaEphemeral(kafkaName, 3).done();
        secondNamespaceResources.kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, false).done();

        LOGGER.info("Waiting for creation {} in namespace {}", CLUSTER_NAME + "-mirror-maker", SECOND_NAMESPACE);
        setNamespace(SECOND_NAMESPACE);
        StUtils.waitForDeploymentReady(CLUSTER_NAME + "-mirror-maker", 1);
    }

    void deployNewTopic(String topicNamespace, String clusterNamespace, String topic) {
        LOGGER.info("Creating topic {} in namespace {}", topic, topicNamespace);
        setNamespace(topicNamespace);
        cmdKubeClient().create(new File(TOPIC_EXAMPLES_DIR));
        TestUtils.waitFor("wait for 'my-topic' to be created in Kafka", GLOBAL_POLL_INTERVAL, TIMEOUT_FOR_TOPIC_CREATION, () -> {
            setNamespace(clusterNamespace);
            List<String> topics2 = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
            return topics2.contains(topic);
        });
    }

    void deleteNewTopic(String namespace, String topic) {
        LOGGER.info("Deleting topic {} in namespace {}", topic, namespace);
        setNamespace(namespace);
        cmdKubeClient().deleteByName("KafkaTopic", topic);
        setNamespace(CO_NAMESPACE);
    }

    @BeforeEach
    void createSecondNamespaceResources() {
        setNamespace(SECOND_NAMESPACE);
        secondNamespaceResources = new Resources(kubeClient());
        setNamespace(CO_NAMESPACE);
    }

    @Override
    void tearDownEnvironmentAfterEach() throws Exception {
        secondNamespaceResources.deleteResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
        setNamespace(CO_NAMESPACE);
    }
}
