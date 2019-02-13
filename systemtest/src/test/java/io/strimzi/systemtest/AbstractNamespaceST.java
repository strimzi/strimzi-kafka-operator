/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

public abstract class AbstractNamespaceST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractNamespaceST.class);

    static final String CO_NAMESPACE = "multiple-namespace-test";
    static final String SECOND_NAMESPACE = "second-multiply-namespace-test";
    private static final String TOPIC_NAME = "my-topic";
    private static final String TOPIC_INSTALL_DIR = "../examples/topic/kafka-topic.yaml";

    private static Resources secondNamespaceResources;

    void checkTOInDiffNamespaceThanCO() {
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(TOPIC_NAME)));

        deployNewTopic(SECOND_NAMESPACE, TOPIC_NAME);
        deleteNewTopic(SECOND_NAMESPACE, TOPIC_NAME);
    }

    void checkKafkaInDiffNamespaceThanCO() {
        String kafkaName = kafkaClusterName(CLUSTER_NAME + "-second");

        secondNamespaceResources.kafkaEphemeral(CLUSTER_NAME + "-second", 3).done();

        LOGGER.info("Waiting for creation {} in namespace {}", kafkaName, SECOND_NAMESPACE);
        kubeClient.namespace(SECOND_NAMESPACE);
        kubeClient.waitForStatefulSet(kafkaName, 3);
    }

    void checkMirrorMakerForKafkaInDifNamespaceThanCO() {
        String kafkaName = CLUSTER_NAME + "-target";
        String kafkaSourceName = kafkaClusterName(CLUSTER_NAME);
        String kafkaTargetName = kafkaClusterName(kafkaName);

        secondNamespaceResources.kafkaEphemeral(kafkaName, 3).done();
        secondNamespaceResources.kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, false).done();

        LOGGER.info("Waiting for creation {} in namespace {}", CLUSTER_NAME + "-mirror-maker", SECOND_NAMESPACE);
        kubeClient.namespace(SECOND_NAMESPACE);
        kubeClient.waitForDeployment(CLUSTER_NAME + "-mirror-maker", 1);
    }

    void deployNewTopic(String namespace, String topic) {
        LOGGER.info("Creating topic {} in namespace {}", topic, namespace);
        kubeClient.namespace(namespace);
        kubeClient.create(new File(TOPIC_INSTALL_DIR));
        TestUtils.waitFor("wait for 'my-topic' to be created in Kafka", 5000, 120000, () -> {
            kubeClient.namespace(CO_NAMESPACE);
            List<String> topics2 = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
            return topics2.contains(topic);
        });
    }

    void deleteNewTopic(String namespace, String topic) {
        LOGGER.info("Deleting topic {} in namespace {}", topic, namespace);
        kubeClient.namespace(namespace);
        kubeClient.deleteByName("KafkaTopic", topic);
        kubeClient.namespace(CO_NAMESPACE);
    }

    @BeforeEach
    void createSecondNamespaceResources() {
        kubeClient.namespace(SECOND_NAMESPACE);
        secondNamespaceResources = new Resources(namespacedClient());
        kubeClient.namespace(CO_NAMESPACE);
    }

    @AfterEach
    void deleteSecondNamespaceResources() throws Exception {
        secondNamespaceResources.deleteResources();
        waitForDeletion(TEARDOWN_GLOBAL_WAIT, SECOND_NAMESPACE, CO_NAMESPACE, SECOND_NAMESPACE);
        kubeClient.namespace(CO_NAMESPACE);
    }
}
