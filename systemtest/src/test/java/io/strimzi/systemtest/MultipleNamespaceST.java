/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziExtension;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.List;

import static io.strimzi.test.StrimziExtension.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

@ExtendWith(StrimziExtension.class)
@Namespace(MultipleNamespaceST.DEFAULT_NAMESPACE)
@Namespace(value = MultipleNamespaceST.SECOND_NAMESPACE, use = false)
@ClusterOperator
class MultipleNamespaceST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);
    static final String DEFAULT_NAMESPACE = "multiple-namespace-test";
    static final String SECOND_NAMESPACE = "topic-operator-namespace";
    private static final String TOPIC_NAME = "my-topic";
    private static final String TOPIC_INSTALL_DIR = "../examples/topic/kafka-topic.yaml";

    private static Resources classResources;
    private static Resources secondNamespaceResources;

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @Test
    @Tag(REGRESSION)
    void testTopicOperatorWatchingOtherNamespace() throws InterruptedException {
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(TOPIC_NAME)));

        deployNewTopic(SECOND_NAMESPACE, TOPIC_NAME);
        deleteNewTopic(SECOND_NAMESPACE, TOPIC_NAME);
    }

    /**
     * Test the case when Kafka will be deployed in different namespace
     */
    @Test
    @Tag(REGRESSION)
    void testKafkaInDifferentNsThanClusterOperator() throws InterruptedException {
        String kafkaName = kafkaClusterName(CLUSTER_NAME + "-second");

        secondNamespaceResources.kafkaEphemeral(CLUSTER_NAME + "-second", 3).done();

        LOGGER.info("Waiting for creation {} in namespace {}", kafkaName, SECOND_NAMESPACE);
        kubeClient.namespace(SECOND_NAMESPACE);
        kubeClient.waitForStatefulSet(kafkaName, 3);
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace across multiple namespaces
     */
    @Test
    @Tag(REGRESSION)
    void testDeployMirrorMakerAcrossMultipleNamespace() throws InterruptedException {
        String kafkaName = CLUSTER_NAME + "-target";
        String kafkaSourceName = kafkaClusterName(CLUSTER_NAME);
        String kafkaTargetName = kafkaClusterName(kafkaName);

        secondNamespaceResources.kafkaEphemeral(kafkaName, 3).done();
        secondNamespaceResources.kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, false).done();

        LOGGER.info("Waiting for creation {} in namespace {}", CLUSTER_NAME + "-mirror-maker", SECOND_NAMESPACE);
        kubeClient.namespace(SECOND_NAMESPACE);
        kubeClient.waitForDeployment(CLUSTER_NAME + "-mirror-maker", 1);
    }


    @BeforeAll
    static void createClassResources(TestInfo testInfo) {
        LOGGER.info("Creating resources before the test class");
        classResources = new Resources(namespacedClient());
        classResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(SECOND_NAMESPACE)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        testClass = testInfo.getTestClass().get().getSimpleName();
    }

    @AfterAll
    static void deleteClassResources() {
        LOGGER.info("Deleting resources after the test class");
        classResources.deleteResources();
        classResources = null;
    }

    @BeforeEach
    void createSecondNamespaceResources() {
        kubeClient.namespace(SECOND_NAMESPACE);
        secondNamespaceResources = new Resources(namespacedClient());
        kubeClient.namespace(DEFAULT_NAMESPACE);
    }

    @AfterEach
    void deleteSecondNamespaceResources() {
        secondNamespaceResources.deleteResources();
        kubeClient.namespace(DEFAULT_NAMESPACE);
    }

    private static Resources classResources() {
        return classResources;
    }

    private void deployNewTopic(String namespace, String topic) {
        LOGGER.info("Creating topic {} in namespace {}", topic, namespace);
        kubeClient.namespace(namespace);
        kubeClient.create(new File(TOPIC_INSTALL_DIR));
        TestUtils.waitFor("wait for 'my-topic' to be created in Kafka", 5000, 120000, () -> {
            kubeClient.namespace(DEFAULT_NAMESPACE);
            List<String> topics2 = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
            return topics2.contains(topic);
        });
    }

    private void deleteNewTopic(String namespace, String topic) {
        LOGGER.info("Deleting topic {} in namespace {}", topic, namespace);
        kubeClient.namespace(namespace);
        kubeClient.deleteByName("KafkaTopic", topic);
        kubeClient.namespace(DEFAULT_NAMESPACE);
    }
}
