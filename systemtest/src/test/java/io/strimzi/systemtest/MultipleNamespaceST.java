package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziExtension;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
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
    private static final String TOPIC = "my-topic";

    private static final String TOPIC_INSTALL_DIR = "../examples/topic/kafka-topic.yaml";

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @Test
    @Tag(REGRESSION)
    void testWatchingOtherNamespace() throws InterruptedException {
        resources().kafkaEphemeral(CLUSTER_NAME, 1)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(SECOND_NAMESPACE)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(TOPIC)));

        LOGGER.info("Creating topic {} in namespace {}", TOPIC, SECOND_NAMESPACE);
        String origNamespace = kubeClient.namespace(SECOND_NAMESPACE);
        kubeClient.create(new File(TOPIC_INSTALL_DIR));
        TestUtils.waitFor("wait for 'my-topic' to be created in Kafka", 120000, 5000, () -> {
            kubeClient.namespace(origNamespace);
            List<String> topics2 = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
            return topics2.contains(TOPIC);
        });
    }

    @BeforeAll
    static void createClassResources(TestInfo testInfo) {
        testClass = testInfo.getTestClass().get().getSimpleName();
    }
}
