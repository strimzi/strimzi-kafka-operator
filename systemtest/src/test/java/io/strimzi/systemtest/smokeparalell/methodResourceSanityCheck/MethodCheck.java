package io.strimzi.systemtest.smokeparalell.methodResourceSanityCheck;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtensionContext;

public class MethodCheck extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MethodCheck.class);
    private static final String NAMESPACE = "method-check-namespace";

    @ParallelTest
    void test1Kafka(ExtensionContext extensionContext) {
        LOGGER.info("This is mine {}", mapTestWithClusterNames.get(extensionContext.getDisplayName()));


        resourceManager.createResource(extensionContext, KafkaResource.kafkaEphemeral(mapTestWithClusterNames.get(extensionContext.getDisplayName()), 1 ,1));

        LOGGER.info("Done 1");
    }

    @ParallelTest
    void test2Kafka(ExtensionContext extensionContext) {
        LOGGER.info("This is mine {}", mapTestWithClusterNames.get(extensionContext.getDisplayName()));

        resourceManager.createResource(extensionContext, KafkaResource.kafkaEphemeral(mapTestWithClusterNames.get(extensionContext.getDisplayName()), 1 ,1));

        LOGGER.info("Done 2");
    }

    @BeforeAll
    void setup() {
        installClusterOperator(NAMESPACE);
    }
}
