package io.strimzi.systemtest.kafkaclients.externalClients;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

public class BasicTest {

    private static final Logger LOGGER = LogManager.getLogger(BasicTest.class);
    private static final StrimziContainer KAFKA_CONTAINER;

    static {
        KAFKA_CONTAINER = new StrimziContainer();
        KAFKA_CONTAINER.start();
    }

    @Test
    void testSomething() {
        LOGGER.info("S...");
    }
}
