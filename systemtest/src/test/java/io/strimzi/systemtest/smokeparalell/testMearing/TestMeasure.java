package io.strimzi.systemtest.smokeparalell.testMearing;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.interfaces.TestSeparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestMeasure extends AbstractST implements TestSeparator {

    private static final Logger LOGGER = LogManager.getLogger(TestMeasure.class);

    @ParallelTest
    void test() {
        LOGGER.info("hello test");
    }
    @ParallelTest
    void test1() {
        LOGGER.info("hello test1");
    }
}