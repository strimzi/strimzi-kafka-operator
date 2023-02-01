/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import org.junit.jupiter.api.Tag;

import java.util.List;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@ParallelSuite
public class KafkaVersionUtilsTest {

    @ParallelTest
    public void parsingTest() {
        List<TestKafkaVersion> versions = TestKafkaVersion.getSupportedKafkaVersions();
        assertTrue(versions.size() > 0);
    }
}
