/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaVersionUtilsTest {

    @Test
    public void parsingTest() throws Exception {
        List<TestKafkaVersion> versions = TestKafkaVersion.getKafkaVersions();
        assertTrue(versions.size() > 0);
    }
}
