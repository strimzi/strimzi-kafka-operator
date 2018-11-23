/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import org.junit.Test;

import java.util.Map;

import static io.strimzi.operator.cluster.model.ModelUtils.parseImageMap;
import static org.junit.Assert.assertEquals;

public class ModelUtilsTest {

    @Test
    public void testParseImageMap() {
        Map<String, String> m = parseImageMap("2.0.0=strimzi/kafka:latest-kafka-2.0.0\n  " +
                "1.1.1=strimzi/kafka:latest-kafka-1.1.1");
        assertEquals(2, m.size());
        assertEquals("strimzi/kafka:latest-kafka-2.0.0", m.get("2.0.0"));
        assertEquals("strimzi/kafka:latest-kafka-1.1.1", m.get("1.1.1"));

        m = parseImageMap(" 2.0.0=strimzi/kafka:latest-kafka-2.0.0," +
                "1.1.1=strimzi/kafka:latest-kafka-1.1.1");
        assertEquals(2, m.size());
        assertEquals("strimzi/kafka:latest-kafka-2.0.0", m.get("2.0.0"));
        assertEquals("strimzi/kafka:latest-kafka-1.1.1", m.get("1.1.1"));
    }
}
