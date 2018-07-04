/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ResourcesTest {

    @Test
    public void testDeserializeInts() {
        Resources opts = TestUtils.fromJson("{\"limits\": {\"memory\": 10737418240}, \"requests\": {\"memory\": 5000000000}}", Resources.class);
        assertEquals(10737418240L, opts.getLimits().memoryAsLong());
        assertEquals(5000000000L, opts.getRequests().memoryAsLong());
    }

    @Test
    public void testDeserializeDefaults() {
        Resources opts = TestUtils.fromJson("{\"limits\": {\"memory\": 10737418240}, \"requests\": {} }", Resources.class);
        assertEquals(10737418240L, opts.getLimits().memoryAsLong());
        assertEquals(0, opts.getRequests().memoryAsLong());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeInvalidMemory() {
        TestUtils.fromJson("{\"limits\": {\"memory\": \"foo\"}, \"requests\": {\"memory\": bar}}", Resources.class);
    }
}
