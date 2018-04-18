/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MilliCpuDeserializerTest {
    @Test
    public void testParse() {
        assertEquals(1000, MilliCpuDeserializer.parse("1"));
        assertEquals(1, MilliCpuDeserializer.parse("1m"));
        assertEquals(500, MilliCpuDeserializer.parse("0.5"));
        assertEquals(0, MilliCpuDeserializer.parse("0"));
        assertEquals(0, MilliCpuDeserializer.parse("0m"));
        assertEquals(0, MilliCpuDeserializer.parse("0.0"));
        assertEquals(0, MilliCpuDeserializer.parse("0.000001"));

        try {
            MilliCpuDeserializer.parse("0.0m");
            fail();
        } catch (NumberFormatException e) { }

        try {
            MilliCpuDeserializer.parse("0.1m");
            fail();
        } catch (NumberFormatException e) { }
    }

    @Test
    public void testFormat() {
        assertEquals("1", MilliCpuDeserializer.format(1000));
        assertEquals("500m", MilliCpuDeserializer.format(500));
        assertEquals("1m", MilliCpuDeserializer.format(1));
    }
}
