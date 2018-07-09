/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.Test;

import static io.strimzi.api.kafka.model.MilliCpuDeserializer.format;
import static io.strimzi.api.kafka.model.MilliCpuDeserializer.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MilliCpuDeserializerTest {
    @Test
    public void testParse() {
        assertEquals(1000, parse("1"));
        assertEquals(1, parse("1m"));
        assertEquals(500, parse("0.5"));
        assertEquals(0, parse("0"));
        assertEquals(0, parse("0m"));
        assertEquals(0, parse("0.0"));
        assertEquals(0, parse("0.000001"));

        try {
            parse("0.0m");
            fail();
        } catch (NumberFormatException e) { }

        try {
            parse("0.1m");
            fail();
        } catch (NumberFormatException e) { }
    }

    @Test
    public void testFormat() {
        assertEquals("1", format(1000));
        assertEquals("500m", format(500));
        assertEquals("1m", format(1));
    }

    @Test
    public void testRt() {
        assertEquals("1", format(parse("1")));
        assertEquals("500m", format(parse("500m")));
        assertEquals("1m", format(parse("1m")));
    }
}
