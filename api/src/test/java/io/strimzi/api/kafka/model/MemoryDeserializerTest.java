/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.Test;

import static io.strimzi.api.kafka.model.MemoryDeserializer.format;
import static io.strimzi.api.kafka.model.MemoryDeserializer.parse;
import static org.junit.Assert.assertEquals;

public class MemoryDeserializerTest {

    @Test
    public void testParse() {
        assertEquals(1234, parse("1234"));
        assertEquals(0, parse("0"));
        assertEquals(1000, parse("1K"));
        assertEquals(1024, parse("1Ki"));
        assertEquals(512 * 1024, parse("512Ki"));
        assertEquals(1_000_000, parse("1e6"));

        assertEquals(0, parse("0"));
        assertEquals(0, parse("0K"));
        assertEquals(0, parse("0e6"));

        try {
            parse("-1K");
        } catch (IllegalArgumentException e) {

        }

        try {
            parse("K");
        } catch (IllegalArgumentException e) {

        }

        try {
            parse("1Kb");
        } catch (IllegalArgumentException e) {

        }

        try {
            parse("foo");
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testFormat() {
        assertEquals("0", format(0));
        assertEquals("1", format(1));
        assertEquals("1023", format(1023));
        assertEquals("1ki", format(1024));
        assertEquals("1k", format(1000));
        assertEquals("2ki", format(2048));
        assertEquals("2k", format(2000));
        assertEquals("2Mi", format(2048 * 1024));
        assertEquals("4096k", format(2048 * 2000));
        assertEquals("4M", format(2000 * 2000));
        assertEquals("1E", format(1_000_000_000_000_000L));
        assertEquals("1000E", format(1_000_000_000_000_000_000L));
        assertEquals("1Ei", format(parse("1Ei")));
        assertEquals("1024Ei", format(parse("1024Ei")));
    }
}
