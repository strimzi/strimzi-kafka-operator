/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.Test;

import static io.strimzi.api.kafka.model.Quantities.formatMemory;
import static io.strimzi.api.kafka.model.Quantities.formatMilliCpu;
import static io.strimzi.api.kafka.model.Quantities.normalizeCpu;
import static io.strimzi.api.kafka.model.Quantities.normalizeMemory;
import static io.strimzi.api.kafka.model.Quantities.parseCpuAsMilliCpus;
import static io.strimzi.api.kafka.model.Quantities.parseMemory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class QuantitiesTest {

    @Test
    public void testParseMemory() {
        assertEquals(1234, parseMemory("1234"));
        assertEquals(0, parseMemory("0"));
        assertEquals(1000, parseMemory("1K"));
        assertEquals(1024, parseMemory("1Ki"));
        assertEquals(512 * 1024, parseMemory("512Ki"));
        assertEquals(1_000_000, parseMemory("1e6"));

        assertEquals(0, parseMemory("0"));
        assertEquals(0, parseMemory("0K"));
        assertEquals(0, parseMemory("0e6"));

        try {
            parseMemory("-1K");
        } catch (IllegalArgumentException e) {

        }

        try {
            parseMemory("K");
        } catch (IllegalArgumentException e) {

        }

        try {
            parseMemory("1Kb");
        } catch (IllegalArgumentException e) {

        }

        try {
            parseMemory("foo");
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testFormatMemory() {
        assertEquals("0", formatMemory(0));
        assertEquals("1", formatMemory(1));
        assertEquals("1023", formatMemory(1023));
        assertEquals("1Ki", formatMemory(1024));
        assertEquals("1K", formatMemory(1000));
        assertEquals("2Ki", formatMemory(2048));
        assertEquals("2K", formatMemory(2000));
        assertEquals("2Mi", formatMemory(2048 * 1024));
        assertEquals("4096K", formatMemory(2048 * 2000));
        assertEquals("4M", formatMemory(2000 * 2000));
        assertEquals("1E", formatMemory(1_000_000_000_000_000L));
        assertEquals("1000E", formatMemory(1_000_000_000_000_000_000L));
        assertEquals("1Ei", formatMemory(parseMemory("1Ei")));
        assertEquals("1024Ei", formatMemory(parseMemory("1024Ei")));
    }

    @Test
    public void testNormalizeMemory() {
        assertEquals("1K", normalizeMemory("1K"));
        assertEquals("1Ki", normalizeMemory("1Ki"));
        assertEquals("1M", normalizeMemory("1M"));
        assertEquals("1Mi", normalizeMemory("1Mi"));
        assertEquals("12345", normalizeMemory("12345"));
    }

    @Test
    public void testParse() {
        assertEquals(1000, parseCpuAsMilliCpus("1"));
        assertEquals(1, parseCpuAsMilliCpus("1m"));
        assertEquals(500, parseCpuAsMilliCpus("0.5"));
        assertEquals(0, parseCpuAsMilliCpus("0"));
        assertEquals(0, parseCpuAsMilliCpus("0m"));
        assertEquals(0, parseCpuAsMilliCpus("0.0"));
        assertEquals(0, parseCpuAsMilliCpus("0.000001"));

        try {
            parseCpuAsMilliCpus("0.0m");
            fail();
        } catch (NumberFormatException e) { }

        try {
            parseCpuAsMilliCpus("0.1m");
            fail();
        } catch (NumberFormatException e) { }
    }

    @Test
    public void testFormat() {
        assertEquals("1", formatMilliCpu(1000));
        assertEquals("500m", formatMilliCpu(500));
        assertEquals("1m", formatMilliCpu(1));
    }

    @Test
    public void testRt() {
        assertEquals("1", formatMilliCpu(parseCpuAsMilliCpus("1")));
        assertEquals("500m", formatMilliCpu(parseCpuAsMilliCpus("500m")));
        assertEquals("1m", formatMilliCpu(parseCpuAsMilliCpus("1m")));
    }

    @Test
    public void testNormalizeCpu() {
        assertEquals("1", normalizeCpu("1"));
        assertEquals("1", normalizeCpu("1000m"));
        assertEquals("500m", normalizeCpu("500m"));
        assertEquals("500m", normalizeCpu("0.5"));
        assertEquals("100m", normalizeCpu("0.1"));
        assertEquals("10m", normalizeCpu("0.01"));
        assertEquals("1m", normalizeCpu("0.001"));
    }
}
